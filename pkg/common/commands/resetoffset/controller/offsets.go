/*
Copyright 2021 The Knative Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"knative.dev/pkg/logging"

	kafkav1alpha1 "knative.dev/eventing-kafka/pkg/apis/kafka/v1alpha1"
)

// SaramaNewClientFnType defines the Sarama NewClient() function signature.
type SaramaNewClientFnType func([]string, *sarama.Config) (sarama.Client, error)

// SaramaNewClientFn is a reference to the Sarama NewClient() function used
// when reconciling offsets which facilitates stubbing in unit tests.
var SaramaNewClientFn SaramaNewClientFnType = sarama.NewClient

// SaramaNewOffsetManagerFromClientFnType defines the Sarama NewOffsetManagerFromClient() function signature.
type SaramaNewOffsetManagerFromClientFnType func(group string, client sarama.Client) (sarama.OffsetManager, error)

// SaramaNewOffsetManagerFromClientFn is a reference to the Sarama NewOffsetManagerFromClient()
// function used when reconciling offsets which facilitates stubbing in unit tests.
var SaramaNewOffsetManagerFromClientFn SaramaNewOffsetManagerFromClientFnType = sarama.NewOffsetManagerFromClient

// updateOffsetsError is the general error for any failure in updating offsets, exposed for test use.
var updateOffsetsError = fmt.Errorf("failed to update Offsets")

// reconcileOffsets updates the Offsets of all Partitions for the specified
// Topic / ConsumerGroup to the Offset value corresponding to the specified
// offsetTime (millis since epoch) and return OffsetMappings of the old/new
// state.  An error will be returned and the Offsets will not be committed
// if any problems occur.
func (r *Reconciler) reconcileOffsets(ctx context.Context, topicName string, groupId string, offsetTime int64) ([]kafkav1alpha1.OffsetMapping, error) {

	// Get The Logger From The Context & Enhance The With Parameters
	logger := logging.FromContext(ctx).Desugar().With(
		zap.String("Topic", topicName),
		zap.String("Group", groupId),
		zap.Int64("Time", offsetTime))

	// Initialize A New Sarama Client
	//
	// ResetOffset is an infrequently used feature so there is no need for
	// reuse, and there are also "broken-pipe" failures (non-recoverable)
	// after periods of inactivity to deal with...
	//   https://github.com/Shopify/sarama/issues/1162
	//   https://github.com/Shopify/sarama/issues/866
	saramaClient, err := SaramaNewClientFn(r.kafkaBrokers, r.saramaConfig)
	defer safeCloseSaramaClient(logger, saramaClient)
	if saramaClient == nil || err != nil {
		logger.Error("Failed to create a new Sarama Client", zap.Error(err))
		return nil, err
	}

	// Get The Partitions Of The Specified Kafka Topic
	partitions, err := saramaClient.Partitions(topicName)
	if err != nil {
		logger.Error("Failed to determine Partitions for Topic", zap.Error(err))
		return nil, err
	}
	logger.Info("Found Topic Partitions", zap.Any("Partitions", partitions))

	// Create An OffsetManager For The Specified ConsumerGroup
	offsetManager, err := SaramaNewOffsetManagerFromClientFn(groupId, saramaClient)
	if offsetManager == nil || err != nil {
		logger.Error("Failed to create OffsetManager for ConsumerGroup", zap.Error(err))
		return nil, err
	}

	// Track Partition Specific Resources
	partitionOffsetManagers := make([]sarama.PartitionOffsetManager, len(partitions))
	offsetMappings := make([]kafkav1alpha1.OffsetMapping, len(partitions))

	// Loop Over The Partitions - Updating Offsets & Tracking Results
	updatesFailed := false
	for index, partition := range partitions {
		partitionOffsetManager, offsetMapping, updateErr := updateOffset(logger, saramaClient, offsetManager, topicName, partition, offsetTime)
		if updateErr != nil {
			updatesFailed = true
		}
		if partitionOffsetManager != nil {
			partitionOffsetManagers[index] = partitionOffsetManager
		}
		if offsetMapping != nil {
			offsetMappings[index] = *offsetMapping
		}
	}

	// If All Partitions Were Updated Successfully Then Commit The New Offsets
	if updatesFailed {
		logger.Error("Failed to update all Offsets - skipping Commit")
	} else {
		logger.Info("All Offsets updated successfully - performing Commit")
		offsetManager.Commit()
	}

	// Close The PartitionOffsetManagers (Must Be Called Before Closing OffsetManager)
	for _, partitionOffsetManager := range partitionOffsetManagers {
		if partitionOffsetManager != nil {
			partitionOffsetManager.AsyncClose()
		}
	}

	// Close The OffsetManager (Must Be Called After Closing PartitionOffsetManagers)
	err = offsetManager.Close()
	if err != nil {
		logger.Error("Failed to close Sarama OffsetManager", zap.Error(err)) // Doesn't appear to be possible but log it and continue
	}

	// Drain The PartitionOffsetManagers Error Channels (Must Be Called After Commit/Close)
	err = drainPartitionOffsetManagerErrors(partitionOffsetManagers...)
	if err != nil {
		logger.Error("Errors encountered during Offset update", zap.Errors("Sarama PartitionOffsetManager Errors", multierr.Errors(err)))
	}

	// Return Results
	var resultErr error
	if err != nil || updatesFailed {
		resultErr = updateOffsetsError
	}
	return offsetMappings, resultErr
}

// updateOffset performs the update of a single Partition's Offset and
// returns an OffsetMapping representing the old/new state.
func updateOffset(logger *zap.Logger,
	saramaClient sarama.Client,
	offsetManager sarama.OffsetManager,
	topic string,
	partition int32,
	offsetTime int64) (sarama.PartitionOffsetManager, *kafkav1alpha1.OffsetMapping, error) {

	// Enhance The Logger With Partition
	logger = logger.With(zap.Int32("Partition", partition))

	// Get The New Offset Of Partition For Specified Time
	newOffset, err := saramaClient.GetOffset(topic, partition, offsetTime)
	if err != nil {
		logger.Error("Failed to get Partition Offset for Time", zap.Int64("Time", offsetTime), zap.Error(err))
		return nil, nil, err
	}

	// Create A PartitionOffsetManager For The Specified Partition
	partitionOffsetManager, err := offsetManager.ManagePartition(topic, partition)
	if partitionOffsetManager == nil || err != nil {
		logger.Error("Failed to create PartitionOffsetManager", zap.Error(err))
		if partitionOffsetManager != nil {
			partitionOffsetManager.AsyncClose()
		}
		return nil, nil, err
	}

	// Get The Current Offset Of Partition (Accuracy Depends On ConsumerGroup Having Been Stopped)
	currentOffset, _ := partitionOffsetManager.NextOffset()

	// Create An OffsetMapping For The Partition
	offsetMapping := &kafkav1alpha1.OffsetMapping{
		Partition: partition,
		OldOffset: currentOffset,
		NewOffset: newOffset,
	}

	// Update The Partition's Offset Forward/Back As Needed
	offsetMetaData := formatOffsetMetaData(offsetTime)
	if newOffset > currentOffset {
		partitionOffsetManager.MarkOffset(newOffset, offsetMetaData)
	} else if newOffset < currentOffset {
		partitionOffsetManager.ResetOffset(newOffset, offsetMetaData)
	}

	// Return The PartitionOffsetManager, OffsetMapping Success
	return partitionOffsetManager, offsetMapping, nil
}

// formatOffsetMetaData returns a "metadata" string, suitable for use with MarkOffset/ResetOffset, for the specified time.
func formatOffsetMetaData(time int64) string {
	return fmt.Sprintf("resetoffset.%d", time)
}

// safeCloseSaramaClient will attempt to close the specified Sarama Client
func safeCloseSaramaClient(logger *zap.Logger, client sarama.Client) {
	if client != nil && !client.Closed() {
		err := client.Close()
		if err != nil {
			logger.Warn("Failed to close Sarama Client", zap.Error(err))
		}
	}
}

// drainPartitionOffsetManagerErrors drains the PartitionOffsetManager's Error channels and
// returns any ConsumerErrors as a Zap multierr, and must be called after Commit() / Close().
func drainPartitionOffsetManagerErrors(partitionOffsetManagers ...sarama.PartitionOffsetManager) error {
	var multiErr error
	for _, partitionOffsetManager := range partitionOffsetManagers {
		if partitionOffsetManager != nil {
			select {
			case consumerErr, ok := <-partitionOffsetManager.Errors():
				if consumerErr != nil {
					if multiErr == nil {
						multiErr = consumerErr.Unwrap()
					} else {
						multierr.AppendInto(&multiErr, consumerErr.Unwrap())
					}
				}
				if !ok {
					break // Error Channel Closed - Stop Draining
				}
			case <-time.After(5 * time.Second):
				break // Error Channel Drain Timeout - Stop Waiting For Channel Close
			}
		}
	}
	return multiErr
}
