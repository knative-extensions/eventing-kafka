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

// PartitionOffsetManagers is a map of Partition -> Sarama PartitionOffsetManager
type PartitionOffsetManagers map[int32]sarama.PartitionOffsetManager

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
	logger.Debug("Found Topic Partitions", zap.Any("Partitions", partitions))

	// Create An OffsetManager For The Specified ConsumerGroup
	offsetManager, err := SaramaNewOffsetManagerFromClientFn(groupId, saramaClient)
	if offsetManager == nil || err != nil {
		logger.Error("Failed to create OffsetManager for ConsumerGroup", zap.Error(err))
		return nil, err
	}

	// Create The Required PartitionOffsetManagers For The Specified Topic / Partitions
	partitionOffsetManagers, err := createPartitionOffsetManagers(offsetManager, topicName, partitions)
	if err != nil {
		logger.Error("Failed to create PartitionOffsetManagers for Topic Partitions", zap.Error(err))
		_ = closeManagersAndDrainErrors(logger, offsetManager, partitionOffsetManagers)
		return nil, err
	}

	// Update All Topic Partitions To The Specified Offset Time
	offsetMappings, err := updateOffsets(logger, saramaClient, offsetManager, partitionOffsetManagers, topicName, partitions, offsetTime)
	if err != nil {
		logger.Error("Failed to update Offsets for Topic Partitions", zap.Error(err))
		_ = closeManagersAndDrainErrors(logger, offsetManager, partitionOffsetManagers)
		return nil, err
	}

	// Close The Sarama Managers And Get Any Accumulated Errors
	err = closeManagersAndDrainErrors(logger, offsetManager, partitionOffsetManagers)
	if err != nil {
		logger.Error("PartitionOffsetManager Errors encountered", zap.Error(err))
		return nil, err
	}

	// Return Success
	return offsetMappings, nil
}

// updateOffsets attempts to update all of the specified Topic's Partitions
// and performs the final Commit() if all were successfully updated.  The
// old/new Offset values are returned if successful.  Per the Sarama library
// implementation, Errors directly related to Offset management are available
// on the respective PartitionOffsetManager's Error channel.  Such errors are
// not returned here as they should be drained after closing the Managers.
func updateOffsets(logger *zap.Logger,
	saramaClient sarama.Client,
	offsetManager sarama.OffsetManager,
	partitionOffsetManagers PartitionOffsetManagers,
	topicName string,
	partitions []int32,
	offsetTime int64) ([]kafkav1alpha1.OffsetMapping, error) {

	// The OffsetMappings To Be Returned For ResetOffset Status
	offsetMappings := make([]kafkav1alpha1.OffsetMapping, len(partitions))

	// Loop Over The Partitions - Updating Offsets & Tracking Results
	for index, partition := range partitions {

		// Enhance The Logger With Partition
		logger = logger.With(zap.Int32("Partition", partition))

		// Get The PartitionOffsetManager For The Current Partition
		partitionOffsetManager := partitionOffsetManagers[partition]
		if partitionOffsetManager == nil {
			logger.Error("Missing PartitionOffsetManager - unable to update Offset")
			return nil, fmt.Errorf("missing PartitionOffsetManager - unable to update Offset")
		}

		// Update The Individual Offset To Specified Time
		offsetMapping, updateErr := updateOffset(logger, saramaClient, partitionOffsetManager, topicName, partition, offsetTime)
		if updateErr != nil {
			logger.Error("Failed to update Offset - skipping Commit", zap.Error(updateErr))
			return nil, updateErr
		}
		offsetMappings[index] = *offsetMapping
	}

	// All Partitions Updated Successfully - Commit The New Offsets!
	logger.Info("All Offsets updated successfully - performing Commit")
	offsetManager.Commit() // No Errors Returned - Will be in PartitionOffsetManager.Errors() Channel Post-Close!

	// Return Success!
	return offsetMappings, nil
}

// updateOffset calculates and performs an update of a single Partition's Offset
// and returns an OffsetMapping representing the old/new state.  No Offset changes
// are committed to allow for atomic commit/fail decision for all Offsets.
func updateOffset(logger *zap.Logger,
	saramaClient sarama.Client,
	partitionOffsetManager sarama.PartitionOffsetManager,
	topic string,
	partition int32,
	offsetTime int64) (*kafkav1alpha1.OffsetMapping, error) {

	// Get The New Offset Of Partition For Specified Time
	newOffset, err := saramaClient.GetOffset(topic, partition, offsetTime)
	if err != nil {
		logger.Error("Failed to get Partition Offset for Time", zap.Int64("Time", offsetTime), zap.Error(err))
		return nil, err
	}

	// Get The Current Offset Of Partition (Accuracy Depends On ConsumerGroup Having Been Stopped)
	currentOffset, _ := partitionOffsetManager.NextOffset()

	// Update The Partition's Offset Forward/Back As Needed
	offsetMetaData := formatOffsetMetaData(offsetTime)
	if newOffset > currentOffset {
		partitionOffsetManager.MarkOffset(newOffset, offsetMetaData) // No Errors Returned - On PartitionOffsetManager.Errors() Channel Instead
	} else if newOffset < currentOffset {
		partitionOffsetManager.ResetOffset(newOffset, offsetMetaData) // No Errors Returned - On PartitionOffsetManager.Errors() Channel Instead
	}

	// Create An OffsetMapping For The Partition
	offsetMapping := &kafkav1alpha1.OffsetMapping{
		Partition: partition,
		OldOffset: currentOffset,
		NewOffset: newOffset,
	}

	// Return The OffsetMapping Success
	return offsetMapping, nil
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

// createPartitionOffsetManagers initializes the PartitionOffsetManagers for the specified topic / partitions.
func createPartitionOffsetManagers(offsetManager sarama.OffsetManager, topicName string, partitions []int32) (PartitionOffsetManagers, error) {
	partitionOffsetManagers := make(PartitionOffsetManagers, len(partitions))
	for _, partition := range partitions {
		partitionOffsetManager, err := offsetManager.ManagePartition(topicName, partition)
		partitionOffsetManagers[partition] = partitionOffsetManager
		if err != nil {
			return partitionOffsetManagers, err
		}
	}
	return partitionOffsetManagers, nil
}

// closePartitionOffsetManagers performs an AsyncClose on the PartitionOffsetManagers
func closePartitionOffsetManagers(partitionOffsetManagers PartitionOffsetManagers) {
	for _, partitionOffsetManager := range partitionOffsetManagers {
		if partitionOffsetManager != nil {
			partitionOffsetManager.AsyncClose() // Fast - No Errors Returned - Works Without AutoCommit ; )
		}
	}
}

// closeManagersAndDrainErrors handles the tear-down of the Sarama OffsetManager and
// PartitionOffsetManagers in the necessary sequence, and returns any errors that have
// accumulated.  This function should be called AFTER the OffsetManager.Commit() operation
// has been performed, and the errors could be related to prior MarkOffset / ResetOffset
// / Commit operations.  These Sarama "managers" are intertwined and Sarama is very
// proscriptive about the order in which they should be closed and drained.
func closeManagersAndDrainErrors(logger *zap.Logger, offsetManager sarama.OffsetManager, partitionOffsetManagers PartitionOffsetManagers) error {

	// Close The PartitionOffsetManagers (Must Be Called Before Closing OffsetManager)
	closePartitionOffsetManagers(partitionOffsetManagers)

	// Close The OffsetManager (Must Be Called After Closing PartitionOffsetManagers)
	if offsetManager != nil {
		closeErr := offsetManager.Close()
		if closeErr != nil {
			logger.Error("Failed to close Sarama OffsetManager", zap.Error(closeErr)) // Doesn't appear to be possible but log it and continue
		}
	}

	// Drain The PartitionOffsetManagers Error Channels (Must Be Called After Close)
	pomErr := drainPartitionOffsetManagerErrors(partitionOffsetManagers)
	if pomErr != nil {
		logger.Error("Errors encountered during Offset update", zap.Errors("Sarama PartitionOffsetManager Errors", multierr.Errors(pomErr)))
	}

	// Return Any PartitionOffsetManager Errors
	return pomErr
}

// drainPartitionOffsetManagerErrors drains the PartitionOffsetManager's Error channels and
// returns any ConsumerErrors as a Zap multierr, and must be called after Commit() / Close().
// This async error channel not ideal but is simply the way the Sarama library operates.
func drainPartitionOffsetManagerErrors(partitionOffsetManagers PartitionOffsetManagers) error {
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
