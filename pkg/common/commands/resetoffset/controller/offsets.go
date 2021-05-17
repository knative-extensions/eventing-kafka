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
var updateOffsetsError = fmt.Errorf("failed to update all Offsets, skipping commit")

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

	// Create An OffsetManager For The Specified ConsumerGroup
	offsetManager, err := SaramaNewOffsetManagerFromClientFn(groupId, saramaClient)
	defer safeCloseOffsetManager(logger, offsetManager)
	if err != nil {
		logger.Error("Failed to create OffsetManager for ConsumerGroup", zap.Error(err))
		return nil, err
	}

	// Get The Partitions Of The Specified Kafka Topic
	partitions, err := saramaClient.Partitions(topicName)
	if err != nil {
		logger.Error("Failed to determine Partitions for Topic", zap.Error(err))
		return nil, err
	}
	logger.Info("Found Topic Partitions", zap.Any("Partitions", partitions))

	// Create An Array Of OffsetMappings To Hold Old/New Offsets
	offsetMappings := make([]kafkav1alpha1.OffsetMapping, len(partitions))

	// Loop Over The Partitions - Tracking Status
	commitOffsets := true
	for index, partition := range partitions {

		// Enhance The Logger With Partition
		partitionLogger := logger.With(zap.Int32("Partition", partition))

		// Update The Partition's Offset (Process all partitions even on error in order to populate OffsetMappings for CRD)
		offsetMapping, updateErr := updateOffset(partitionLogger, saramaClient, offsetManager, topicName, partition, offsetTime)
		if offsetMapping != nil {
			offsetMappings[index] = *offsetMapping
		}
		if updateErr != nil {
			partitionLogger.Error("Failed to update offset", zap.Error(updateErr))
			commitOffsets = false
		} else {
			partitionLogger.Info("Successfully updated offset", zap.Any("OffsetMapping", offsetMapping))
		}
	}

	// If All Partitions Were Updated Successfully Then Commit The New Offsets
	if commitOffsets {
		logger.Info("All Offsets updated successfully - performing Commit")
		offsetManager.Commit()
		return offsetMappings, nil
	} else {
		logger.Error("Failed to update all Offsets - skipping Commit")
		return offsetMappings, updateOffsetsError
	}
}

// updateOffset performs the update of a single Partition's Offset and
// returns an OffsetMapping representing the old/new state.
func updateOffset(logger *zap.Logger,
	saramaClient sarama.Client,
	offsetManager sarama.OffsetManager,
	topic string,
	partition int32,
	offsetTime int64) (*kafkav1alpha1.OffsetMapping, error) {

	// Create A PartitionOffsetManager For The Specified Partition
	partitionOffsetManager, err := offsetManager.ManagePartition(topic, partition)
	defer safeClosePartitionOffsetManager(logger, partitionOffsetManager)
	if err != nil {
		logger.Error("Failed to create PartitionOffsetManager", zap.Error(err))
		return nil, err
	}

	// Get The Current Offset Of Partition (Accuracy Depends On ConsumerGroup Having Been Stopped)
	currentOffset, _ := partitionOffsetManager.NextOffset()

	// Get The New Offset Of Partition For Specified Time
	newOffset, err := saramaClient.GetOffset(topic, partition, offsetTime)
	if err != nil {
		logger.Error("Failed to get Partition Offset for Time", zap.Int64("Time", offsetTime), zap.Error(err))
		return nil, err
	}

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

	// Check For Any Errors From The PartitionOffsetManager
	select {
	case err = <-partitionOffsetManager.Errors():
	case <-time.After(1 * time.Second):
	}

	// Return Results
	return offsetMapping, err
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

// safeCloseOffsetManager wraps the Sarama OffsetManager.Close() call for safe usage as a defer statement
func safeCloseOffsetManager(logger *zap.Logger, offsetManager sarama.OffsetManager) {
	if offsetManager != nil {
		err := offsetManager.Close() // This is fast ; )
		if err != nil {
			logger.Error("Failed to close Sarama OffsetManager", zap.Error(err))
		}
	}
}

// safeClosePartitionOffsetManager wraps the Sarama OffsetManager.Close() call for safe usage as a defer statement
func safeClosePartitionOffsetManager(logger *zap.Logger, partitionOffsetManager sarama.PartitionOffsetManager) {
	if partitionOffsetManager != nil {
		err := partitionOffsetManager.Close() // Takes several seconds in success case & can hang (on AsyncClose lock) if ConsumerGroups not stopped!
		if err != nil {
			logger.Error("Failed to close Sarama PartitionOffsetManager", zap.Error(err))
		}
	}
}
