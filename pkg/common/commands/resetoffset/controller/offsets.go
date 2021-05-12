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

// updateOffsets performs an update of all Partitions Offsets for the specified
// Topic / Group and returns OffsetMappings representing the old/new state.
func (r *Reconciler) updateOffsets(ctx context.Context, saramaClient sarama.Client, topic string, group string, offsetTime int64) ([]kafkav1alpha1.OffsetMapping, error) {

	// Get The Logger From Context & And Enhance With Topic
	logger := logging.FromContext(ctx).Desugar().With(
		zap.String("Topic", topic),
		zap.String("Group", group),
		zap.Int64("Time", offsetTime))

	// Create An OffsetManager For The Specified ConsumerGroup
	offsetManager, err := sarama.NewOffsetManagerFromClient(group, saramaClient)
	defer safeCloseOffsetManager(logger, offsetManager)
	if err != nil {
		logger.Error("Failed to create OffsetManager for ConsumerGroup", zap.Error(err))
		return nil, err
	}

	// Get The Partitions Of The Specified Kafka Topic
	partitions, err := saramaClient.Partitions(topic)
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
		offsetMapping, err := r.updateOffset(partitionLogger, saramaClient, offsetManager, topic, partition, offsetTime)
		if offsetMapping == nil || err != nil {
			partitionLogger.Error("Failed to update offset", zap.Error(err))
			commitOffsets = false
		} else {
			offsetMappings[index] = *offsetMapping
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
		return offsetMappings, fmt.Errorf("failed to update all Offsets, skipping commit")
	}
}

// updateOffset performs the update of a single Partition's Offset and
// returns an OffsetMapping representing the old/new state.
func (r *Reconciler) updateOffset(logger *zap.Logger,
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
