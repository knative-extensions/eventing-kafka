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

	"github.com/Shopify/sarama"
	"go.uber.org/zap"
	"knative.dev/pkg/logging"

	kafkav1alpha1 "knative.dev/eventing-kafka/pkg/apis/kafka/v1alpha1"
)

// updateOffsets performs an update of all Partitions Offsets for the specified
// Topic / Group and returns OffsetMappings representing the old/new state.
func (r *Reconciler) updateOffsets(ctx context.Context, saramaClient sarama.Client, topic string, group string, time int64) ([]kafkav1alpha1.OffsetMapping, error) {

	// Get The Logger From Context & And Enhance With Topic
	logger := logging.FromContext(ctx).Desugar().With(
		zap.String("Topic", topic),
		zap.String("Group", group),
		zap.Int64("Time", time))

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
		offsetMapping, err := r.updateOffset(partitionLogger, saramaClient, offsetManager, topic, partition, time)
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
	time int64) (*kafkav1alpha1.OffsetMapping, error) {

	// Create A PartitionOffsetManager For This Partition
	partitionOffsetManager, err := offsetManager.ManagePartition(topic, partition)
	defer safeClosePartitionOffsetManager(logger, partitionOffsetManager) // TODO - can i safely close this before committing ???  appears to be ok? but need to test starting up dispatcher and doing a replay
	if err != nil {
		logger.Error("Failed to create PartitionOffsetManager", zap.Error(err))
		return nil, err
	}

	// TODO - Handle errors, select statement with exit and sleep, etc...  FOR NOW JUST LOG ERRORS !!!
	//// Setup PartitionOffsetManager Error Handler (Depends onConfig.Consumer.Return.Errors=true)
	//offsetManagerErrorChan := partitionOffsetManager.Errors()
	//go func() {
	//	for {
	//		consumerError := <-offsetManagerErrorChan
	//		logger.Error("PartitionOffsetManager ConsumerError", zap.Error(consumerError))
	//	}
	//}()

	// Get The Current Offset Of Partition (Accuracy Depends On ConsumerGroup Having Been Stopped)
	currentOffset, _ := partitionOffsetManager.NextOffset()

	// Get The New Offset Of Partition For Specified Time
	newOffset, err := saramaClient.GetOffset(topic, partition, time)
	if err != nil {
		logger.Error("Failed to get Partition Offset for Time", zap.Int64("Time", time), zap.Error(err))
		return nil, err
	}

	// Create An OffsetMapping For The Partition
	offsetMapping := &kafkav1alpha1.OffsetMapping{
		Partition: partition,
		OldOffset: currentOffset,
		NewOffset: newOffset,
	}

	// Update The Partition's Offset Forward/Back As Needed
	if newOffset > currentOffset {
		partitionOffsetManager.MarkOffset(newOffset, "TODO")
	} else {
		partitionOffsetManager.ResetOffset(newOffset, "TODO") // TODO - should use "ResetOffset-<time>" or something ?
	}

	// Return The OffsetMapping / Success
	return offsetMapping, nil
}

// safeCloseOffsetManager wraps the Sarama OffsetManager.Close() call for safe usage as a defer statement
func safeCloseOffsetManager(logger *zap.Logger, offsetManager sarama.OffsetManager) {
	if offsetManager != nil {
		err := offsetManager.Close()
		if err != nil {
			logger.Error("Failed to close Sarama OffsetManager", zap.Error(err))
		}
	}
}

// safeClosePartitionOffsetManager wraps the Sarama OffsetManager.Close() call for safe usage as a defer statement
func safeClosePartitionOffsetManager(logger *zap.Logger, partitionOffsetManager sarama.PartitionOffsetManager) {
	if partitionOffsetManager != nil {
		err := partitionOffsetManager.Close()
		if err != nil {
			logger.Error("Failed to close Sarama PartitionOffsetManager", zap.Error(err))
		}
	}
}
