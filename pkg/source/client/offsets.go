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

package client

import (
	"context"
	"fmt"

	"github.com/Shopify/sarama"
	"go.uber.org/zap"
	"knative.dev/pkg/logging"

	knsarama "knative.dev/eventing-kafka/pkg/common/kafka/sarama"
)

// We want to make sure that ALL consumer group offsets are set before marking
// the source as ready, to avoid "losing" events in case the consumer group session
// is closed before at least one message is consumed from ALL partitions.
// Without InitOffsets, an event sent to a partition with an uninitialized offset
// will not be forwarded when the session is closed (or a rebalancing is in progress).
func InitOffsets(ctx context.Context, kafkaClient sarama.Client, topics []string, consumerGroup string) error {
	offsetManager, err := sarama.NewOffsetManagerFromClient(consumerGroup, kafkaClient)
	if err != nil {
		return err
	}

	kafkaAdminClient, err := sarama.NewClusterAdminFromClient(kafkaClient)
	if err != nil {
		return fmt.Errorf("failed to create a Kafka admin client: %w", err)
	}
	defer kafkaAdminClient.Close()

	// Retrieve all partitions
	topicPartitions := make(map[string][]int32)
	for _, topic := range topics {
		partitions, err := kafkaClient.Partitions(topic)

		if err != nil {
			return fmt.Errorf("failed to get partitions for topic %s: %w", topic, err)
		}

		topicPartitions[topic] = partitions
	}

	// Fetch topic offsets
	topicOffsets, err := knsarama.GetOffsets(kafkaClient, topicPartitions, sarama.OffsetNewest)
	if err != nil {
		return fmt.Errorf("failed to get the topic offsets: %w", err)
	}

	// Look for uninitialized offset (-1)
	offsets, err := kafkaAdminClient.ListConsumerGroupOffsets(consumerGroup, topicPartitions)
	if err != nil {
		return err
	}

	dirty := false
	for topic, partitions := range offsets.Blocks {
		for partitionID, block := range partitions {
			if block.Offset == -1 { // not initialized?
				partitionsOffsets, ok := topicOffsets[topic]
				if !ok {
					// topic may have been deleted. ignore.
					continue
				}

				offset, ok := partitionsOffsets[partitionID]
				if !ok {
					// partition may have been deleted. ignore.
					continue
				}

				logging.FromContext(ctx).Infow("initializing offset", zap.String("topic", topic), zap.Int32("partition", partitionID), zap.Int64("offset", offset))

				pm, err := offsetManager.ManagePartition(topic, partitionID)
				if err != nil {
					return fmt.Errorf("failed to create the partition manager for topic %s and partition %d: %w", topic, partitionID, err)
				}

				pm.MarkOffset(offset, "")
				dirty = true
			}
		}
	}

	if dirty {
		offsetManager.Commit()
		logging.FromContext(ctx).Infow("consumer group offsets committed", zap.String("consumergroup", consumerGroup))
	}

	// At this stage the KafkaSource instance is considered Ready
	return nil

}
