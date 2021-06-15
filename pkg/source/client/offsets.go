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

	// Look for uninitialized offset (-1)
	offsets, err := kafkaAdminClient.ListConsumerGroupOffsets(consumerGroup, topicPartitions)
	if err != nil {
		return err
	}

	dirty := false
	for topic, partitions := range offsets.Blocks {
		for partition, block := range partitions {
			if block.Offset == -1 { // not initialized?
				// Fetch the newest offset in the topic/partition and set it in the consumer group
				offset, err := kafkaClient.GetOffset(topic, partition, sarama.OffsetNewest)
				if err != nil {
					return fmt.Errorf("failed to get the offset for topic %s and partition %d: %w", topic, partition, err)
				}

				logging.FromContext(ctx).Infow("initializing offset", zap.String("topic", topic), zap.Int32("partition", partition), zap.Int64("offset", offset))

				pm, err := offsetManager.ManagePartition(topic, partition)
				if err != nil {
					return fmt.Errorf("failed to create the partition manager for topic %s and partition %d: %w", topic, partition, err)
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
