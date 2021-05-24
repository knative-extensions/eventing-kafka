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
	"fmt"

	"github.com/Shopify/sarama"
)

// CheckOffsetsCommitted returns true if all consumer group offsets are committed
func CheckOffsetsCommitted(kafkaClient sarama.Client, topics []string, consumerGroup string) (bool, error) {
	kafkaAdminClient, err := sarama.NewClusterAdminFromClient(kafkaClient)
	if err != nil {
		return false, fmt.Errorf("failed to create a Kafka admin client: %w", err)
	}
	defer kafkaAdminClient.Close()

	// Retrieve all partitions
	topicPartitions := make(map[string][]int32)
	for _, topic := range topics {
		partitions, err := kafkaClient.Partitions(topic)

		if err != nil {
			return false, fmt.Errorf("failed to get partitions for topic %s: %w", topic, err)
		}

		topicPartitions[topic] = partitions
	}

	// Look for uninitialized offset (-1)
	offsets, err := kafkaAdminClient.ListConsumerGroupOffsets(consumerGroup, topicPartitions)
	if err != nil {
		return false, err
	}

	for _, partitions := range offsets.Blocks {
		for _, block := range partitions {
			if block.Offset == -1 { // not initialized?
				return false, nil
			}
		}
	}

	return true, nil
}
