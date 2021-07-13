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

package sarama

import (
	"testing"
	"time"

	"github.com/Shopify/sarama"
)

func TestGetOffset(t *testing.T) {
	testCases := map[string]struct {
		topicPartitions map[string][]int32
		topicOffsets    map[string]map[int32]int64
	}{
		"no topics": {
			topicPartitions: map[string][]int32{},
			topicOffsets:    map[string]map[int32]int64{},
		},
		"one topic, one partition": {
			topicPartitions: map[string][]int32{"my-topic": {0}},
			topicOffsets: map[string]map[int32]int64{
				"my-topic": {
					0: 5,
				},
			},
		},
		"several topics, several partitions": {
			topicPartitions: map[string][]int32{
				"my-topic":   {0, 1},
				"my-topic-2": {0, 1, 2},
				"my-topic-3": {0, 1, 2, 3}},
			topicOffsets: map[string]map[int32]int64{
				"my-topic":   {0: 5, 1: 7},
				"my-topic-2": {0: 5, 1: 7, 2: 9},
				"my-topic-3": {0: 5, 1: 7, 2: 2, 3: 10},
			},
		},
	}

	for n, tc := range testCases {
		t.Run(n, func(t *testing.T) {
			broker := sarama.NewMockBroker(t, 1)
			defer broker.Close()
			offsetResponse := sarama.NewMockOffsetResponse(t).SetVersion(1)
			for topic, partitions := range tc.topicOffsets {
				for partition, offset := range partitions {
					offsetResponse = offsetResponse.SetOffset(topic, partition, -1, offset)
				}
			}

			metadataResponse := sarama.NewMockMetadataResponse(t).
				SetController(broker.BrokerID()).
				SetBroker(broker.Addr(), broker.BrokerID())

			for topic, partitions := range tc.topicOffsets {
				for partition := range partitions {
					metadataResponse = metadataResponse.SetLeader(topic, partition, broker.BrokerID())
				}
			}

			broker.SetHandlerByMap(map[string]sarama.MockResponse{
				"OffsetRequest":   offsetResponse,
				"MetadataRequest": metadataResponse,
			})

			config := sarama.NewConfig()
			config.Version = sarama.MaxVersion

			sc, err := sarama.NewClient([]string{broker.Addr()}, config)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			defer sc.Close()

			offsets, err := GetOffsets(sc, tc.topicPartitions, sarama.OffsetNewest)

			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			if len(tc.topicOffsets) != len(offsets) {
				t.Errorf("unexpected number of topics. wanted %d, got %d", len(tc.topicOffsets), len(offsets))
			}

			for topic, topicOffsets := range offsets {
				if len(tc.topicOffsets[topic]) != len(topicOffsets) {
					t.Errorf("unexpected number of partitions. wanted %d, got %d", len(tc.topicOffsets[topic]), len(topicOffsets))
				}

				for partitionID, offset := range topicOffsets {
					if tc.topicOffsets[topic][partitionID] != offset {
						t.Errorf("unexpected offset. wanted %d, got %d", tc.topicOffsets[topic][partitionID], offset)
					}
				}
			}

		})
	}

	// Seems like there is no way to check the broker has been really closed. So wait few seconds
	time.Sleep(2 * time.Second)
}
