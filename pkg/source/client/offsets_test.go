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
	"testing"

	"github.com/Shopify/sarama"
)

func TestCheckOffsetsCommitted(t *testing.T) {
	testCases := map[string]struct {
		topics       []string
		topicOffsets map[string]map[int32]int64
		cgOffsets    map[string]map[int32]int64
		committed    bool
	}{
		"one topic, one partition, initialized": {
			topics: []string{"my-topic"},
			topicOffsets: map[string]map[int32]int64{
				"my-topic": {
					0: 5,
				},
			},
			cgOffsets: map[string]map[int32]int64{
				"my-topic": {
					0: 2,
				},
			},
			committed: true,
		},
		"one topic, one partition, uninitialized": {
			topics: []string{"my-topic"},
			topicOffsets: map[string]map[int32]int64{
				"my-topic": {
					0: 5,
				},
			},
			cgOffsets: map[string]map[int32]int64{
				"my-topic": {
					0: -1,
				},
			},
			committed: false,
		},
		"several topics, several partitions, not all initialized": {
			topics: []string{"my-topic", "my-topic-2", "my-topic-3"},
			topicOffsets: map[string]map[int32]int64{
				"my-topic":   {0: 5, 1: 7},
				"my-topic-2": {0: 5, 1: 7, 2: 9},
				"my-topic-3": {0: 5, 1: 7, 2: 2, 3: 10},
			},
			cgOffsets: map[string]map[int32]int64{
				"my-topic":   {0: -1, 1: 7},
				"my-topic-2": {0: 5, 1: -1, 2: -1},
				"my-topic-3": {0: 5, 1: 7, 2: -1, 3: 10},
			},
			committed: false,
		},
	}

	for n, tc := range testCases {
		t.Run(n, func(t *testing.T) {
			broker := sarama.NewMockBroker(t, 1)
			defer broker.Close()

			group := "my-group"

			offsetFetchResponse := sarama.NewMockOffsetFetchResponse(t).SetError(sarama.ErrNoError)
			for topic, partitions := range tc.cgOffsets {
				for partition, offset := range partitions {
					offsetFetchResponse = offsetFetchResponse.SetOffset(group, topic, partition, offset, "", sarama.ErrNoError)
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
				"OffsetFetchRequest": offsetFetchResponse,

				"FindCoordinatorRequest": sarama.NewMockFindCoordinatorResponse(t).
					SetCoordinator(sarama.CoordinatorGroup, group, broker),

				"MetadataRequest": metadataResponse,
			})

			config := sarama.NewConfig()
			config.Version = sarama.MaxVersion

			sc, err := sarama.NewClient([]string{broker.Addr()}, config)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			got, err := CheckOffsetsCommitted(sc, tc.topics, group)

			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			if tc.committed != got {
				t.Errorf("wanted %t, got %t", tc.committed, got)
			}
		})
	}
}
