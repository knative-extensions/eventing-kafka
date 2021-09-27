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

package offset

import (
	"testing"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"

	logtesting "knative.dev/pkg/logging/testing"
)

func TestInitOffsets(t *testing.T) {
	testCases := map[string]struct {
		topics       []string
		topicOffsets map[string]map[int32]int64
		cgOffsets    map[string]map[int32]int64
		wantCommit   bool
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
			wantCommit: false,
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
			wantCommit: true,
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
			wantCommit: true,
		},
	}

	for n, tc := range testCases {
		t.Run(n, func(t *testing.T) {
			broker := sarama.NewMockBroker(t, 1)
			defer broker.Close()

			group := "my-group"

			offsetResponse := sarama.NewMockOffsetResponse(t).SetVersion(1)
			for topic, partitions := range tc.topicOffsets {
				for partition, offset := range partitions {
					offsetResponse = offsetResponse.SetOffset(topic, partition, -1, offset)
				}
			}

			offsetFetchResponse := sarama.NewMockOffsetFetchResponse(t).SetError(sarama.ErrNoError)
			for topic, partitions := range tc.cgOffsets {
				for partition, offset := range partitions {
					offsetFetchResponse = offsetFetchResponse.SetOffset(group, topic, partition, offset, "", sarama.ErrNoError)
				}
			}

			offsetCommitResponse := sarama.NewMockOffsetCommitResponse(t)
			serr := sarama.ErrNoError
			if !tc.wantCommit {
				serr = sarama.ErrUnknown // could be anything

			}

			for topic, partitions := range tc.cgOffsets {
				for partition := range partitions {
					offsetCommitResponse = offsetCommitResponse.SetError(group, topic, partition, serr)
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
				"OffsetRequest":       offsetResponse,
				"OffsetFetchRequest":  offsetFetchResponse,
				"OffsetCommitRequest": offsetCommitResponse,

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
			defer sc.Close()

			kac, err := sarama.NewClusterAdminFromClient(sc)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			defer kac.Close()

			ctx := logtesting.TestContextWithLogger(t)
			partitionCt, err := InitOffsets(ctx, sc, kac, tc.topics, group)
			total := 0
			for _, partitions := range tc.topicOffsets {
				total += len(partitions)
			}
			assert.Equal(t, int(partitionCt), total)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}

		})
	}

}

func TestCheckIfAllOffsetsInitialized(t *testing.T) {
	testCases := map[string]struct {
		topics       []string
		topicOffsets map[string]map[int32]int64
		cgOffsets    map[string]map[int32]int64
		result       bool
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
			result: true,
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
			result: false,
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
			result: false,
		},
	}

	for n, tc := range testCases {
		t.Run(n, func(t *testing.T) {
			broker := sarama.NewMockBroker(t, 1)
			defer broker.Close()

			group := "my-group"

			offsetResponse := sarama.NewMockOffsetResponse(t).SetVersion(1)
			for topic, partitions := range tc.topicOffsets {
				for partition, offset := range partitions {
					offsetResponse = offsetResponse.SetOffset(topic, partition, -1, offset)
				}
			}

			offsetFetchResponse := sarama.NewMockOffsetFetchResponse(t).SetError(sarama.ErrNoError)
			for topic, partitions := range tc.cgOffsets {
				for partition, offset := range partitions {
					offsetFetchResponse = offsetFetchResponse.SetOffset(group, topic, partition, offset, "", sarama.ErrNoError)
				}
			}

			offsetCommitResponse := sarama.NewMockOffsetCommitResponse(t)
			serr := sarama.ErrNoError
			if !tc.result {
				serr = sarama.ErrUnknown // could be anything

			}

			for topic, partitions := range tc.cgOffsets {
				for partition := range partitions {
					offsetCommitResponse = offsetCommitResponse.SetError(group, topic, partition, serr)
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
				"OffsetRequest":       offsetResponse,
				"OffsetFetchRequest":  offsetFetchResponse,
				"OffsetCommitRequest": offsetCommitResponse,

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
			defer sc.Close()

			kac, err := sarama.NewClusterAdminFromClient(sc)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			defer kac.Close()

			retrieved, err := CheckIfAllOffsetsInitialized(sc, kac, tc.topics, group)
			assert.Equal(t, retrieved, tc.result)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}

		})
	}
}
