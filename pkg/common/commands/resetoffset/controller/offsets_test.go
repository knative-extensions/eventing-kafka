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
	"testing"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
	"knative.dev/pkg/logging"
	logtesting "knative.dev/pkg/logging/testing"

	kafkav1alpha1 "knative.dev/eventing-kafka/pkg/apis/kafka/v1alpha1"
	controllertesting "knative.dev/eventing-kafka/pkg/common/commands/resetoffset/controller/testing"
)

//
// Test The Kafka Offset Reconciliation
//
// The Knative Eventing TableTest framework traditionally used to test Reconciliation is exclusively
// focused on verifying the pre/post state of Kubernetes resources, and does not allow for easy
// verification of other external interactions such as the Kafka Offset management happening here.
// Therefore we will verify the Offset Reconciliation independently from the larger Reconciler testing.
//
func TestReconciler_ReconcileOffsets(t *testing.T) {

	// Test Data
	kafkaBrokers := []string{controllertesting.Brokers}
	saramaConfig := sarama.NewConfig()
	topicName := controllertesting.TopicName
	groupId := controllertesting.GroupId
	testErr := fmt.Errorf("test-error")

	partition1 := int32(0)
	oldOffset1 := int64(100)
	newPastOffset1 := oldOffset1 - 50
	newFutureOffset1 := oldOffset1 + 50

	partition2 := int32(1)
	oldOffset2 := int64(200)
	newPastOffset2 := oldOffset2 - 50
	newFutureOffset2 := oldOffset2 + 50

	offsetTime := int64(123456789)
	metadata := formatOffsetMetaData(offsetTime)

	// Define The Test Cases
	tests := []struct {
		name                    string
		client                  *controllertesting.MockClient
		offsetManager           *controllertesting.MockOffsetManager
		partitionOffsetManagers map[int32]*controllertesting.MockPartitionOffsetManager
		expectedOffsetMappings  []kafkav1alpha1.OffsetMapping
		expectedErr             error
	}{
		//
		// Success Tests
		//

		{
			name: "Successful MarkOffset",
			client: controllertesting.NewMockClient(
				controllertesting.WithClientMockPartitions(topicName, []int32{partition1, partition2}, nil),
				controllertesting.WithClientMockGetOffset(topicName, partition1, offsetTime, newFutureOffset1, nil),
				controllertesting.WithClientMockGetOffset(topicName, partition2, offsetTime, newFutureOffset2, nil),
				controllertesting.WithClientMockClosed(false),
				controllertesting.WithClientMockClose(nil)),
			offsetManager: controllertesting.NewMockOffsetManager(
				controllertesting.WithOffsetManagerMockCommit(),
				controllertesting.WithOffsetManagerMockClose(nil)),
			partitionOffsetManagers: map[int32]*controllertesting.MockPartitionOffsetManager{
				partition1: controllertesting.NewMockPartitionOffsetManager(
					controllertesting.WithPartitionOffsetManagerMockNextOffset(oldOffset1, ""),
					controllertesting.WithPartitionOffsetManagerMockMarkOffset(newFutureOffset1, metadata),
					controllertesting.WithPartitionOffsetManagerMockErrors(),
					controllertesting.WithPartitionOffsetManagerMockAsyncClose()),
				partition2: controllertesting.NewMockPartitionOffsetManager(
					controllertesting.WithPartitionOffsetManagerMockNextOffset(oldOffset2, ""),
					controllertesting.WithPartitionOffsetManagerMockMarkOffset(newFutureOffset2, metadata),
					controllertesting.WithPartitionOffsetManagerMockErrors(),
					controllertesting.WithPartitionOffsetManagerMockAsyncClose()),
			},
			expectedOffsetMappings: []kafkav1alpha1.OffsetMapping{
				{Partition: partition1, OldOffset: oldOffset1, NewOffset: newFutureOffset1},
				{Partition: partition2, OldOffset: oldOffset2, NewOffset: newFutureOffset2},
			},
			expectedErr: nil,
		},
		{
			name: "Successful ResetOffset",
			client: controllertesting.NewMockClient(
				controllertesting.WithClientMockPartitions(topicName, []int32{partition1, partition2}, nil),
				controllertesting.WithClientMockGetOffset(topicName, partition1, offsetTime, newPastOffset1, nil),
				controllertesting.WithClientMockGetOffset(topicName, partition2, offsetTime, newPastOffset2, nil),
				controllertesting.WithClientMockClosed(false),
				controllertesting.WithClientMockClose(nil)),
			offsetManager: controllertesting.NewMockOffsetManager(
				controllertesting.WithOffsetManagerMockCommit(),
				controllertesting.WithOffsetManagerMockClose(nil)),
			partitionOffsetManagers: map[int32]*controllertesting.MockPartitionOffsetManager{
				partition1: controllertesting.NewMockPartitionOffsetManager(
					controllertesting.WithPartitionOffsetManagerMockNextOffset(oldOffset1, ""),
					controllertesting.WithPartitionOffsetManagerMockResetOffset(newPastOffset1, metadata),
					controllertesting.WithPartitionOffsetManagerMockErrors(),
					controllertesting.WithPartitionOffsetManagerMockAsyncClose()),
				partition2: controllertesting.NewMockPartitionOffsetManager(
					controllertesting.WithPartitionOffsetManagerMockNextOffset(oldOffset2, ""),
					controllertesting.WithPartitionOffsetManagerMockResetOffset(newPastOffset2, metadata),
					controllertesting.WithPartitionOffsetManagerMockErrors(),
					controllertesting.WithPartitionOffsetManagerMockAsyncClose()),
			},
			expectedOffsetMappings: []kafkav1alpha1.OffsetMapping{
				{Partition: partition1, OldOffset: oldOffset1, NewOffset: newPastOffset1},
				{Partition: partition2, OldOffset: oldOffset2, NewOffset: newPastOffset2},
			},
			expectedErr: nil,
		},

		//
		// Sarama Error Tests
		//

		{
			name:                   "SaramaNewClientFn() Error",
			client:                 nil,
			expectedOffsetMappings: nil,
			expectedErr:            testErr,
		},
		{
			name: "SaramaNewOffsetManagerFromClientFn() Error",
			client: controllertesting.NewMockClient(
				controllertesting.WithClientMockPartitions(topicName, []int32{partition1, partition2}, nil),
				controllertesting.WithClientMockClosed(false),
				controllertesting.WithClientMockClose(nil)),
			offsetManager:          nil,
			expectedOffsetMappings: nil,
			expectedErr:            testErr,
		},

		//
		// Client Error Tests
		//

		{
			name: "Client.Partitions() Error",
			client: controllertesting.NewMockClient(
				controllertesting.WithClientMockPartitions(topicName, nil, testErr),
				controllertesting.WithClientMockClosed(false),
				controllertesting.WithClientMockClose(nil)),
			expectedOffsetMappings: nil,
			expectedErr:            testErr,
		},
		{
			name: "Client.GetOffset() Error",
			client: controllertesting.NewMockClient(
				controllertesting.WithClientMockPartitions(topicName, []int32{partition1}, nil),
				controllertesting.WithClientMockGetOffset(topicName, partition1, offsetTime, 0, testErr),
				controllertesting.WithClientMockClosed(true)),
			offsetManager: controllertesting.NewMockOffsetManager(
				controllertesting.WithOffsetManagerMockClose(nil)),
			expectedOffsetMappings: []kafkav1alpha1.OffsetMapping{{Partition: partition1, OldOffset: 0, NewOffset: 0}},
			expectedErr:            updateOffsetsError,
		},
		{
			name: "Client.Close() Error",
			client: controllertesting.NewMockClient(
				controllertesting.WithClientMockPartitions(topicName, []int32{partition1}, nil),
				controllertesting.WithClientMockGetOffset(topicName, partition1, offsetTime, newPastOffset1, nil),
				controllertesting.WithClientMockClosed(false),
				controllertesting.WithClientMockClose(testErr)),
			offsetManager: controllertesting.NewMockOffsetManager(
				controllertesting.WithOffsetManagerMockCommit(),
				controllertesting.WithOffsetManagerMockClose(nil)),
			partitionOffsetManagers: map[int32]*controllertesting.MockPartitionOffsetManager{
				partition1: controllertesting.NewMockPartitionOffsetManager(
					controllertesting.WithPartitionOffsetManagerMockNextOffset(oldOffset1, ""),
					controllertesting.WithPartitionOffsetManagerMockResetOffset(newPastOffset1, metadata),
					controllertesting.WithPartitionOffsetManagerMockErrors(),
					controllertesting.WithPartitionOffsetManagerMockAsyncClose()),
			},
			expectedOffsetMappings: []kafkav1alpha1.OffsetMapping{{Partition: partition1, OldOffset: oldOffset1, NewOffset: newPastOffset1}},
			expectedErr:            nil,
		},

		//
		// OffsetManager Error Tests
		//

		{
			name: "OffsetManager.ManagePartition() Error",
			client: controllertesting.NewMockClient(
				controllertesting.WithClientMockPartitions(topicName, []int32{partition1}, nil),
				controllertesting.WithClientMockGetOffset(topicName, partition1, offsetTime, newPastOffset1, nil),
				controllertesting.WithClientMockClosed(true)),
			offsetManager: controllertesting.NewMockOffsetManager(
				controllertesting.WithOffsetManagerMockManagePartition(topicName, partition1,
					controllertesting.NewMockPartitionOffsetManager(controllertesting.WithPartitionOffsetManagerMockAsyncClose()), testErr),
				controllertesting.WithOffsetManagerMockClose(nil)),
			expectedOffsetMappings: []kafkav1alpha1.OffsetMapping{{Partition: partition1, OldOffset: 0, NewOffset: 0}},
			expectedErr:            updateOffsetsError,
		},
		{
			name: "OffsetManager.Close() Error",
			client: controllertesting.NewMockClient(
				controllertesting.WithClientMockPartitions(topicName, []int32{partition1}, nil),
				controllertesting.WithClientMockGetOffset(topicName, partition1, offsetTime, newPastOffset1, nil),
				controllertesting.WithClientMockClosed(true)),
			offsetManager: controllertesting.NewMockOffsetManager(
				controllertesting.WithOffsetManagerMockCommit(),
				controllertesting.WithOffsetManagerMockClose(testErr)),
			partitionOffsetManagers: map[int32]*controllertesting.MockPartitionOffsetManager{
				partition1: controllertesting.NewMockPartitionOffsetManager(
					controllertesting.WithPartitionOffsetManagerMockNextOffset(oldOffset1, ""),
					controllertesting.WithPartitionOffsetManagerMockResetOffset(newPastOffset1, metadata),
					controllertesting.WithPartitionOffsetManagerMockErrors(),
					controllertesting.WithPartitionOffsetManagerMockAsyncClose()),
			},
			expectedOffsetMappings: []kafkav1alpha1.OffsetMapping{{Partition: partition1, OldOffset: oldOffset1, NewOffset: newPastOffset1}},
			expectedErr:            nil,
		},

		//
		// PartitionsOffsetManager Error Tests
		//

		{
			name: "PartitionsOffsetManager.Errors()",
			client: controllertesting.NewMockClient(
				controllertesting.WithClientMockPartitions(topicName, []int32{partition1, partition2}, nil),
				controllertesting.WithClientMockGetOffset(topicName, partition1, offsetTime, newPastOffset1, nil),
				controllertesting.WithClientMockGetOffset(topicName, partition2, offsetTime, newPastOffset2, nil),
				controllertesting.WithClientMockClosed(true)),
			offsetManager: controllertesting.NewMockOffsetManager(
				controllertesting.WithOffsetManagerMockCommit(),
				controllertesting.WithOffsetManagerMockClose(nil)),
			partitionOffsetManagers: map[int32]*controllertesting.MockPartitionOffsetManager{
				partition1: controllertesting.NewMockPartitionOffsetManager(
					controllertesting.WithPartitionOffsetManagerMockNextOffset(oldOffset1, ""),
					controllertesting.WithPartitionOffsetManagerMockResetOffset(newPastOffset1, metadata),
					controllertesting.WithPartitionOffsetManagerMockErrors(&sarama.ConsumerError{
						Topic:     topicName,
						Partition: partition1,
						Err:       testErr,
					}),
					controllertesting.WithPartitionOffsetManagerMockAsyncClose()),
				partition2: controllertesting.NewMockPartitionOffsetManager(
					controllertesting.WithPartitionOffsetManagerMockNextOffset(oldOffset2, ""),
					controllertesting.WithPartitionOffsetManagerMockResetOffset(newPastOffset2, metadata),
					controllertesting.WithPartitionOffsetManagerMockErrors(&sarama.ConsumerError{
						Topic:     topicName,
						Partition: partition2,
						Err:       testErr,
					}),
					controllertesting.WithPartitionOffsetManagerMockAsyncClose()),
			},
			expectedOffsetMappings: []kafkav1alpha1.OffsetMapping{
				{Partition: partition1, OldOffset: oldOffset1, NewOffset: newPastOffset1},
				{Partition: partition2, OldOffset: oldOffset2, NewOffset: newPastOffset2},
			},
			expectedErr: updateOffsetsError,
		},
	}

	// Execute The Test Cases
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {

			// Create A Context With Test Logger
			logger := logtesting.TestLogger(t)
			ctx := logging.WithLogger(context.Background(), logger)

			// Stub The Sarama NewClient() Implementation To Return Mock Sarama Client
			if test.client == nil {
				stubSaramaNewClientFn(t, kafkaBrokers, saramaConfig, nil, testErr)
			} else {
				stubSaramaNewClientFn(t, kafkaBrokers, saramaConfig, test.client, nil)
			}
			defer restoreSaramaNewClientFn()

			// Stub The Sarama NewOffsetManagerFromClient() Implementation To Return Mock Sarama OffsetManager
			if test.offsetManager == nil {
				stubSaramaNewOffsetManagerFromClientFn(t, groupId, test.client, nil, testErr)
			} else {
				stubSaramaNewOffsetManagerFromClientFn(t, groupId, test.client, test.offsetManager, nil)
			}
			defer restoreSaramaNewOffsetManagerFromClientFn()

			// Configure The Test OffsetManager With Partitions
			for partition, partitionOffsetManager := range test.partitionOffsetManagers {
				controllertesting.WithOffsetManagerMockManagePartition(topicName, partition, partitionOffsetManager, nil)(test.offsetManager)
			}

			// Create A Reconciler
			reconciler := &Reconciler{
				kafkaBrokers:      kafkaBrokers,
				saramaConfig:      saramaConfig,
				resetoffsetLister: nil,
				refMapper:         nil,
			}

			// Perform The Test
			offsetMappings, err := reconciler.reconcileOffsets(ctx, topicName, groupId, offsetTime)

			// Verify The Results
			assert.Equal(t, test.expectedErr, err)
			assert.Equal(t, test.expectedOffsetMappings, offsetMappings)
			if test.client != nil {
				test.client.AssertExpectations(t)
			}
			if test.offsetManager != nil {
				test.offsetManager.AssertExpectations(t)
			}
			if len(test.partitionOffsetManagers) > 0 {
				for _, partitionOffsetManager := range test.partitionOffsetManagers {
					partitionOffsetManager.AssertExpectations(t)
				}
			}
		})
	}
}

//
// Stubbing Utilities
//

// stubSaramaNewClientFn replaces the Sarama NewClient function with a test instance which performs
// validation and returns the specified parameters.
func stubSaramaNewClientFn(t *testing.T, expectedBrokers []string, expectedConfig *sarama.Config, saramaClient sarama.Client, err error) {
	SaramaNewClientFn = func(brokers []string, config *sarama.Config) (sarama.Client, error) {
		assert.Equal(t, expectedBrokers, brokers)
		assert.Equal(t, expectedConfig, config)
		return saramaClient, err
	}
}

// restoreSaramaNewClient restores the default/official Sarama NewClient function.
func restoreSaramaNewClientFn() {
	SaramaNewClientFn = sarama.NewClient
}

// stubSaramaNewOffsetManagerFromClientFn replaces the Sarama NewOffsetManagerFromClient function
// with a test instance which performs validation and returns the specified parameters.
func stubSaramaNewOffsetManagerFromClientFn(t *testing.T, expectedGroupId string, expectedClient sarama.Client, offsetManager sarama.OffsetManager, err error) {
	SaramaNewOffsetManagerFromClientFn = func(groupId string, client sarama.Client) (sarama.OffsetManager, error) {
		assert.Equal(t, expectedGroupId, groupId)
		assert.Equal(t, expectedClient, client)
		return offsetManager, err
	}
}

// restoreSaramaNewOffsetManagerFromClientFn restores the default/official Sarama NewOffsetManagerFromClient function.
func restoreSaramaNewOffsetManagerFromClientFn() {
	SaramaNewOffsetManagerFromClientFn = sarama.NewOffsetManagerFromClient
}
