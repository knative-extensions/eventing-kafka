/*
Copyright 2020 The Knative Authors

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

package consumer

import (
	"testing"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
	consumertesting "knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/consumer/testing"
	kafkatesting "knative.dev/eventing-kafka/pkg/common/kafka/testing"
)

// Test The CreateConsumerGroup() Functionality
func TestCreateConsumerGroup(t *testing.T) {

	// Test Data
	brokers := []string{"TestBrokers"}
	groupId := "TestGroupId"
	config := sarama.NewConfig()

	// Create A Mock ConsumerGroup, Stub NewConsumerGroupWrapper() & Restore After Test
	mockConsumerGroup := kafkatesting.NewMockConsumerGroup()
	consumertesting.StubNewConsumerGroupFn(consumertesting.ValidatingNewConsumerGroupFn(t, brokers, groupId, config, mockConsumerGroup))
	defer consumertesting.RestoreNewConsumerGroupFn()

	// Perform The Test
	consumerGroup, err := CreateConsumerGroup(brokers, groupId, config)

	// Verify The Results
	assert.Nil(t, err)
	assert.Equal(t, mockConsumerGroup, consumerGroup)
}
