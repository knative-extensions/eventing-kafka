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

package testing

import (
	"testing"

	"knative.dev/eventing-kafka/pkg/common/consumer"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
	"knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/consumer/wrapper"
)

//
// Test Utilities For Stubbing The NewConsumerGroupFn
//

// Replace The NewConsumerGroupFn With Specified Mock / Test Value
func StubNewConsumerGroupFn(stubNewConsumerGroupFn consumer.NewConsumerGroupFnType) {
	wrapper.NewConsumerGroupFn = stubNewConsumerGroupFn
}

// Restore The NewConsumerGroupFn To Official Production Value
func RestoreNewConsumerGroupFn() {
	wrapper.NewConsumerGroupFn = wrapper.SaramaNewConsumerGroupWrapper
}

// Non-Validating NewConsumerGroup Function
func NonValidatingNewConsumerGroupFn(mockConsumerGroup sarama.ConsumerGroup) consumer.NewConsumerGroupFnType {
	return func(brokers []string, groupId string, config *sarama.Config) (sarama.ConsumerGroup, error) {
		return mockConsumerGroup, nil
	}
}

// Validating NewConsumerGroup Function
func ValidatingNewConsumerGroupFn(t *testing.T,
	expectedBrokers []string,
	expectedGroupId string,
	expectedConfig *sarama.Config,
	mockConsumerGroup sarama.ConsumerGroup) consumer.NewConsumerGroupFnType {

	return func(brokers []string, groupId string, config *sarama.Config) (sarama.ConsumerGroup, error) {
		assert.Equal(t, expectedBrokers, brokers)
		assert.Equal(t, expectedGroupId, groupId)
		assert.Equal(t, expectedConfig, config)
		return mockConsumerGroup, nil
	}
}
