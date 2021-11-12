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

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
	"knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/producer/wrapper"
)

//
// Test Utilities For Stubbing The NewSyncProducerFn
//

// Replace The NewSyncProducerFn With Specified Mock / Test Value
func StubNewSyncProducerFn(stubNewSyncProducerFn wrapper.NewSyncProducerFnType) {
	wrapper.NewSyncProducerFn = stubNewSyncProducerFn
}

// Restore The NewSyncProducerFn To Official Production Value
func RestoreNewSyncProducerFn() {
	wrapper.NewSyncProducerFn = wrapper.SaramaNewSyncProducerWrapper
}

// Non-Validating NewSyncProducer Function
func NonValidatingNewSyncProducerFn(mockSyncProducer sarama.SyncProducer) wrapper.NewSyncProducerFnType {
	return func(brokers []string, config *sarama.Config) (sarama.SyncProducer, error) {
		return mockSyncProducer, nil
	}
}

// Validating NewSyncProducer Function
func ValidatingNewSyncProducerFn(t *testing.T,
	expectedBrokers []string,
	expectedConfig *sarama.Config,
	mockSyncProducer sarama.SyncProducer) wrapper.NewSyncProducerFnType {

	return func(brokers []string, config *sarama.Config) (sarama.SyncProducer, error) {
		assert.Equal(t, brokers, expectedBrokers)
		assert.Equal(t, config, expectedConfig)
		return mockSyncProducer, nil
	}
}
