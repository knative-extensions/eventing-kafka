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

package producer

import (
	"testing"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
	producertesting "knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/producer/testing"
)

// Test The CreateSyncProducer() Functionality
func TestCreateSyncProducer(t *testing.T) {

	// Test Data
	brokers := []string{"TestBrokers"}
	config := sarama.NewConfig()

	// Create A Mock SyncProducer, Stub NewSyncProducerWrapper() & Restore After Test
	mockSyncProducer := producertesting.NewMockSyncProducer()
	producertesting.StubNewSyncProducerFn(producertesting.ValidatingNewSyncProducerFn(t, brokers, config, mockSyncProducer))
	defer producertesting.RestoreNewSyncProducerFn()

	// Perform The Test
	producer, err := CreateSyncProducer(brokers, config)

	// Verify The Results
	assert.Nil(t, err)
	assert.Equal(t, mockSyncProducer, producer)
}
