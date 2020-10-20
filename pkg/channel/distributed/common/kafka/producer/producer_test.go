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
	"strconv"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
	"knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/constants"
	kafkasarama "knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/sarama"
	commontesting "knative.dev/eventing-kafka/pkg/channel/distributed/common/testing"
)

// Test Constants
const (
	ClientId      = "TestClientId"
	KafkaBrokers  = "TestBrokers"
	KafkaUsername = "TestUsername"
	KafkaPassword = "TestPassword"
)

// Test The CreateSyncProducer() Functionality
func TestCreateSyncProducer(t *testing.T) {
	performCreateSyncProducerTest(t, "", "")
}

// Test The CreateSyncProducer() Functionality With SASL PLAIN Auth
func TestCreateSyncProducerWithSasl(t *testing.T) {
	performCreateSyncProducerTest(t, KafkaUsername, KafkaPassword)
}

// Perform A Single Instance Of The CreateProducer() Test With Specified Config
func performCreateSyncProducerTest(t *testing.T, username string, password string) {

	// Create A Mock SyncProducer
	mockSyncProducer := &MockSyncProducer{}

	// Stub The Kafka SyncProducer Creation Wrapper With Test Version Returning Mock SyncProducer
	newSyncProducerWrapperPlaceholder := newSyncProducerWrapper
	newSyncProducerWrapper = func(brokers []string, config *sarama.Config) (sarama.SyncProducer, error) {
		assert.NotNil(t, brokers)
		assert.Len(t, brokers, 1)
		assert.Equal(t, brokers[0], KafkaBrokers)
		verifySaramaConfig(t, config, ClientId, username, password)
		return mockSyncProducer, nil
	}
	defer func() { newSyncProducerWrapper = newSyncProducerWrapperPlaceholder }()

	// Perform The Test
	config := commontesting.GetDefaultSaramaConfig(t)
	kafkasarama.UpdateSaramaConfig(config, ClientId, username, password)
	producer, registry, err := CreateSyncProducer([]string{KafkaBrokers}, config)

	// Verify The Results
	assert.Nil(t, err)
	assert.NotNil(t, producer)
	assert.Equal(t, mockSyncProducer, producer)
	assert.NotNil(t, registry)
}

// Test that the UpdateSaramaConfig sets values as expected
func TestUpdateConfig(t *testing.T) {
	config := sarama.NewConfig()
	kafkasarama.UpdateSaramaConfig(config, ClientId, KafkaUsername, KafkaPassword)
	assert.Equal(t, ClientId, config.ClientID)
	assert.Equal(t, KafkaUsername, config.Net.SASL.User)
	assert.Equal(t, KafkaPassword, config.Net.SASL.Password)
}

// Verify The Sarama Config Is As Expected
func verifySaramaConfig(t *testing.T, config *sarama.Config, clientId string, username string, password string) {
	assert.NotNil(t, config)
	assert.Equal(t, clientId, config.ClientID)
	assert.NotNil(t, config.Net)
	assert.NotNil(t, config.Net.SASL)
	assert.Equal(t, username, config.Net.SASL.User)
	assert.Equal(t, password, config.Net.SASL.Password)
	assert.Equal(t, constants.ConfigKafkaVersionDefault, config.Version)
	assert.Equal(t, commontesting.ConfigNetKeepAlive, strconv.FormatInt(int64(config.Net.KeepAlive), 10))
	assert.Equal(t, commontesting.ConfigProducerIdempotent, strconv.FormatBool(config.Producer.Idempotent))
	assert.Equal(t, commontesting.ConfigProducerRequiredAcks, strconv.FormatInt(int64(config.Producer.RequiredAcks), 10))
	assert.True(t, config.Producer.Return.Successes)
	assert.Equal(t, commontesting.ConfigMetadataRefreshFrequency, strconv.FormatInt(int64(config.Metadata.RefreshFrequency), 10))
}

//
// Mock Sarama SyncProducer Implementation
//

var _ sarama.SyncProducer = &MockSyncProducer{}

type MockSyncProducer struct {
}

func (p *MockSyncProducer) SendMessage(msg *sarama.ProducerMessage) (partition int32, offset int64, err error) {
	return 0, 0, nil
}

func (p *MockSyncProducer) SendMessages(msgs []*sarama.ProducerMessage) error {
	return nil
}

func (p *MockSyncProducer) Close() error {
	return nil
}
