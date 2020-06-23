package producer

import (
	"crypto/tls"
	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
	"testing"
)

// Test Constants
const (
	KafkaBrokers  = "TestBrokers"
	KafkaUsername = "TestUsername"
	KafkaPassword = "TestPassword"
)

// Mock Producer Reference
var mockSyncProducer sarama.SyncProducer

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
		verifySaramaConfig(t, config, username, password)
		return mockSyncProducer, nil
	}
	defer func() { newSyncProducerWrapper = newSyncProducerWrapperPlaceholder }()

	// Perform The Test
	producer, err := CreateSyncProducer([]string{KafkaBrokers}, username, password)

	// Verify The Results
	assert.Nil(t, err)
	assert.NotNil(t, producer)
	assert.Equal(t, mockSyncProducer, producer)

}

// Verify The Sarama Config Is As Expected
func verifySaramaConfig(t *testing.T, config *sarama.Config, username string, password string) {
	assert.NotNil(t, config)
	assert.Equal(t, sarama.V2_3_0_0, config.Version)
	assert.NotNil(t, config.MetricRegistry) // TODO ???
	assert.True(t, config.Producer.Return.Successes)

	if len(username) > 0 && len(password) > 0 {
		assert.Equal(t, sarama.SASLHandshakeV0, config.Net.SASL.Version) // TODO - not sure which version this will land on?
		assert.True(t, config.Net.SASL.Enable)
		assert.Equal(t, sarama.SASLMechanism(sarama.SASLTypePlaintext), config.Net.SASL.Mechanism)
		assert.Equal(t, username, config.Net.SASL.User)
		assert.Equal(t, password, config.Net.SASL.Password)
		assert.True(t, config.Net.TLS.Enable)
		assert.NotNil(t, config.Net.TLS.Config)
		assert.True(t, config.Net.TLS.Config.InsecureSkipVerify)
		assert.Equal(t, tls.NoClientCert, config.Net.TLS.Config.ClientAuth)
	} else {
		assert.NotNil(t, config.Net)
		assert.False(t, config.Net.TLS.Enable)
		assert.Nil(t, config.Net.TLS.Config)
		assert.Equal(t, sarama.SASLHandshakeV0, config.Net.SASL.Version) // TODO - not sure which version this will land on?
		assert.NotNil(t, config.Net.SASL)
		assert.False(t, config.Net.SASL.Enable)
		assert.Equal(t, sarama.SASLMechanism(""), config.Net.SASL.Mechanism)
		assert.Equal(t, "", config.Net.SASL.User)
		assert.Equal(t, "", config.Net.SASL.Password)
	}
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
