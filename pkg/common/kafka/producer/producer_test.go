package producer

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/kyma-incubator/knative-kafka/pkg/common/kafka/constants"
	"github.com/stretchr/testify/assert"
	"testing"
)

// Mock Producer Reference
var mockProducer ProducerInterface

// Test The CreateProducer() Functionality
func TestCreateProducer(t *testing.T) {
	performCreateProducerTest(t, "TestBrokers", "", "")
}

// Test The CreateProducer() Functionality With SASL Auth
func TestCreateProducerWithSaslAuth(t *testing.T) {
	performCreateProducerTest(t, "TestBrokers", "TestUsername", "TestPassword")
}

// Perform A Single Instance Of The CreateProducer() Test With Specified Config
func performCreateProducerTest(t *testing.T, brokers string, username string, password string) {

	// Create The Expected Kafka Producer ConfigMap
	expectedConfigMap := createKafkaProducerConfigMap(t, brokers, username, password)

	// Replace The NewProducer Wrapper To Provide Mock Producer & Defer Reset
	newProducerWrapperPlaceholder := NewProducerWrapper
	NewProducerWrapper = func(configMap *kafka.ConfigMap) (ProducerInterface, error) {
		assert.Equal(t, expectedConfigMap, configMap)
		mockProducer = &MockProducer{}
		return mockProducer, nil
	}
	defer func() { NewProducerWrapper = newProducerWrapperPlaceholder }()

	// Perform The Test
	producer, err := CreateProducer(brokers, username, password)

	// Verify The Results
	assert.Nil(t, err)
	assert.Equal(t, mockProducer, producer)
}

// Create The Specified Kafka Producer ConfigMap
func createKafkaProducerConfigMap(t *testing.T, brokers string, username string, password string) *kafka.ConfigMap {

	// Create The Expected Kafka Base ConfigMap
	configMap := &kafka.ConfigMap{
		constants.ConfigPropertyBootstrapServers:      brokers,
		constants.ConfigPropertyPartitioner:           constants.ConfigPropertyPartitionerValue,
		constants.ConfigPropertyIdempotence:           constants.ConfigPropertyIdempotenceValue,
		constants.ConfigPropertyStatisticsInterval:    constants.ConfigPropertyStatisticsIntervalValue,
		constants.ConfigPropertyRequestTimeoutMs:      constants.ConfigPropertyRequestTimeoutMsValue,
		constants.ConfigPropertyMetadataMaxAgeMs:      constants.ConfigPropertyMetadataMaxAgeMsValue,
		constants.ConfigPropertySocketKeepAliveEnable: constants.ConfigPropertySocketKeepAliveEnableValue,
	}

	// Optionally Add The SASL ConfigMap Entries
	if len(username) > 0 && len(password) > 0 {
		assert.Nil(t, configMap.SetKey(constants.ConfigPropertySecurityProtocol, constants.ConfigPropertySecurityProtocolValue))
		assert.Nil(t, configMap.SetKey(constants.ConfigPropertySaslMechanisms, constants.ConfigPropertySaslMechanismsPlain))
		assert.Nil(t, configMap.SetKey(constants.ConfigPropertySaslUsername, username))
		assert.Nil(t, configMap.SetKey(constants.ConfigPropertySaslPassword, password))
	}

	// Return The Populated Kafka ConfigMap
	return configMap
}

//
// Mock ProducerInterface
//

var _ ProducerInterface = &MockProducer{}

type MockProducer struct {
}

// String returns a human readable name for a Producer instance
func (p MockProducer) String() string {
	return ""
}

func (p MockProducer) Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error {
	return nil
}

func (p MockProducer) Events() chan kafka.Event {
	return nil
}

func (p MockProducer) ProduceChannel() chan *kafka.Message {
	return nil
}

func (p MockProducer) Len() int {
	return 0
}

func (p MockProducer) Flush(timeoutMs int) int {
	return 0
}

func (p MockProducer) Close() {
	return
}
