package consumer

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"knative.dev/eventing-kafka/pkg/common/kafka/constants"
	"github.com/stretchr/testify/assert"
	"testing"
)

// Mock Producer Reference
var mockConsumer ConsumerInterface

// Test The CreateConsumer() Functionality
func TestCreateConsumer(t *testing.T) {
	performCreateConsumerTest(t, "TestBrokers", "TestGroupId", "TestOffset", "", "")
}

// Test The CreateConsumer() Functionality With SASL Auth
func TestCreateConsumerWithSaslAuth(t *testing.T) {
	performCreateConsumerTest(t, "TestBrokers", "TestGroupId", "TestOffset", "TestUsername", "TestPassword")
}

// Perform A Single Instance Of The CreateConsumer() Test With Specified Config
func performCreateConsumerTest(t *testing.T, brokers string, groupId string, offset string, username string, password string) {

	// Create The Expected Kafka Consumer ConfigMap
	expectedConfigMap := createKafkaConsumerConfigMap(t, brokers, groupId, offset, username, password)

	// Replace The NewConsumer Wrapper To Perform Validation, Provide Mock Consumer & Defer Reset
	newConsumerWrapperPlaceholder := NewConsumerWrapper
	NewConsumerWrapper = func(configMap *kafka.ConfigMap) (ConsumerInterface, error) {
		assert.Equal(t, expectedConfigMap, configMap)
		mockConsumer = &MockConsumer{}
		return mockConsumer, nil
	}
	defer func() { NewConsumerWrapper = newConsumerWrapperPlaceholder }()

	// Perform The Test
	consumer, err := CreateConsumer(brokers, groupId, offset, username, password)

	// Verify The Results
	assert.Nil(t, err)
	assert.Equal(t, mockConsumer, consumer)
}

// Create The Specified Kafka ConfigMap
func createKafkaConsumerConfigMap(t *testing.T, brokers string, groupId string, offset string, username string, password string) *kafka.ConfigMap {

	// Create The Expected Kafka Base ConfigMap
	configMap := &kafka.ConfigMap{
		constants.ConfigPropertyBootstrapServers:        brokers,
		constants.ConfigPropertyBrokerAddressFamily:     constants.ConfigPropertyBrokerAddressFamilyValue,
		constants.ConfigPropertyGroupId:                 groupId,
		constants.ConfigPropertyEnableAutoOffsetStore:   constants.ConfigPropertyEnableAutoOffsetStoreValue,
		constants.ConfigPropertyEnableAutoOffsetCommit:  constants.ConfigPropertyEnableAutoOffsetCommitValue,
		constants.ConfigPropertyAutoOffsetReset:         offset,
		constants.ConfigPropertyQueuedMaxMessagesKbytes: constants.ConfigPropertyQueuedMaxMessagesKbytesValue,
		constants.ConfigPropertyStatisticsInterval:      constants.ConfigPropertyStatisticsIntervalValue,
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
// Mock ConsumerInterface
//

var _ ConsumerInterface = &MockConsumer{}

type MockConsumer struct {
}

func (mc *MockConsumer) StoreOffsets(offsets []kafka.TopicPartition) (storedOffsets []kafka.TopicPartition, err error) {
	return nil, nil
}

func (mc *MockConsumer) Commit() ([]kafka.TopicPartition, error) {
	return nil, nil
}

func (mc *MockConsumer) QueryWatermarkOffsets(topic string, partition int32, timeoutMs int) (low, high int64, err error) {
	return 0, 0, nil
}

func (mc *MockConsumer) OffsetsForTimes(times []kafka.TopicPartition, timeoutMs int) (offsets []kafka.TopicPartition, err error) {
	return nil, nil
}

func (mc *MockConsumer) GetMetadata(topic *string, allTopics bool, timeoutMs int) (*kafka.Metadata, error) {
	return nil, nil
}

func (mc *MockConsumer) CommitOffsets(offsets []kafka.TopicPartition) ([]kafka.TopicPartition, error) {
	return nil, nil
}

func (mc *MockConsumer) Poll(timeout int) kafka.Event {
	return nil
}

func (mc *MockConsumer) CommitMessage(*kafka.Message) ([]kafka.TopicPartition, error) {
	return nil, nil
}

func (mc *MockConsumer) Close() error {
	return nil
}

func (mc *MockConsumer) Subscribe(topic string, rebalanceCb kafka.RebalanceCb) error {
	return nil
}
