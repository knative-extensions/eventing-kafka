package consumer

import (
	"errors"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"knative.dev/eventing-kafka/pkg/common/kafka/constants"
	"knative.dev/eventing-kafka/pkg/common/kafka/util"
)

// Confluent Client Doesn't Code To Interfaces Or Provide Mocks So We're Wrapping Our Usage Of The Consumer For Testing
type ConsumerInterface interface {
	Subscribe(topic string, rebalanceCb kafka.RebalanceCb) error
	Poll(int) kafka.Event
	CommitMessage(*kafka.Message) ([]kafka.TopicPartition, error)
	Close() error
	OffsetsForTimes(times []kafka.TopicPartition, timeoutMs int) (offsets []kafka.TopicPartition, err error)
	GetMetadata(topic *string, allTopics bool, timeoutMs int) (*kafka.Metadata, error)
	CommitOffsets(offsets []kafka.TopicPartition) ([]kafka.TopicPartition, error)
	QueryWatermarkOffsets(topic string, partition int32, timeoutMs int) (low, high int64, err error)
	StoreOffsets(offsets []kafka.TopicPartition) (storedOffsets []kafka.TopicPartition, err error)
	Commit() ([]kafka.TopicPartition, error)
}

// Create A Kafka Consumer (Optional SASL Authentication)
func CreateConsumer(brokers string, groupId string, offset string, username string, password string) (ConsumerInterface, error) {

	// Validate Parameters
	if len(brokers) <= 0 ||
		len(groupId) <= 0 ||
		len(offset) <= 0 {
		return nil, errors.New("required parameters not provided")
	}

	// Create The Kafka Consumer Configuration
	configMap := getBaseConsumerConfigMap(brokers, groupId, offset)

	// Append SASL Authentication If Specified
	if len(username) > 0 && len(password) > 0 {
		util.AddSaslAuthentication(configMap, constants.ConfigPropertySaslMechanismsPlain, username, password)
	}

	// Enable Kafka Consumer Debug Logging (Debug Only - Do Not Commit Enabled!)
	// util.AddDebugFlags(configMap, "consumer,cgrp,topic,fetch,protocol,security")

	// Create The Kafka Consumer Via Wrapper & Return Results
	return NewConsumerWrapper(configMap)
}

// Kafka Function Reference Variable To Facilitate Mocking In Unit Tests
var NewConsumerWrapper = func(configMap *kafka.ConfigMap) (ConsumerInterface, error) {
	return kafka.NewConsumer(configMap)
}

// Utility Function For Returning The Base/Common Kafka ConfigMap (Values Shared By All Connections)
func getBaseConsumerConfigMap(brokers string, groupId string, offset string) *kafka.ConfigMap {
	return &kafka.ConfigMap{
		constants.ConfigPropertyBootstrapServers:        brokers,
		constants.ConfigPropertyBrokerAddressFamily:     constants.ConfigPropertyBrokerAddressFamilyValue,
		constants.ConfigPropertyGroupId:                 groupId,
		constants.ConfigPropertyEnableAutoOffsetStore:   constants.ConfigPropertyEnableAutoOffsetStoreValue,
		constants.ConfigPropertyEnableAutoOffsetCommit:  constants.ConfigPropertyEnableAutoOffsetCommitValue,
		constants.ConfigPropertyAutoOffsetReset:         offset,
		constants.ConfigPropertyQueuedMaxMessagesKbytes: constants.ConfigPropertyQueuedMaxMessagesKbytesValue,
		constants.ConfigPropertyStatisticsInterval:      constants.ConfigPropertyStatisticsIntervalValue,
	}
}
