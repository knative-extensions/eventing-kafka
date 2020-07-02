package consumer

import (
	"github.com/Shopify/sarama"
	"knative.dev/eventing-kafka/pkg/common/kafka/util"
)

// Create A Kafka ConsumerGroup (Optional SASL Authentication)
func CreateConsumerGroup(brokers []string, groupId string, username string, password string) (sarama.ConsumerGroup, error) {

	// TODO - From Paul - Not Sure We Want This In Open Source?  Or Maybe in ConfigMap also?
	// Limit Max Request size To 1M Down From Default Of 100M
	//sarama.MaxRequestSize = 1024 * 1024

	// Create The Sarama Consumer Config
	config := getConfig(username, password)

	// Create A New Sarama ConsumerGroup & Return Results
	return NewConsumerGroupWrapper(brokers, groupId, config)
}

// Function Reference Variable To Facilitate Mocking In Unit Tests
var NewConsumerGroupWrapper = func(brokers []string, groupId string, configMap *sarama.Config) (sarama.ConsumerGroup, error) {
	return sarama.NewConsumerGroup(brokers, groupId, configMap)
}

// TODO - The entire sarama.Config should be externalized to a K8S ConfigMap for complete customization.
// Get The Default Sarama Consumer Config
func getConfig(username string, password string) *sarama.Config {

	// Create The Basic Sarama Config
	config := sarama.NewConfig()

	// TODO - Strimzi is up on kafka v2.4.1 and 2.5.0
	//      - kafka eventhubs are compatible with version 1.0 and later?
	//      - will need to be exposed for end users though in ConfigMap ; )
	config.Version = sarama.V2_3_0_0

	// Add Optional SASL Configuration
	util.AddSaslAuthenticationNEW(config, username, password)

	// TODO - Configure The ConsumerGroup !!!
	// config.Consumer.Group.Rebalance.Strategy ???

	// We Want To Know About Consumer Errors
	config.Consumer.Return.Errors = true

	// Return The Sarama Config
	return config
}

// TODO - ORIG ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

//// Confluent Client Doesn't Code To Interfaces Or Provide Mocks So We're Wrapping Our Usage Of The Consumer For Testing
//type ConsumerInterface interface {
//	Subscribe(topic string, rebalanceCb kafka.RebalanceCb) error
//	Poll(int) kafka.Event
//	CommitMessage(*kafka.Message) ([]kafka.TopicPartition, error)
//	Close() error
//	OffsetsForTimes(times []kafka.TopicPartition, timeoutMs int) (offsets []kafka.TopicPartition, err error)
//	GetMetadata(topic *string, allTopics bool, timeoutMs int) (*kafka.Metadata, error)
//	CommitOffsets(offsets []kafka.TopicPartition) ([]kafka.TopicPartition, error)
//	QueryWatermarkOffsets(topic string, partition int32, timeoutMs int) (low, high int64, err error)
//	StoreOffsets(offsets []kafka.TopicPartition) (storedOffsets []kafka.TopicPartition, err error)
//	Commit() ([]kafka.TopicPartition, error)
//}
//
//// Create A Kafka Consumer (Optional SASL Authentication)
//func CreateConsumer(brokers string, groupId string, offset string, username string, password string) (ConsumerInterface, error) {
//
//	// Validate Parameters
//	if len(brokers) <= 0 ||
//		len(groupId) <= 0 ||
//		len(offset) <= 0 {
//		return nil, errors.New("required parameters not provided")
//	}
//
//	// Create The Kafka Consumer Configuration
//	configMap := getBaseConsumerConfigMap(brokers, groupId, offset)
//
//	// Append SASL Authentication If Specified
//	if len(username) > 0 && len(password) > 0 {
//		util.AddSaslAuthentication(configMap, constants.ConfigPropertySaslMechanismsPlain, username, password)
//	}
//
//	// Enable Kafka Consumer Debug Logging (Debug Only - Do Not Commit Enabled!)
//	// util.AddDebugFlags(configMap, "consumer,cgrp,topic,fetch,protocol,security")
//
//	// Create The Kafka Consumer Via Wrapper & Return Results
//	return NewConsumerWrapper(configMap)
//}
//
//// TODO - remove this
//// Kafka Function Reference Variable To Facilitate Mocking In Unit Tests
//var NewConsumerWrapper = func(configMap *kafka.ConfigMap) (ConsumerInterface, error) {
//	return kafka.NewConsumer(configMap)
//}
//
//// Utility Function For Returning The Base/Common Kafka ConfigMap (Values Shared By All Connections)
//func getBaseConsumerConfigMap(brokers string, groupId string, offset string) *kafka.ConfigMap {
//	// tODO - will have to tune the sarma consumergroup as well - look into these and see how to carry over...
//	return &kafka.ConfigMap{
//		constants.ConfigPropertyBootstrapServers:        brokers,
//		constants.ConfigPropertyBrokerAddressFamily:     constants.ConfigPropertyBrokerAddressFamilyValue,
//		constants.ConfigPropertyGroupId:                 groupId,
//		constants.ConfigPropertyEnableAutoOffsetStore:   constants.ConfigPropertyEnableAutoOffsetStoreValue,
//		constants.ConfigPropertyEnableAutoOffsetCommit:  constants.ConfigPropertyEnableAutoOffsetCommitValue,
//		constants.ConfigPropertyAutoOffsetReset:         offset,
//		constants.ConfigPropertyQueuedMaxMessagesKbytes: constants.ConfigPropertyQueuedMaxMessagesKbytesValue,
//		constants.ConfigPropertyStatisticsInterval:      constants.ConfigPropertyStatisticsIntervalValue,
//	}
//}
