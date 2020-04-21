package util

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"knative.dev/eventing-kafka/pkg/common/kafka/constants"
	"strings"
)

// Utility Function For Adding Authentication Credentials To Kafka ConfigMap
func AddSaslAuthentication(configMap *kafka.ConfigMap, mechanism string, username string, password string) {

	// Update The Kafka ConfigMap With SASL Username & Password (Ignoring Impossible Errors)
	_ = configMap.SetKey(constants.ConfigPropertySecurityProtocol, constants.ConfigPropertySecurityProtocolValue)
	_ = configMap.SetKey(constants.ConfigPropertySaslMechanisms, mechanism)
	_ = configMap.SetKey(constants.ConfigPropertySaslUsername, username)
	_ = configMap.SetKey(constants.ConfigPropertySaslPassword, password)
}

//
// Utility Function For Adding Debug Flags To Kafka ConfigMap
//
// Note - Flags is a CSV string of all the appropriate kafka/librdkafka debug settings.
//        Valid values as of this writing include...
//            generic, broker, topic, metadata, feature, queue, msg, protocol, cgrp,
//            security, fetch, interceptor, plugin, consumer, admin, eos, all
//
// Example - Common Producer values might be "broker,topic,msg,protocol,security"
//
func AddDebugFlags(configMap *kafka.ConfigMap, flags string) {

	// Update The Kafka ConfigMap With The Specified Debug Flags (Ignoring Impossible Errors)
	_ = configMap.SetKey(constants.ConfigPropertyDebug, flags)

}

// Get The Formatted Kafka Topic Name From The Specified Components
func TopicName(namespace string, name string) string {
	return fmt.Sprintf("%s.%s", namespace, name)
}

// Append The KafkaChannel Service Name Suffix To The Specified String (To Workaround Kyma Naming Conflict)
func AppendKafkaChannelServiceNameSuffix(channelName string) string {
	return fmt.Sprintf("%s-%s", channelName, constants.KafkaChannelServiceNameSuffix)
}

// Remove The KafkaChannel Service Name Suffix From The Specified String (To Workaround Kyma Naming Conflict)
func TrimKafkaChannelServiceNameSuffix(serviceName string) string {
	return strings.TrimSuffix(serviceName, "-"+constants.KafkaChannelServiceNameSuffix)
}
