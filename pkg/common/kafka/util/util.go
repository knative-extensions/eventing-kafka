package util

import (
	"crypto/tls"
	"fmt"
	"github.com/Shopify/sarama"
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

// TODO - remove above and rename below once adminclient has been converted to sarama ; )
func AddSaslAuthenticationNEW(config *sarama.Config, username string, password string) {

	// Update Config With With PLAIN SASL Auth If Specified
	if config != nil && len(username) > 0 && len(password) > 0 {

		// TODO - Default is v0 (required for azure eventhubs ?!)  TRY IT OUT WITH V1 and EVENTHUBS !!!
		//      - Docs say use v1 for kafka v1 or later but should work?
		//      - But we need to expose the whole config as a ConfigMap eventually anyway...
		//config.Net.SASL.Version = sarama.SASLHandshakeV1

		config.Net.SASL.Enable = true
		config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		config.Net.SASL.User = username
		config.Net.SASL.Password = password

		config.Net.TLS.Enable = true
		config.Net.TLS.Config = &tls.Config{
			InsecureSkipVerify: true,
			ClientAuth:         tls.NoClientCert,
		}
	}
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

// Append The KafkaChannel Service Name Suffix To The Specified String
func AppendKafkaChannelServiceNameSuffix(channelName string) string {
	return fmt.Sprintf("%s-%s", channelName, constants.KafkaChannelServiceNameSuffix)
}

// Remove The KafkaChannel Service Name Suffix From The Specified String
func TrimKafkaChannelServiceNameSuffix(serviceName string) string {
	return strings.TrimSuffix(serviceName, "-"+constants.KafkaChannelServiceNameSuffix)
}
