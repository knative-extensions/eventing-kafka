package util

import (
	"crypto/tls"
	"fmt"
	"github.com/Shopify/sarama"
	"knative.dev/eventing-kafka/pkg/common/kafka/constants"
	"strings"
)

// Utility Function For Adding SASL Credentials To Kafka Config
func AddSaslAuthentication(config *sarama.Config, username string, password string) {

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
