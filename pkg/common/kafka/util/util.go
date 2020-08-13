package util

import (
	"crypto/tls"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/Shopify/sarama"
	"knative.dev/eventing-kafka/pkg/common/kafka/constants"
)

// Utility Function For Enabling Sarama Logging (Debugging)
func EnableSaramaLogging() {
	sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
}

// Creates A sarama.Config With Some Default Settings
func NewSaramaConfig() *sarama.Config {
	config := sarama.NewConfig()
	enforceSaramaConfig(config)
	return config
}

// Forces Some Sarama Settings To Have Mandatory Values
func enforceSaramaConfig(config *sarama.Config) {

	// Latest version, seems to work with EventHubs as well.
	config.Version = constants.ConfigKafkaVersion

	// We Want To Know About Consumer Errors
	config.Consumer.Return.Errors = true

	// We Want "Message Produced" Success Messages For Use With SyncProducer
	config.Producer.Return.Successes = true
}

// Utility Function For Configuring Common Settings For Admin/Producer/Consumer
func UpdateSaramaConfig(config *sarama.Config, clientId string, username string, password string) {

	// Set The ClientID For Logging
	config.ClientID = clientId

	// Update Config With With Additional SASL Auth If Specified
	if len(username) > 0 && len(password) > 0 {
		config.Net.SASL.Version = constants.ConfigNetSaslVersion
		config.Net.SASL.Enable = true
		config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		config.Net.SASL.User = username
		config.Net.SASL.Password = password
		config.Net.TLS.Enable = true
		config.Net.TLS.Config = &tls.Config{
			ClientAuth: tls.NoClientCert,
		}
	}

	// Do not permit changing of some particular settings
	enforceSaramaConfig(config)
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
