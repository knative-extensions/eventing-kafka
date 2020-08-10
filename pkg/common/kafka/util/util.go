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

// Utility Function For Configuring Common Settings For Admin/Producer/Consumer
// TODO:  EDV:  This constants in this function should not be necessary any longer when our configmap has this information in it; remove
func UpdateSaramaConfig(config *sarama.Config, clientId string, username string, password string) {

	// Set The ClientID For Logging
	config.ClientID = clientId

	// Specify Kafka Version Compatibility
	config.Version = constants.ConfigKafkaVersion

	// Set Basic Network Settings
	config.Net.KeepAlive = constants.ConfigNetKeepAlive

	// Update Config With With PLAIN SASL Auth If Specified
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

	// Increase The MetaData Refresh Frequency
	config.Metadata.RefreshFrequency = constants.ConfigMetadataRefreshFrequency
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
