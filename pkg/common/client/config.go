package client

import (
	"crypto/tls"
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/ghodss/yaml"
	"knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/constants"
)

// Merge existing Sarama config with provided YAML string.
// Values in YAML string will override the values in the config.
// If config Is nil, A New sarama.Config Struct Will Be Created With Default Values
func MergeSaramaSettings(config *sarama.Config, saramaSettingsYamlString string) (*sarama.Config, error) {
	// Merging To A Nil Config Requires Creating An Default One First
	if config == nil {
		// Start With Base Sarama Defaults
		config = sarama.NewConfig()

		// Use Our Default Minimum Version
		config.Version = constants.ConfigKafkaVersionDefault

		// Add Any Required Settings
		UpdateSaramaConfig(config, config.ClientID, "", "")
	}

	// Extract (Remove) The KafkaVersion From The Sarama Config YAML
	saramaSettingsYamlString, kafkaVersion, err := extractKafkaVersion(saramaSettingsYamlString)
	if err != nil {
		return nil, fmt.Errorf("failed to extract KafkaVersion from Sarama Config YAML: err=%s : config=%+v", err, saramaSettingsYamlString)
	}

	// Extract (Remove) Any TLS.Config RootCAs & Set In Sarama.Config
	saramaSettingsYamlString, certPool, err := extractRootCerts(saramaSettingsYamlString)
	if err != nil {
		return nil, fmt.Errorf("failed to extract RootPEMs from Sarama Config YAML: err=%s : config=%+v", err, saramaSettingsYamlString)
	}

	// Unmarshall The Sarama Config Yaml Into The Provided Sarama.Config Object
	err = yaml.Unmarshal([]byte(saramaSettingsYamlString), &config)
	if err != nil {
		return nil, fmt.Errorf("ConfigMap's sarama value could not be converted to a Sarama.Config struct: %s : %v", err, saramaSettingsYamlString)
	}

	// Override The Custom Parsed KafkaVersion
	config.Version = kafkaVersion

	// Override Any Custom Parsed TLS.Config.RootCAs
	if certPool != nil && len(certPool.Subjects()) > 0 {
		config.Net.TLS.Config = &tls.Config{RootCAs: certPool}
	}

	// Return Success
	return config, nil
}

// Utility Function For Configuring Common Settings For Admin/Producer/Consumer
func UpdateSaramaConfig(config *sarama.Config, clientId string, username string, password string) {

	// Set The ClientID For Logging
	config.ClientID = clientId

	// Set The SASL Username / Password
	config.Net.SASL.User = username
	config.Net.SASL.Password = password

	// We Always Want To Know About Consumer Errors
	config.Consumer.Return.Errors = true

	// We Always Want Success Messages From Producer
	config.Producer.Return.Successes = true
}
