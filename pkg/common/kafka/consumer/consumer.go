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
var NewConsumerGroupWrapper = func(brokers []string, groupId string, config *sarama.Config) (sarama.ConsumerGroup, error) {
	return sarama.NewConsumerGroup(brokers, groupId, config)
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
	util.AddSaslAuthentication(config, username, password)

	// TODO - Configure The ConsumerGroup !!!
	// config.Consumer.Group.Rebalance.Strategy ???

	// We Want To Know About Consumer Errors
	config.Consumer.Return.Errors = true

	// Return The Sarama Config
	return config
}
