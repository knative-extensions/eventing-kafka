package consumer

import (
	"github.com/Shopify/sarama"
	"knative.dev/eventing-kafka/pkg/common/kafka/constants"
	"knative.dev/eventing-kafka/pkg/common/kafka/util"
)

// Create A Kafka ConsumerGroup (Optional SASL Authentication)
func CreateConsumerGroup(clientId string, brokers []string, groupId string, username string, password string) (sarama.ConsumerGroup, error) {

	// Create The Sarama Consumer Config
	config := getConfig(clientId, username, password)

	// Create A New Sarama ConsumerGroup & Return Results
	return NewConsumerGroupWrapper(brokers, groupId, config)
}

// Function Reference Variable To Facilitate Mocking In Unit Tests
var NewConsumerGroupWrapper = func(brokers []string, groupId string, config *sarama.Config) (sarama.ConsumerGroup, error) {
	return sarama.NewConsumerGroup(brokers, groupId, config)
}

// Get The Default Sarama Consumer Config
func getConfig(clientId string, username string, password string) *sarama.Config {

	// Create The Basic Sarama Config
	config := sarama.NewConfig()

	// Set The Consumer's ClientID
	config.ClientID = clientId

	// Specify Kafka Version Compatibility
	config.Version = constants.ConfigKafkaVersion

	// Add Optional SASL Configuration
	util.AddSaslAuthentication(config, username, password)

	// Increase Default Offset Commit Interval So As To Not Overwhelm EventHub RateLimit Specs
	config.Consumer.Offsets.CommitInterval = constants.ConfigConsumerOffsetsCommitInterval

	// We Want To Know About Consumer Errors
	config.Consumer.Return.Errors = true

	// Return The Sarama Config
	return config
}
