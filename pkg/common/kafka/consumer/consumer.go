package consumer

import (
	"github.com/Shopify/sarama"
	"github.com/rcrowley/go-metrics"
	"knative.dev/eventing-kafka/pkg/common/kafka/constants"
	"knative.dev/eventing-kafka/pkg/common/kafka/util"
)

// Create A Kafka ConsumerGroup (Optional SASL Authentication)
func CreateConsumerGroup(clientId string, brokers []string, groupId string, username string, password string) (sarama.ConsumerGroup, metrics.Registry, error) {

	// Create The Sarama Consumer Config
	config := getConfig(clientId, username, password)

	// Create A New Sarama ConsumerGroup & Return Results
	consumerGroup, err := NewConsumerGroupWrapper(brokers, groupId, config)
	return consumerGroup, config.MetricRegistry, err
}

// Function Reference Variable To Facilitate Mocking In Unit Tests
var NewConsumerGroupWrapper = func(brokers []string, groupId string, config *sarama.Config) (sarama.ConsumerGroup, error) {
	return sarama.NewConsumerGroup(brokers, groupId, config)
}

// Get The Default Sarama Consumer Config
func getConfig(clientId string, username string, password string) *sarama.Config {

	// Create A New Base Sarama Config
	config := util.NewSaramaConfig(clientId, username, password)

	// Increase Default Offset Commit Interval So As To Not Overwhelm EventHub RateLimit Specs
	config.Consumer.Offsets.AutoCommit.Interval = constants.ConfigConsumerOffsetsAutoCommitInterval

	// Increase Default Offset Retention In (Kafka Default Is 24Hrs)
	config.Consumer.Offsets.Retention = constants.ConfigConsumerOffsetsRetention

	// We Want To Know About Consumer Errors
	config.Consumer.Return.Errors = true

	// Return The Sarama Config
	return config
}
