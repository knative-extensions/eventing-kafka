package consumer

import (
	"github.com/Shopify/sarama"
	"github.com/rcrowley/go-metrics"
	"knative.dev/eventing-kafka/pkg/common/kafka/constants"
	"knative.dev/eventing-kafka/pkg/common/kafka/util"
)

// Create A Kafka ConsumerGroup (Optional SASL Authentication)
func CreateConsumerGroup(brokers []string, config *sarama.Config, groupId string) (sarama.ConsumerGroup, metrics.Registry, error) {

	// Create A New Sarama ConsumerGroup & Return Results
	consumerGroup, err := NewConsumerGroupWrapper(brokers, groupId, config)
	return consumerGroup, config.MetricRegistry, err
}

// Function Reference Variable To Facilitate Mocking In Unit Tests
var NewConsumerGroupWrapper = func(brokers []string, groupId string, config *sarama.Config) (sarama.ConsumerGroup, error) {
	return sarama.NewConsumerGroup(brokers, groupId, config)
}

// Add our fields to the provided sarama.Config struct
// TODO: EDV: The constants that aren't username/password/clientId should be moved to our configmap
func UpdateConfig(config *sarama.Config, clientId string, username string, password string) *sarama.Config {

	util.UpdateSaramaConfig(config, clientId, username, password)

	// Increase Default Offset Commit Interval So As To Not Overwhelm EventHub RateLimit Specs
	config.Consumer.Offsets.AutoCommit.Interval = constants.ConfigConsumerOffsetsAutoCommitInterval

	// Increase Default Offset Retention In (Kafka Default Is 24Hrs)
	config.Consumer.Offsets.Retention = constants.ConfigConsumerOffsetsRetention

	// We Want To Know About Consumer Errors
	config.Consumer.Return.Errors = true

	// Return The Modified Sarama Config
	return config
}
