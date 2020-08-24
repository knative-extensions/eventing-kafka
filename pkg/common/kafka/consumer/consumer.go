package consumer

import (
	"github.com/Shopify/sarama"
	"github.com/rcrowley/go-metrics"
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
