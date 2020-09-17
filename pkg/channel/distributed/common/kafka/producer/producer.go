package producer

import (
	"github.com/Shopify/sarama"
	"github.com/rcrowley/go-metrics"
)

// Create A Sarama Kafka SyncProducer (Optional Authentication)
func CreateSyncProducer(brokers []string, config *sarama.Config) (sarama.SyncProducer, metrics.Registry, error) {

	// Create A New Sarama SyncProducer & Return Results
	syncProducer, err := newSyncProducerWrapper(brokers, config)
	return syncProducer, config.MetricRegistry, err
}

// Function Reference Variable To Facilitate Mocking In Unit Tests
var newSyncProducerWrapper = func(brokers []string, config *sarama.Config) (sarama.SyncProducer, error) {
	return sarama.NewSyncProducer(brokers, config)
}
