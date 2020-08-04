package producer

import (
	"github.com/Shopify/sarama"
	"github.com/rcrowley/go-metrics"
	"knative.dev/eventing-kafka/pkg/common/kafka/constants"
	"knative.dev/eventing-kafka/pkg/common/kafka/util"
)

// Create A Sarama Kafka SyncProducer (Optional Authentication)
func CreateSyncProducer(clientId string, brokers []string, username string, password string) (sarama.SyncProducer, metrics.Registry, error) {

	// Create The Sarama SyncProducer Config
	config := getConfig(clientId, username, password)

	// Create A New Sarama SyncProducer & Return Results
	syncProducer, err := newSyncProducerWrapper(brokers, config)
	return syncProducer, config.MetricRegistry, err
}

// Function Reference Variable To Facilitate Mocking In Unit Tests
var newSyncProducerWrapper = func(brokers []string, config *sarama.Config) (sarama.SyncProducer, error) {
	return sarama.NewSyncProducer(brokers, config)
}

// Get The Default Sarama SyncProducer Config
func getConfig(clientId string, username string, password string) *sarama.Config {

	// Create A New Base Sarama Config
	config := util.NewSaramaConfig(clientId, username, password)

	// Specify Producer Idempotence
	config.Producer.Idempotent = constants.ConfigProducerIdempotent

	// Specify Producer RequiredAcks For At-Least-Once Delivery
	config.Producer.RequiredAcks = constants.ConfigProducerRequiredAcks

	// We Want "Message Produced" Success Messages For Use With SyncProducer
	config.Producer.Return.Successes = true

	// Return The Sarama Config
	return config
}
