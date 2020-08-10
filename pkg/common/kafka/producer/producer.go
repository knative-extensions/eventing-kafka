package producer

import (
	"github.com/Shopify/sarama"
	"github.com/rcrowley/go-metrics"
	"knative.dev/eventing-kafka/pkg/common/kafka/constants"
	"knative.dev/eventing-kafka/pkg/common/kafka/util"
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

// Add our fields to the provided sarama.Config struct
// TODO: EDV: The constants that aren't username/password/clientId should be moved to our configmap
func UpdateConfig(config *sarama.Config, clientId string, username string, password string) {

	util.UpdateSaramaConfig(config, clientId, username, password)

	// Specify Producer Idempotence
	config.Producer.Idempotent = constants.ConfigProducerIdempotent

	// Specify Producer RequiredAcks For At-Least-Once Delivery
	config.Producer.RequiredAcks = constants.ConfigProducerRequiredAcks

	// We Want "Message Produced" Success Messages For Use With SyncProducer
	config.Producer.Return.Successes = true
}
