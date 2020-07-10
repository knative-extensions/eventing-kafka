package producer

import (
	"github.com/Shopify/sarama"
	"knative.dev/eventing-kafka/pkg/common/kafka/constants"
	"knative.dev/eventing-kafka/pkg/common/kafka/util"
)

// Create A Sarama Kafka SyncProducer (Optional Authentication)
func CreateSyncProducer(clientId string, brokers []string, username string, password string) (sarama.SyncProducer, error) {

	// Create The Sarama SyncProducer Config
	config := getConfig(clientId, username, password)

	// Create A New Sarama SyncProducer & Return Results
	return newSyncProducerWrapper(brokers, config)
}

// Function Reference Variable To Facilitate Mocking In Unit Tests
var newSyncProducerWrapper = func(brokers []string, config *sarama.Config) (sarama.SyncProducer, error) {
	return sarama.NewSyncProducer(brokers, config)
}

// Get The Default Sarama SyncProducer Config
func getConfig(clientId string, username string, password string) *sarama.Config {

	// Create The Basic Sarama Config
	config := sarama.NewConfig()

	// Set The Consumer's ClientID
	config.ClientID = clientId

	// Specify Kafka Version Compatibility
	config.Version = constants.ConfigKafkaVersion

	// Specify Producer Idempotence
	config.Producer.Idempotent = constants.ConfigProducerIdempotent

	// TODO - From Paul's performance testing - do we want to use this? - thinking we do in one form or another...
	//config.MetricRegistry = metrics.DefaultRegistry

	// Add Optional SASL Configuration
	util.AddSaslAuthentication(config, username, password)

	// We Want "Message Produced" Success Messages For Use With SyncProducer
	config.Producer.Return.Successes = true

	// Return The Sarama Config
	return config
}
