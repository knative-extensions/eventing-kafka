package producer

import (
	"github.com/Shopify/sarama"
	"knative.dev/eventing-kafka/pkg/common/kafka/util"
)

// Create A Sarama Kafka SyncProducer (Optional Authentication)
func CreateSyncProducer(brokers []string, username string, password string) (sarama.SyncProducer, error) {

	// TODO - From Paul - Not Sure We Want This In Open Source?  Or Maybe in ConfigMap also?
	// Limit Max Request size To 1M Down From Default Of 100M
	//sarama.MaxRequestSize = 1024 * 1024

	// Create The Sarama SyncProducer Config
	config := getConfig(username, password)

	// Create A New Sarama SyncProducer & Return Results
	return newSyncProducerWrapper(brokers, config)
}

// Function Reference Variable To Facilitate Mocking In Unit Tests
var newSyncProducerWrapper = func(brokers []string, config *sarama.Config) (sarama.SyncProducer, error) {
	return sarama.NewSyncProducer(brokers, config)
}

// TODO - The entire sarama.Config should be externalized to a K8S ConfigMap for complete customization.
// Get The Default Sarama SyncProducer Config
func getConfig(username string, password string) *sarama.Config {

	// Create The Basic Sarama Config
	config := sarama.NewConfig()

	// TODO - Strimzi is up on kafka v2.4.1 and 2.5.0
	//      - kafka eventhubs are compatible with version 1.0 and later?
	//      - will need to be exposed for end users though in ConfigMap ; )
	config.Version = sarama.V2_3_0_0

	// TODO - From Paul's performance testing - do we want to use this? - thinking we do in one form or another...
	//config.MetricRegistry = metrics.DefaultRegistry

	// Add Optional SASL Configuration
	util.AddSaslAuthenticationNEW(config, username, password)

	// We Want "Message Produced" Success Messages For Use With SyncProducer
	config.Producer.Return.Successes = true

	// Return The Sarama Config
	return config
}
