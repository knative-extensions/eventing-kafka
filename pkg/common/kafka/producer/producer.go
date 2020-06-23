package producer

import (
	"crypto/tls"
	"github.com/Shopify/sarama"
	"github.com/rcrowley/go-metrics"
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
	config.MetricRegistry = metrics.DefaultRegistry

	// We Want Message Produced Success Messages For Use With SyncProducer
	config.Producer.Return.Successes = true

	// TODO - this will replace AddSaslAuthentication() in util once dispatcher/consumer is converted over
	// Update Config With With PLAIN SASL Auth If Specified
	if len(username) > 0 && len(password) > 0 {

		// TODO - Default is v0 (required for azure eventhubs ?!)  TRY IT OUT WITH V1 and EVENTHUBS !!!
		//      - Docs say use v1 for kafka v1 or later but should work?
		//      - But we need to expose the whole config as a ConfigMap eventually anyway...
		//config.Net.SASL.Version = sarama.SASLHandshakeV1

		config.Net.SASL.Enable = true
		config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		config.Net.SASL.User = username
		config.Net.SASL.Password = password

		config.Net.TLS.Enable = true
		config.Net.TLS.Config = &tls.Config{
			InsecureSkipVerify: true,
			ClientAuth:         tls.NoClientCert,
		}
	}

	// Return The Sarama Config
	return config
}
