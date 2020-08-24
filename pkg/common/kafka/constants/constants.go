package constants

import (
	"github.com/Shopify/sarama"
)

// Constants
const (
	// Duration Convenience
	MillisPerDay = 24 * 60 * 60 * 1000 // 86400000

	// Kafka Secret Label
	KafkaSecretLabel = "eventing-kafka.knative.dev/kafka-secret"

	// Kafka Secret Keys
	KafkaSecretKeyBrokers   = "brokers"
	KafkaSecretKeyNamespace = "namespace"
	KafkaSecretKeyUsername  = "username"
	KafkaSecretKeyPassword  = "password"

	// Kafka Admin/Consumer/Producer Config Values
	ConfigNetSaslVersion = sarama.SASLHandshakeV1 // Latest version, seems to work with EventHubs as well.

	// Kafka Topic Config Keys
	TopicDetailConfigRetentionMs = "retention.ms"

	// EventHub Error Codes
	EventHubErrorCodeUnknown       = -2
	EventHubErrorCodeParseFailure  = -1
	EventHubErrorCodeCapacityLimit = 403
	EventHubErrorCodeConflict      = 409

	// EventHub Constraints
	MaxEventHubNamespaces = 100

	// KafkaChannel Constants
	KafkaChannelServiceNameSuffix = "kn-channel" // Specific Value For Use With Knative e2e Tests!

	// Plugin AdminClient Name (Plugin Writers Must Ensure The Binary Is Names As Such)
	PluginAdminClientName = "kafka-admin-client.so"
)

// Non-Constant Constants ;)
var (

	//
	// Kafka Version
	//
	// As with all the Sarama / Kafka config above this should be exposed for customization.
	// Until then the value is hard-coded to the lowest common denominator version to provide
	// the most compatible solution.  Specifically, Sarama's ConsumerGroups repeatedly close
	// due to EOF failures when working against Azure EventHubs.
	//
	ConfigKafkaVersion = sarama.V1_0_0_0
)
