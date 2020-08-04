package constants

import (
	"github.com/Shopify/sarama"
	"time"
)

// Constants
const (

	// Knative Eventing Namespace
	KnativeEventingNamespace = "knative-eventing"

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
	ConfigAdminTimeout                      = 10 * time.Second       // Bumped up from 3 seconds just for some extra margin.
	ConfigNetSaslVersion                    = sarama.SASLHandshakeV1 // Latest version, seems to work with EventHubs as well.
	ConfigNetKeepAlive                      = 30 * time.Second       // Pretty sure Sarama documentation is incorrect and 0 means default of 15 seconds but we'll forcibly set that anyway. (see Golang Net.Dialer.KeepAlive)
	ConfigMetadataRefreshFrequency          = 5 * time.Minute        // How often to refresh metadata, reduced from default value of 10 minutes.
	ConfigConsumerOffsetsAutoCommitInterval = 5 * time.Second        // Auto offset commit interval for message Marked as consumed.
	ConfigConsumerOffsetsRetention          = 7 * 24 * time.Hour     // Increase default offset retention from 1 day to 1 week.
	ConfigProducerIdempotent                = false                  // Desirable but not available in Azure EventHubs yet, so disabled for now.
	ConfigProducerRequiredAcks              = sarama.WaitForAll      // Most stringent option for "at-least-once" delivery.

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
	KafkaChannelServiceNameSuffix = "kafkachannel"
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
