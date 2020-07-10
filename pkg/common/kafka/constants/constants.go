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
	ConfigAdminTimeout                  = 10 * time.Second
	ConfigNetSaslVersion                = sarama.SASLHandshakeV0 // TODO - Docs say to use v1 for kafka v1 or later but might need v0 for EventHubs ???  (Try v1 with eventhubs and use that if it works!)
	ConfigConsumerOffsetsCommitInterval = 5 * time.Second
	ConfigProducerIdempotent            = false // Desirable but not available in Azure EventHubs yet, so disabled for now.

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
	// Kafka Admin/Consumer/Producer Config Values
	ConfigKafkaVersion = sarama.V2_3_0_0
)
