package constants

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

	// Common Kafka Configuration Properties
	ConfigPropertyDebug                      = "debug"
	ConfigPropertyBootstrapServers           = "bootstrap.servers"
	ConfigPropertyRequestTimeoutMs           = "request.timeout.ms"
	ConfigPropertyRequestTimeoutMsValue      = 30000
	ConfigPropertyStatisticsInterval         = "statistics.interval.ms"
	ConfigPropertyStatisticsIntervalValue    = 5000
	ConfigPropertySocketKeepAliveEnable      = "socket.keepalive.enable"
	ConfigPropertySocketKeepAliveEnableValue = true
	ConfigPropertyMetadataMaxAgeMs           = "metadata.max.age.ms"
	ConfigPropertyMetadataMaxAgeMsValue      = 180000

	// Kafka Security/Auth Configuration Properties
	ConfigPropertySecurityProtocol      = "security.protocol"
	ConfigPropertySecurityProtocolValue = "SASL_SSL"
	ConfigPropertySaslMechanisms        = "sasl.mechanisms"
	// TODO - can probably remove these below (and above too ; )
	ConfigPropertySaslMechanismsPlain = "PLAIN"
	ConfigPropertySaslUsername        = "sasl.username"
	ConfigPropertySaslPassword        = "sasl.password"

	// Kafka Producer Configuration Properties
	ConfigPropertyPartitioner      = "partitioner"
	ConfigPropertyPartitionerValue = "murmur2_random"
	ConfigPropertyIdempotence      = "enable.idempotence"
	ConfigPropertyIdempotenceValue = false // Desirable but not available in Azure EventHubs yet, so disabled for now.

	// Kafka Consumer Configuration Properties
	ConfigPropertyBrokerAddressFamily          = "broker.address.family"
	ConfigPropertyBrokerAddressFamilyValue     = "v4"
	ConfigPropertyGroupId                      = "group.id"
	ConfigPropertyEnableAutoOffsetStore        = "enable.auto.offset.store"
	ConfigPropertyEnableAutoOffsetStoreValue   = false
	ConfigPropertyEnableAutoOffsetCommit       = "enable.auto.commit"
	ConfigPropertyEnableAutoOffsetCommitValue  = false // Event loss is possible with auto-commit enabled so we will manually commit offsets!
	ConfigPropertyAutoOffsetReset              = "auto.offset.reset"
	ConfigPropertyQueuedMaxMessagesKbytes      = "queued.max.messages.kbytes" // Controls the amount of pre-fetched messages the consumer will pull down per partition
	ConfigPropertyQueuedMaxMessagesKbytesValue = "7000"

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
