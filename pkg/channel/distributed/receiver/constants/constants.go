package constants

import "time"

// Global Constants
const (
	Component = "eventing-kafka-channel-receiver"

	MetricsInterval = 5 * time.Second

	ExtensionKeyPartitionKey = "partitionkey"

	KafkaHeaderKeyContentType = "content-type"

	CeKafkaHeaderKeySpecVersion  = "ce_specversion"
	CeKafkaHeaderKeyType         = "ce_type"
	CeKafkaHeaderKeySource       = "ce_source"
	CeKafkaHeaderKeyId           = "ce_id"
	CeKafkaHeaderKeySubject      = "ce_subject"
	CeKafkaHeaderKeyDataSchema   = "ce_dataschema"
	CeKafkaHeaderKeyPartitionKey = "ce_partitionkey"
)
