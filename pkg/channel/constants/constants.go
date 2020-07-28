package constants

import "time"

// Global Constants
const (
	Component = "KafkaChannel"

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
	CeKafkaHeaderKeyTime         = "ce_time"
)
