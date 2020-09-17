package test

// Test Data
const (
	ClientId = "TestClientId"

	KafkaBrokers  = "TestBrokers"
	KafkaUsername = "TestUsername"
	KafkaPassword = "TestPassword"

	ChannelName      = "TestChannelName"
	ChannelNamespace = "TestChannelNamespace"

	TopicName = "TestChannelNamespace.TestChannelName" // Match util.go TopicName() Implementation For Channel Name/Namespace Abovce

	PartitionKey         = "TestPartitionKey"
	EventId              = "TestEventId"
	EventType            = "com.cloudevents.readme.sent"
	EventSource          = "http://localhost:8080/"
	EventDataContentType = "application/json"
	EventSubject         = "TestEventSubject"
	EventDataSchema      = "TestEventDataSchema"
	EventDataKey         = "TestEventDataKey"
	EventDataValue       = "TestEventDataValue"
)
