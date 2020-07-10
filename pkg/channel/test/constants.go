package test

import cloudevents "github.com/cloudevents/sdk-go/v2"

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
	EventVersion         = cloudevents.VersionV1
	EventId              = "TestEventId"
	EventType            = "com.cloudevents.readme.sent"
	EventSource          = "http://localhost:8080/"
	EventDataContentType = "application/json"
	EventSubject         = "TestEventSubject"
	EventDataSchema      = "TestEventDataSchema"
	EventDataKey         = "TestEventDataKey"
	EventDataValue       = "TestEventDataValue"
)
