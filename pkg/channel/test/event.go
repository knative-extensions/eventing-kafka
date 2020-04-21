package test

import (
	"encoding/json"
	cloudevents "github.com/cloudevents/sdk-go/v1"
	"knative.dev/eventing-kafka/pkg/channel/constants"
)

// Test Data
var EventDataJson, _ = json.Marshal(map[string]string{EventDataKey: EventDataValue})

// Utility Function For Creating A Test CloudEvent
func CreateCloudEvent(cloudEventVersion string) cloudevents.Event {
	event := cloudevents.NewEvent(cloudEventVersion)
	event.SetID(EventId)
	event.SetType(EventType)
	event.SetSource(EventSource)
	event.SetDataContentType(EventDataContentType)
	event.SetSubject(EventSubject)
	event.SetDataSchema(EventDataSchema)
	event.SetExtension(constants.ExtensionKeyPartitionKey, PartitionKey)
	event.SetData(EventDataJson)
	return event
}
