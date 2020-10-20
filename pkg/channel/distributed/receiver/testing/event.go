/*
Copyright 2020 The Knative Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package testing

import (
	"encoding/json"

	"github.com/cloudevents/sdk-go/v2/event"
	"knative.dev/eventing-kafka/pkg/channel/distributed/receiver/constants"
)

// Test Data
var EventDataJson, _ = json.Marshal(map[string]string{EventDataKey: EventDataValue})

// Utility Function For Creating A Test CloudEvent
func CreateCloudEvent(cloudEventVersion string) *event.Event {
	cloudEvent := event.New(cloudEventVersion)
	cloudEvent.SetID(EventId)
	cloudEvent.SetType(EventType)
	cloudEvent.SetSource(EventSource)
	cloudEvent.SetDataContentType(EventDataContentType)
	cloudEvent.SetSubject(EventSubject)
	cloudEvent.SetDataSchema(EventDataSchema)
	cloudEvent.SetExtension(constants.ExtensionKeyPartitionKey, PartitionKey)
	_ = cloudEvent.SetData(EventDataContentType, EventDataJson)
	return &cloudEvent
}
