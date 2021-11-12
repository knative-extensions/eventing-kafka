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

// Test Data
const (
	ChannelName      = "TestChannelName"
	ChannelNamespace = "TestChannelNamespace"

	TopicName = "TestChannelNamespace.TestChannelName" // Match util.go TopicName() Implementation For Channel Name/Namespace Above

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
