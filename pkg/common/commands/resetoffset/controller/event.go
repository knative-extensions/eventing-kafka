/*
Copyright 2021 The Knative Authors

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

package controller

// CoreV1EventType "Enum" Type
type CoreV1EventType int

// CoreV1 EventType "Enum" Values For ResetOffset
const (
	ResetOffsetReconciled CoreV1EventType = iota
	ResetOffsetFinalized
	ResetOffsetSkipped
	ResetOffsetMappedRef
	ResetOffsetParsedTime
	ResetOffsetUpdatedOffsets
)

// CoreV1 EventType String Value
func (et CoreV1EventType) String() string {

	// Default The EventType String Value
	eventTypeString := "Unknown Event Type"

	// Map EventTypes To Their String Values
	switch et {
	case ResetOffsetReconciled:
		eventTypeString = "ResetOffsetReconciled"
	case ResetOffsetFinalized:
		eventTypeString = "ResetOffsetFinalized"
	case ResetOffsetSkipped:
		eventTypeString = "ResetOffsetSkipped"
	case ResetOffsetMappedRef:
		eventTypeString = "ResetOffsetMappedRef"
	case ResetOffsetParsedTime:
		eventTypeString = "ResetOffsetParsedTime"
	case ResetOffsetUpdatedOffsets:
		eventTypeString = "ResetOffsetUpdatedOffsets"
	}

	// Return The EventType String Value
	return eventTypeString
}
