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

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

// Test The CoreV1 EventType "Enum" String Values
func TestEventTypes(t *testing.T) {

	tests := []struct {
		name      string
		eventType CoreV1EventType
		expect    string
	}{
		{name: "ResetOffsetReconciled", eventType: ResetOffsetReconciled, expect: "ResetOffsetReconciled"},
		{name: "ResetOffsetFinalized", eventType: ResetOffsetFinalized, expect: "ResetOffsetFinalized"},
		{name: "ResetOffsetSkipped", eventType: ResetOffsetSkipped, expect: "ResetOffsetSkipped"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual := test.eventType.String()
			assert.Equal(t, test.expect, actual)
		})
	}
}
