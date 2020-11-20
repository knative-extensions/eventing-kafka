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

package k8s

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// Test The TruncateLabelValue() Functionality
func TestTruncateLabelValue(t *testing.T) {

	// Define The TestCase Struct
	type TestCase struct {
		Value  string
		Result string
	}

	// Create The TestCases
	testCases := []TestCase{
		{Value: "", Result: ""},
		{Value: "a", Result: "a"},
		{Value: "123456789012345678901234567890123456789012345678901234567890ab", Result: "123456789012345678901234567890123456789012345678901234567890ab"},    // 62 Characters
		{Value: "123456789012345678901234567890123456789012345678901234567890abc", Result: "123456789012345678901234567890123456789012345678901234567890abc"},  // 63 Characters
		{Value: "123456789012345678901234567890123456789012345678901234567890abcd", Result: "123456789012345678901234567890123456789012345678901234567890abc"}, // 64 Characters
	}

	// Run The TestCases
	for _, testCase := range testCases {
		t.Run(testCase.Value, func(t *testing.T) {
			result := TruncateLabelValue(testCase.Value)
			assert.Equal(t, result, testCase.Result)
		})
	}
}
