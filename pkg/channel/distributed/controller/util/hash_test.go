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

package util

import (
	"fmt"
	"testing"

	"crypto/md5"

	"github.com/stretchr/testify/assert"
)

// Test the GenerateHash Functionality
func TestGenerateHash(t *testing.T) {
	// Define The TestCase Struct
	type TestCase struct {
		Name   string
		Length int
	}

	// Create The TestCases
	testCases := []TestCase{
		{Name: "", Length: 32},
		{Name: "short string", Length: 4},
		{Name: "long string, 8-character hash", Length: 8},
		{Name: "odd hash length, 13-characters", Length: 13},
		{Name: "very long string with 16-character hash and more than 64 characters total", Length: 16},
	}

	// Run The TestCases
	for _, testCase := range testCases {
		hash := GenerateHash(testCase.Name, testCase.Length)
		expected := fmt.Sprintf("%x", md5.Sum([]byte(testCase.Name)))[0:testCase.Length]
		assert.Equal(t, expected, hash)
	}

}
