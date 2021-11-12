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
	"testing"

	"github.com/stretchr/testify/assert"
)

// Test The GenerateValidDnsName() Functionality
func TestGenerateValidDnsName(t *testing.T) {

	// Define The TestCase Struct
	type TestCase struct {
		Name   string
		Length int
		Result string
		Prefix bool
		Suffix bool
	}

	// Create The TestCases
	testCases := []TestCase{
		{Name: "testname", Length: 10, Result: "testname", Prefix: true, Suffix: true},
		{Name: "TeStNaMe", Length: 10, Result: "testname", Prefix: true, Suffix: true},
		{Name: "testnamelooooong", Length: 10, Result: "testnamelo", Prefix: true, Suffix: true},
		{Name: "testname1234567890", Length: 50, Result: "testname", Prefix: true, Suffix: true},
		{Name: "testname1234567890", Length: 50, Result: "testname1234567890", Prefix: true, Suffix: false},
		{Name: "abcdefghijk1234567890lmnopqrstuvwxyz", Length: 50, Result: "abcdefghijk1234567890lmnopqrstuvwxyz", Prefix: true, Suffix: true},
		{Name: "a~!@#$%^&*()_+=`<>?/\\z", Length: 50, Result: "az", Prefix: true, Suffix: true},
		{Name: "123testname", Length: 50, Result: "kk-123testname", Prefix: true, Suffix: true},
		{Name: "123testname", Length: 50, Result: "123testname", Prefix: false, Suffix: true},
		{Name: "123testNAME", Length: 10, Result: "kk-123test", Prefix: true, Suffix: true},
		{Name: "123testNAME", Length: 10, Result: "123testnam", Prefix: false, Suffix: true},
		{Name: "a123456789012345678901234567890123456789012345678901234567890z", Length: -1, Result: "a123456789012345678901234567890123456789012345678901234567890z", Prefix: true, Suffix: true},
		{Name: "a123456789012345678901234567890123456789012345678901234567890z", Length: 0, Result: "a123456789012345678901234567890123456789012345678901234567890z", Prefix: true, Suffix: true},
		{Name: "abc123456789012345678901234567890123456789012345678901234567890z", Length: 3, Result: "abc", Prefix: true, Suffix: true},
		{Name: "a123456789012345678901234567890123456789012345678901234567890z", Length: 100, Result: "a123456789012345678901234567890123456789012345678901234567890z", Prefix: true, Suffix: true},
		{Name: "a123456789012345678901234567890123456789012345678901234567890zxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", Length: 100, Result: "a123456789012345678901234567890123456789012345678901234567890zx", Prefix: true, Suffix: true},
	}

	// Run The TestCases
	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			actualDnsName := GenerateValidDnsName(testCase.Name, testCase.Length, testCase.Prefix, testCase.Suffix)
			assert.Equal(t, testCase.Result, actualDnsName)
		})
	}
}
