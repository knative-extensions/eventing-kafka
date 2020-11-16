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

package eventhubcache

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	logtesting "knative.dev/pkg/logging/testing"
)

// Test The NewNamespace() Constructor
func TestNewNamespace(t *testing.T) {

	// Test Data
	serviceBus := "test-namespace.servicebus.windows.net"
	connectionString := fmt.Sprintf("Endpoint=sb://%s/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=TestSharedAccessKey", serviceBus)

	// Create A Mock HubManager
	mockHubManager := &MockHubManager{}

	// Replace The NewHubManagerFromConnectionString Wrapper To Provide Mock Implementation & Defer Reset
	newHubManagerFromConnectionStringWrapperPlaceholder := NewHubManagerFromConnectionStringWrapper
	NewHubManagerFromConnectionStringWrapper = func(connectionString string) (managerInterface HubManagerInterface, e error) {
		return mockHubManager, nil
	}
	defer func() { NewHubManagerFromConnectionStringWrapper = newHubManagerFromConnectionStringWrapperPlaceholder }()

	// Create A Test Logger
	logger := logtesting.TestLogger(t).Desugar()

	// Perform The Test
	namespace, err := NewNamespace(logger, connectionString)

	// Verify Results
	assert.NotNil(t, namespace)
	assert.Nil(t, err)
	assert.Equal(t, serviceBus, namespace.Name)
	assert.Equal(t, 0, namespace.Count)
	assert.Equal(t, mockHubManager, namespace.HubManager)
}

// Test The NewNamespace() Constructor With Namespace Error Handling
func TestNewNamespaceServiceBusError(t *testing.T) {

	// Test Data
	connectionString := "FOO"

	// Create A Test Logger
	logger := logtesting.TestLogger(t).Desugar()

	// Perform The Test
	namespace, err := NewNamespace(logger, connectionString)

	// Verify Results
	assert.Nil(t, namespace)
	assert.NotNil(t, err)
}

// Test The NewNamespace() Constructor With HubManager Error Handling
func TestNewNamespaceHubManagerError(t *testing.T) {

	// Test Data
	serviceBus := "test-namespace.servicebus.windows.net"
	connectionString := fmt.Sprintf("Endpoint=sb://%s/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=TestSharedAccessKey", serviceBus)

	// Replace The NewHubManagerFromConnectionString Wrapper To Return Error & Defer Reset
	newHubManagerFromConnectionStringWrapperPlaceholder := NewHubManagerFromConnectionStringWrapper
	NewHubManagerFromConnectionStringWrapper = func(connectionString string) (managerInterface HubManagerInterface, e error) {
		return nil, fmt.Errorf("expected test error")
	}
	defer func() { NewHubManagerFromConnectionStringWrapper = newHubManagerFromConnectionStringWrapperPlaceholder }()

	// Create A Test Logger
	logger := logtesting.TestLogger(t).Desugar()

	// Perform The Test
	namespace, err := NewNamespace(logger, connectionString)

	// Verify Results
	assert.Nil(t, namespace)
	assert.NotNil(t, err)
}

// Test The parseServiceBusFromConnectionString() Functionality
func TestParseServiceBusFromConnectionString(t *testing.T) {

	// Define The TestCase
	type testCase struct {
		name             string
		connectionString string
		want             string
		wantErr          bool
	}

	// Define The TestCases
	testCases := []testCase{
		{
			name:             "Valid ConnectionString",
			connectionString: "Endpoint=sb://test-namespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=TestSharedAccessKey",
			want:             "test-namespace.servicebus.windows.net",
			wantErr:          false,
		},
		{
			name:             "Invalid ConnectionString",
			connectionString: "FOO-Endpoint=sb://test-namespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=TestSharedAccessKey",
			want:             "",
			wantErr:          true,
		},
	}

	// Loop Over The TestCases
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {

			// Perform The Individual TestCase
			got, err := parseServiceBusFromConnectionString(testCase.connectionString)
			if (err != nil) != testCase.wantErr {
				t.Errorf("parseServiceBusFromConnectionString() error = %v, wantErr %v", err, testCase.wantErr)
				return
			}
			if got != testCase.want {
				t.Errorf("parseServiceBusFromConnectionString() got = %v, want %v", got, testCase.want)
			}
		})
	}
}
