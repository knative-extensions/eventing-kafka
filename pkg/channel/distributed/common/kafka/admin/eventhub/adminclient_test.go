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

package eventhub

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
	"knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/constants"
	logtesting "knative.dev/pkg/logging/testing"
)

// Test The NewAdminClient() Constructor
func TestNewAdminClient(t *testing.T) {

	// Test Data
	invalidConfig := sarama.NewConfig()
	validConfig := sarama.NewConfig()
	validConfig.Net.SASL.Password = "TestConnectionString"
	mockHubManager := &MockHubManager{}
	hubManagerErr := fmt.Errorf("test-hubmanager-error")

	// Define The TestCase Struct
	type TestCase struct {
		only              bool
		name              string
		config            *sarama.Config
		hubManager        HubManagerInterface
		hubManagerErr     error
		expectAdminClient bool
		expectErr         bool
	}

	// Create The TestCases
	testCases := []TestCase{
		{
			name:              "Success",
			config:            validConfig,
			hubManager:        mockHubManager,
			hubManagerErr:     nil,
			expectAdminClient: true,
			expectErr:         false,
		},
		{
			name:              "Nil Config Error",
			config:            nil,
			hubManager:        mockHubManager,
			hubManagerErr:     nil,
			expectAdminClient: false,
			expectErr:         true,
		},
		{
			name:              "Invalid Config Error",
			config:            invalidConfig,
			hubManager:        mockHubManager,
			hubManagerErr:     nil,
			expectAdminClient: false,
			expectErr:         true,
		},
		{
			name:              "Nil HubManager",
			config:            validConfig,
			hubManager:        nil,
			hubManagerErr:     nil,
			expectAdminClient: false,
			expectErr:         true,
		},
		{
			name:              "HubManager Error",
			config:            validConfig,
			hubManager:        nil,
			hubManagerErr:     hubManagerErr,
			expectAdminClient: false,
			expectErr:         true,
		},
	}

	// Filter To Those With "only" Flag (If Any Specified)
	filteredTestCases := make([]TestCase, 0)
	for _, testCase := range testCases {
		if testCase.only {
			filteredTestCases = append(filteredTestCases, testCase)
		}
	}
	if len(filteredTestCases) == 0 {
		filteredTestCases = testCases
	}

	// Run The TestCases
	for _, testCase := range filteredTestCases {
		t.Run(testCase.name, func(t *testing.T) {

			// Stub The NewHubManagerFromConnectionStringWrapper
			newHubManagerFromConnectionStringWrapperPlaceholder := NewHubManagerFromConnectionStringWrapper
			NewHubManagerFromConnectionStringWrapper = func(connectionString string) (HubManagerInterface, error) {
				assert.Equal(t, testCase.config.Net.SASL.Password, connectionString)
				return testCase.hubManager, testCase.hubManagerErr
			}

			// Perform The Test
			adminClient, err := NewAdminClient(context.TODO(), testCase.config)

			// Restore The NewHubManagerFromConnectionStringWrapper
			NewHubManagerFromConnectionStringWrapper = newHubManagerFromConnectionStringWrapperPlaceholder

			// Verify The Results
			assert.Equal(t, testCase.expectAdminClient, adminClient != nil)
			assert.Equal(t, testCase.expectErr, err != nil)
		})
	}
}

// Test The CreateTopic() Functionality
func TestCreateTopic(t *testing.T) {

	// Test Data
	ctx := context.TODO()
	logger := logtesting.TestLogger(t).Desugar()
	topicName := "TestTopicName"
	numPartitions := int32(4)
	topicRetentionDays := int32(3)
	topicRetentionMillis := int64(topicRetentionDays * constants.MillisPerDay)
	validTopicRetentionMillisString := strconv.FormatInt(topicRetentionMillis, 10)
	invalidTopicRetentionMillisString := "Invalid RetentionMillis"
	validTopicDetail := createTopicDetail(numPartitions, validTopicRetentionMillisString)
	invalidTopicDetail := createTopicDetail(numPartitions, invalidTopicRetentionMillisString)

	// Define The TestCase Struct
	type TestCase struct {
		only           bool
		name           string
		mockHubManager *MockHubManager
		topicDetail    *sarama.TopicDetail
		expectedKError sarama.KError
	}

	// Create The TestCases
	testCases := []TestCase{
		{
			name:           "Success",
			mockHubManager: NewMockHubManager(WithMockedPut(ctx, topicName, false, 0)),
			topicDetail:    validTopicDetail,
			expectedKError: sarama.ErrNoError,
		},
		{
			name:           "Invalid TopicDetail",
			topicDetail:    invalidTopicDetail,
			expectedKError: sarama.ErrInvalidConfig,
		},
		{
			name:           "Nil HubManager",
			mockHubManager: nil,
			topicDetail:    validTopicDetail,
			expectedKError: sarama.ErrInvalidConfig,
		},
		{
			name:           "Conflict ErrorCode",
			mockHubManager: NewMockHubManager(WithMockedPut(ctx, topicName, true, constants.EventHubErrorCodeConflict)),
			topicDetail:    validTopicDetail,
			expectedKError: sarama.ErrTopicAlreadyExists,
		},
		{
			name:           "CapacityLimit ErrorCode",
			mockHubManager: NewMockHubManager(WithMockedPut(ctx, topicName, true, constants.EventHubErrorCodeCapacityLimit)),
			topicDetail:    validTopicDetail,
			expectedKError: sarama.ErrInvalidTxnState,
		},
		{
			name:           "Unknown ErrorCode",
			mockHubManager: NewMockHubManager(WithMockedPut(ctx, topicName, true, constants.EventHubErrorCodeUnknown)),
			topicDetail:    validTopicDetail,
			expectedKError: sarama.ErrUnknown,
		},
		{
			name:           "ParseFailure ErrorCode",
			mockHubManager: NewMockHubManager(WithMockedPut(ctx, topicName, true, constants.EventHubErrorCodeParseFailure)),
			topicDetail:    validTopicDetail,
			expectedKError: sarama.ErrUnknown,
		},
		{
			name:           "Unmapped ErrorCode",
			mockHubManager: NewMockHubManager(WithMockedPut(ctx, topicName, true, 999)),
			topicDetail:    validTopicDetail,
			expectedKError: sarama.ErrUnknown,
		},
	}

	// Filter To Those With "only" Flag (If Any Specified)
	filteredTestCases := make([]TestCase, 0)
	for _, testCase := range testCases {
		if testCase.only {
			filteredTestCases = append(filteredTestCases, testCase)
		}
	}
	if len(filteredTestCases) == 0 {
		filteredTestCases = testCases
	}

	// Run The TestCases
	for _, testCase := range filteredTestCases {
		t.Run(testCase.name, func(t *testing.T) {

			// Create A New EventHub AdminClient With Mock HubManager To Test
			adminClient := &EventHubAdminClient{logger: logger}
			if testCase.mockHubManager != nil {
				adminClient.hubManager = testCase.mockHubManager
			}

			// Perform The Test
			resultTopicError := adminClient.CreateTopic(ctx, topicName, testCase.topicDetail)

			// Verify The Results
			assert.NotNil(t, resultTopicError)
			assert.Equal(t, testCase.expectedKError, resultTopicError.Err)
			if testCase.mockHubManager != nil {
				testCase.mockHubManager.AssertExpectations(t)
			}
		})
	}
}

// Test The DeleteTopic() Functionality
func TestDeleteTopic(t *testing.T) {

	// Test Data
	ctx := context.TODO()
	logger := logtesting.TestLogger(t).Desugar()
	topicName := "TestTopicName"

	// Define The TestCase Struct
	type TestCase struct {
		only           bool
		name           string
		mockHubManager *MockHubManager
		expectedKError sarama.KError
	}

	// Create The TestCases
	testCases := []TestCase{
		{
			name:           "Success",
			mockHubManager: NewMockHubManager(WithMockedDelete(ctx, topicName, false, 0)),
			expectedKError: sarama.ErrNoError,
		},
		{
			name:           "Nil HubManager",
			mockHubManager: nil,
			expectedKError: sarama.ErrInvalidConfig,
		},
		{
			name:           "Delete Error",
			mockHubManager: NewMockHubManager(WithMockedDelete(ctx, topicName, true, 999)),
			expectedKError: sarama.ErrUnknown,
		},
	}

	// Filter To Those With "only" Flag (If Any Specified)
	filteredTestCases := make([]TestCase, 0)
	for _, testCase := range testCases {
		if testCase.only {
			filteredTestCases = append(filteredTestCases, testCase)
		}
	}
	if len(filteredTestCases) == 0 {
		filteredTestCases = testCases
	}

	// Run The TestCases
	for _, testCase := range filteredTestCases {
		t.Run(testCase.name, func(t *testing.T) {

			// Create A New EventHub AdminClient With Mock HubManager To Test
			adminClient := &EventHubAdminClient{logger: logger}
			if testCase.mockHubManager != nil {
				adminClient.hubManager = testCase.mockHubManager
			}

			// Perform The Test
			resultTopicError := adminClient.DeleteTopic(ctx, topicName)

			// Verify The Results
			assert.NotNil(t, resultTopicError)
			assert.Equal(t, testCase.expectedKError, resultTopicError.Err)
			if testCase.mockHubManager != nil {
				testCase.mockHubManager.AssertExpectations(t)
			}
		})
	}
}

// Test The Close() Functionality
func TestClose(t *testing.T) {

	// Create A Test Logger
	logger := logtesting.TestLogger(t).Desugar()

	// Create A New EventHub AdminClient To Test
	adminClient := &EventHubAdminClient{logger: logger}

	// Perform The Test
	err := adminClient.Close()

	// Nothing To Verify (No-Op)
	assert.Nil(t, err)
}

//
// Test Utilities
//

// Create A Sarama TopicDetail For Testing
func createTopicDetail(numPartitions int32, retentionMsString string) *sarama.TopicDetail {
	return &sarama.TopicDetail{
		NumPartitions: numPartitions,
		ConfigEntries: map[string]*string{constants.TopicDetailConfigRetentionMs: &retentionMsString},
	}
}
