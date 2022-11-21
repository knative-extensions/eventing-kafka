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

package custom

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
	"knative.dev/pkg/logging"
	logtesting "knative.dev/pkg/logging/testing"

	"knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/constants"
)

// Test The NewAdminClient() Functionality
func TestNewAdminClient(t *testing.T) {

	// Create A Context With Test Logger
	logger := logtesting.TestLogger(t)
	ctx := logging.WithLogger(context.TODO(), logger)

	// Perform The Test
	adminClient, err := NewAdminClient(ctx)

	// Verify The Result
	assert.Nil(t, err)
	assert.NotNil(t, adminClient)
	customAdminClient, ok := adminClient.(*CustomAdminClient)
	assert.True(t, ok)
	assert.NotNil(t, customAdminClient)
	assert.NotEqual(t, logger, customAdminClient.logger)
	assert.NotNil(t, customAdminClient.httpClient)
}

// Test The CreateTopic() Functionality
func TestCreateTopic(t *testing.T) {

	// Test Data
	topicName := "TestTopicName"
	topicNumPartitions := int32(4)
	topicReplicationFactor := int16(2)
	topicRetentionDays := int32(3)
	topicRetentionMillis := int64(topicRetentionDays * constants.MillisPerDay)
	topicRetentionMillisString := strconv.FormatInt(topicRetentionMillis, 10)

	// Create & Start The Test Sidecar HTTP Server (Success Response) & Defer Close
	mockSidecarServer := NewMockSidecarServer(t, http.StatusOK)
	mockSidecarServer.Start()
	defer mockSidecarServer.Close()

	// Create A Context With Test Logger
	logger := logtesting.TestLogger(t)
	ctx := logging.WithLogger(context.TODO(), logger)

	// Create The Sarama TopicDetail For The Topic/EventHub To Be Created
	topicDetail := &sarama.TopicDetail{
		NumPartitions:     topicNumPartitions,
		ReplicationFactor: topicReplicationFactor,
		ConfigEntries:     map[string]*string{constants.TopicDetailConfigRetentionMs: &topicRetentionMillisString},
	}

	// Create A New Custom AdminClient
	adminClient, err := NewAdminClient(ctx)
	assert.Nil(t, err)
	assert.NotNil(t, adminClient)

	// Perform The Test
	resultTopicError := adminClient.CreateTopic(ctx, topicName, topicDetail)

	// Verify The Results
	assert.NotNil(t, resultTopicError)
	assert.Equal(t, sarama.ErrNoError, resultTopicError.Err)
	assert.Equal(t, "custom sidecar topic 'create' operation succeeded with status code '200' and body ''", *resultTopicError.ErrMsg)
	assert.Equal(t, 1, len(mockSidecarServer.requests))
	for request, body := range mockSidecarServer.requests {
		verifySidecarRequest(t, request, body, topicName, topicDetail)
	}
}

// Test The DeleteTopic() Functionality
func TestDeleteTopic(t *testing.T) {

	// Test Data
	topicName := "TestTopicName"

	// Create & Start The Test Sidecar HTTP Server (Success Response) & Defer Close
	mockSidecarServer := NewMockSidecarServer(t, http.StatusOK)
	mockSidecarServer.Start()
	defer mockSidecarServer.Close()

	// Create A Context With Test Logger
	logger := logtesting.TestLogger(t)
	ctx := logging.WithLogger(context.TODO(), logger)

	// Create A New Custom AdminClient
	adminClient, err := NewAdminClient(ctx)
	assert.Nil(t, err)
	assert.NotNil(t, adminClient)

	// Perform The Test
	resultTopicError := adminClient.DeleteTopic(ctx, topicName)

	// Verify The Results
	assert.NotNil(t, resultTopicError)
	assert.Equal(t, sarama.ErrNoError, resultTopicError.Err)
	assert.Equal(t, "custom sidecar topic 'delete' operation succeeded with status code '200' and body ''", *resultTopicError.ErrMsg)
	assert.Equal(t, 1, len(mockSidecarServer.requests))
	for request, body := range mockSidecarServer.requests {
		verifySidecarRequest(t, request, body, topicName, nil)
	}
}

// Test The Close() Functionality
func TestClose(t *testing.T) {

	// Create A New Custom AdminClient To Test
	adminClient := &CustomAdminClient{}

	// Perform The Test
	err := adminClient.Close()

	// Verify The Results
	assert.Nil(t, err)
}

// Test The mapHttpResponse() Functionality
func TestMapHttpResponse(t *testing.T) {

	// Test Data
	bodyString := "TestBody"
	bodyBytes := []byte(bodyString)

	// Define The TestCase
	type testCase struct {
		only      bool
		name      string
		operation string
		response  *http.Response
		expected  *sarama.TopicError
	}

	// Define The TestCases
	testCases := []testCase{
		{
			name:      "Create 100",
			operation: "create",
			response:  &http.Response{StatusCode: 100, Body: io.NopCloser(bytes.NewReader(bodyBytes))},
			expected:  &sarama.TopicError{Err: sarama.ErrInvalidRequest},
		},
		{
			name:      "Delete 100",
			operation: "delete",
			response:  &http.Response{StatusCode: 100, Body: io.NopCloser(bytes.NewReader(bodyBytes))},
			expected:  &sarama.TopicError{Err: sarama.ErrInvalidRequest},
		},
		{
			name:      "Create 200",
			operation: "create",
			response:  &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewReader(bodyBytes))},
			expected:  &sarama.TopicError{Err: sarama.ErrNoError},
		},
		{
			name:      "Delete 200",
			operation: "delete",
			response:  &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewReader(bodyBytes))},
			expected:  &sarama.TopicError{Err: sarama.ErrNoError},
		},
		{
			name:      "Create 300",
			operation: "create",
			response:  &http.Response{StatusCode: 300, Body: io.NopCloser(bytes.NewReader(bodyBytes))},
			expected:  &sarama.TopicError{Err: sarama.ErrInvalidRequest},
		},
		{
			name:      "Delete 300",
			operation: "delete",
			response:  &http.Response{StatusCode: 300, Body: io.NopCloser(bytes.NewReader(bodyBytes))},
			expected:  &sarama.TopicError{Err: sarama.ErrInvalidRequest},
		},
		{
			name:      "Create 404",
			operation: "create",
			response:  &http.Response{StatusCode: 404, Body: io.NopCloser(bytes.NewReader(bodyBytes))},
			expected:  &sarama.TopicError{Err: sarama.ErrInvalidRequest},
		},
		{
			name:      "Delete 404",
			operation: "delete",
			response:  &http.Response{StatusCode: 404, Body: io.NopCloser(bytes.NewReader(bodyBytes))},
			expected:  &sarama.TopicError{Err: sarama.ErrUnknownTopicOrPartition},
		},
		{
			name:      "Create 409",
			operation: "create",
			response:  &http.Response{StatusCode: 409, Body: io.NopCloser(bytes.NewReader(bodyBytes))},
			expected:  &sarama.TopicError{Err: sarama.ErrTopicAlreadyExists},
		},
		{
			name:      "Delete 409",
			operation: "delete",
			response:  &http.Response{StatusCode: 409, Body: io.NopCloser(bytes.NewReader(bodyBytes))},
			expected:  &sarama.TopicError{Err: sarama.ErrInvalidRequest},
		},
		{
			name:      "Create 500",
			operation: "create",
			response:  &http.Response{StatusCode: 500, Body: io.NopCloser(bytes.NewReader(bodyBytes))},
			expected:  &sarama.TopicError{Err: sarama.ErrInvalidRequest},
		},
		{
			name:      "Delete 500",
			operation: "delete",
			response:  &http.Response{StatusCode: 500, Body: io.NopCloser(bytes.NewReader(bodyBytes))},
			expected:  &sarama.TopicError{Err: sarama.ErrInvalidRequest},
		},
	}

	// Filter To Those With "only" Flag (If Any Specified)
	filteredTestCases := make([]testCase, 0)
	for _, testCase := range testCases {
		if testCase.only {
			filteredTestCases = append(filteredTestCases, testCase)
		}
	}
	if len(filteredTestCases) == 0 {
		filteredTestCases = testCases
	}

	// Create A New Custom AdminClient To Test
	adminClient := &CustomAdminClient{}

	// Loop Over The Filtered TestCases
	for _, testCase := range filteredTestCases {
		t.Run(testCase.name, func(t *testing.T) {

			// Perform The Individual TestCase
			actual := adminClient.mapHttpResponse(testCase.operation, testCase.response)
			assert.Equal(t, testCase.expected.Err, actual.Err)
		})
	}
}

//
// Test HTTP Server - Pretending To Be The Custom Sidecar
//

// MockSidecarServer Struct
type MockSidecarServer struct {
	t          *testing.T
	statusCode int
	server     *httptest.Server
	requests   map[*http.Request][]byte // Map Of Request Pointers To BodyBytes For Tracking Requests For Subsequent Validation
}

// MockSidecarServer Constructor
func NewMockSidecarServer(t *testing.T, statusCode int) *MockSidecarServer {

	// Create The Mock Sidecar Server
	mockSidecarServer := &MockSidecarServer{
		t:          t,
		statusCode: statusCode,
		requests:   make(map[*http.Request][]byte),
	}

	// Create A Custom TCP Listener On The Expected Sidecar Host:Port
	listener, err := net.Listen("tcp", SidecarHost+":"+SidecarPort)
	assert.Nil(t, err)
	assert.NotNil(t, listener)

	// Create A Test HTTP Server With Handler Function & Custom Listener
	mockSidecarServer.server = httptest.NewUnstartedServer(mockSidecarServer)
	mockSidecarServer.server.Listener = listener

	// Return The Initialized Mock Sidecar Server
	return mockSidecarServer
}

// Start The Mock Http Server
func (s *MockSidecarServer) Start() {
	if s.server != nil {
		s.server.Start()
	}
}

// Close The Mock Http Server
func (s *MockSidecarServer) Close() {
	if s.server != nil {
		s.server.Close()
	}
}

// The HTTP Handler Interface Implementation
func (s *MockSidecarServer) ServeHTTP(responseWriter http.ResponseWriter, request *http.Request) {

	// Read The Request Body Before Responding (It Will Otherwise Be Closed!)
	bodyBytes, err := io.ReadAll(request.Body)
	assert.Nil(s.t, err)

	// Track The Received HTTP Request & Body For Future Validation
	s.requests[request] = bodyBytes

	// Return The Desired StatusCode
	responseWriter.WriteHeader(s.statusCode)
}

// Utility Function For Verifying The Inbound HTTP Request (What Is Sent To The Sidecar)
func verifySidecarRequest(t *testing.T, request *http.Request, body []byte, topicName string, saramaTopicDetail *sarama.TopicDetail) {

	// Verify Common Request Data
	assert.Equal(t, SidecarHost+":"+SidecarPort, request.Host)

	// Verify Method Specific Request Data
	switch request.Method {

	case http.MethodPost:
		assert.Equal(t, TopicsPath, request.URL.Path)
		assert.Equal(t, topicName, request.Header.Get(TopicNameHeader))
		customTopicDetail := &TopicDetail{}
		err := json.Unmarshal(body, customTopicDetail)
		assert.Nil(t, err)
		assert.NotNil(t, customTopicDetail)
		assert.Equal(t, saramaTopicDetail.NumPartitions, customTopicDetail.NumPartitions)
		assert.Equal(t, saramaTopicDetail.ReplicationFactor, customTopicDetail.ReplicationFactor)
		assert.Equal(t, saramaTopicDetail.ConfigEntries, customTopicDetail.ConfigEntries)
		assert.Equal(t, saramaTopicDetail.ReplicaAssignment, customTopicDetail.ReplicaAssignment)

	case http.MethodDelete:
		assert.Equal(t, TopicsPath+"/"+topicName, request.URL.Path)
		assert.Equal(t, "", request.Header.Get(TopicNameHeader))
		assert.Empty(t, body)

	default:
		assert.Fail(t, "Unexpected Request Method Sent To Sidecar")
	}
}
