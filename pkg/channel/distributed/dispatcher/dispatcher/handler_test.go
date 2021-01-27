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

package dispatcher

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	kafkasaramaprotocol "github.com/cloudevents/sdk-go/protocol/kafka_sarama/v2"
	"github.com/cloudevents/sdk-go/v2/binding"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/types"
	dispatchertesting "knative.dev/eventing-kafka/pkg/channel/distributed/dispatcher/testing"
	eventingduck "knative.dev/eventing/pkg/apis/duck/v1"
	"knative.dev/eventing/pkg/channel"
	"knative.dev/eventing/pkg/kncloudevents"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	logtesting "knative.dev/pkg/logging/testing"
)

// Test Data
const (
	testSubscriberUID        = types.UID("123")
	testSubscriberURIString  = "https://www.foo.bar/test/path"
	testReplyURIString       = "https://www.something.com/"
	testDeadLetterURIString  = "https://www.made.up/url"
	testTopic                = "TestTopic"
	testPartition            = 0
	testOffset               = 1
	testMsgSpecVersion       = "1.0"
	testMsgContentType       = "application/json"
	testMsgId                = "TestMsgId"
	testMsgSource            = "TestMsgSource"
	testMsgType              = "TestMsgType"
	testMsgEventTypeVersion  = "TestMsgEventTypeVersion"
	testMsgKnativeHistory    = "TestKnativeHistory"
	testMsgJsonContentString = "{\"content\": \"Test Message 1\"}"
)

var (
	testSubscriberURI, _ = apis.ParseURL(testSubscriberURIString)
	testReplyURI, _      = apis.ParseURL(testReplyURIString)
	testRetryCount       = int32(4)
	testBackoffPolicy    = eventingduck.BackoffPolicyExponential
	testBackoffDelay     = "PT1S"
	testDeadLetterURI, _ = apis.ParseURL(testDeadLetterURIString)
	testMsgTime          = time.Now().UTC().Format(time.RFC3339)
)

// Test The NewHandler() Functionality
func TestNewHandler(t *testing.T) {
	assert.NotNil(t, createTestHandler)
}

// Test The Handler's Setup() Functionality
func TestHandlerSetup(t *testing.T) {
	handler := createTestHandler(t, testSubscriberURI, testReplyURI, nil)
	assert.Nil(t, handler.Setup(nil))
}

// Test The Handler's Cleanup() Functionality
func TestHandlerCleanup(t *testing.T) {
	handler := createTestHandler(t, testSubscriberURI, testReplyURI, nil)
	assert.Nil(t, handler.Cleanup(nil))
}

// Test The Handler's ConsumeClaim() Functionality
func TestHandlerConsumeClaim(t *testing.T) {

	// Define The TestCase Type
	type TestCase struct {
		only           bool
		name           string
		destinationUri *apis.URL
		replyUri       *apis.URL
		deadLetterUri  *apis.URL
		retry          bool
	}

	// Define The TestCases
	testCases := []TestCase{
		{
			name:           "Complete Subscriber Configuration",
			destinationUri: testSubscriberURI,
			replyUri:       testReplyURI,
			deadLetterUri:  testDeadLetterURI,
			retry:          true,
		},
		{
			name:          "No Subscriber URL",
			replyUri:      testReplyURI,
			deadLetterUri: testDeadLetterURI,
			retry:         true,
		},
		{
			name:           "No Reply URL",
			destinationUri: testSubscriberURI,
			deadLetterUri:  testDeadLetterURI,
			retry:          true,
		},
		{
			name:           "No DeadLetter URL",
			destinationUri: testSubscriberURI,
			replyUri:       testReplyURI,
			retry:          true,
		},
		{
			name:           "No Retry",
			destinationUri: testSubscriberURI,
			replyUri:       testReplyURI,
			deadLetterUri:  testDeadLetterURI,
			retry:          false,
		},
		{
			name:  "Empty Subscriber Configuration",
			retry: false,
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

	// Execute The Individual Test Cases
	for _, testCase := range filteredTestCases {
		t.Run(testCase.name, func(t *testing.T) {
			performHandlerConsumeClaimTest(t, testCase.destinationUri, testCase.replyUri, testCase.deadLetterUri, testCase.retry)
		})
	}
}

// Test One Permutation Of The Handler's ConsumeClaim() Functionality
func performHandlerConsumeClaimTest(t *testing.T, destinationUri, replyUri, deadLetterUri *apis.URL, retry bool) {

	// Initialize Destination As Specified
	var destinationUrl *url.URL
	if destinationUri != nil {
		destinationUrl = destinationUri.URL()
	}

	// Initialize Reply As Specified
	var replyUrl *url.URL
	if replyUri != nil {
		replyUrl = replyUri.URL()
	}

	// Initialize DeadLetter As Specified
	var deadLetterUrl *url.URL
	if deadLetterUri != nil {
		deadLetterUrl = deadLetterUri.URL()
	}

	// Create The Specified DeliverySpec
	deliverySpec := createDeliverySpec(deadLetterUri, retry)

	// Create The Expected RetryConfig
	var retryConfig kncloudevents.RetryConfig
	if retry {
		var err error
		retryConfig, err = kncloudevents.RetryConfigFromDeliverySpec(deliverySpec)
		assert.NotNil(t, retryConfig)
		assert.Nil(t, err)
	}

	// Create Mocks For Testing
	mockConsumerGroupSession := dispatchertesting.NewMockConsumerGroupSession(t)
	mockConsumerGroupClaim := dispatchertesting.NewMockConsumerGroupClaim(t)
	mockMessageDispatcher := dispatchertesting.NewMockMessageDispatcher(t, nil, destinationUrl, replyUrl, deadLetterUrl, &retryConfig, nil)

	// Mock The newMessageDispatcherWrapper Function (And Restore Post-Test)
	newMessageDispatcherWrapperPlaceholder := newMessageDispatcherWrapper
	newMessageDispatcherWrapper = func(logger *zap.Logger) channel.MessageDispatcher {
		return mockMessageDispatcher
	}
	defer func() { newMessageDispatcherWrapper = newMessageDispatcherWrapperPlaceholder }()

	// Create The Handler To Test
	handler := createTestHandler(t, destinationUri, replyUri, &deliverySpec)

	// Background Start Consuming Claims
	go func() {
		err := handler.ConsumeClaim(mockConsumerGroupSession, mockConsumerGroupClaim)
		assert.Nil(t, err)
	}()

	// Perform The Test (Add ConsumerMessages To Claims)
	consumerMessage := createConsumerMessage(t)
	mockConsumerGroupClaim.MessageChan <- consumerMessage

	// Wait For Message To Be Marked As Complete
	markedMessage := <-mockConsumerGroupSession.MarkMessageChan

	// Close The Mock ConsumerGroupClaim Message Channel To Complete/Exit Handler's ConsumeClaim()
	close(mockConsumerGroupClaim.MessageChan)

	// Verify The Results (CloudEvent Was Dispatched & ConsumerMessage Was Marked)
	assert.Equal(t, consumerMessage, markedMessage)
	assert.NotNil(t, mockMessageDispatcher.Message())
	verifyDispatchedMessage(t, mockMessageDispatcher.Message())
}

// Test The Custom CheckRetry() Implementation
func TestCheckRetry(t *testing.T) {

	// Define The TestCase Type
	type TestCase struct {
		only     bool
		name     string
		response *http.Response
		err      error
		result   bool
	}

	// Define The TestCases
	testCases := []TestCase{
		{
			name:   "Nil Response",
			result: true,
		},
		{
			name:     "Http Error",
			response: &http.Response{StatusCode: http.StatusOK},
			err:      errors.New("test error"),
			result:   true,
		},
		{
			name:     "Http StatusCode -1",
			response: &http.Response{StatusCode: -1},
			result:   true,
		},
		{
			name:     "Http StatusCode 100",
			response: &http.Response{StatusCode: http.StatusContinue},
			result:   false,
		},
		{
			name:     "Http StatusCode 102",
			response: &http.Response{StatusCode: http.StatusProcessing},
			result:   false,
		},
		{
			name:     "Http StatusCode 200",
			response: &http.Response{StatusCode: http.StatusOK},
			result:   false,
		},
		{
			name:     "Http StatusCode 201",
			response: &http.Response{StatusCode: http.StatusCreated},
			result:   false,
		},
		{
			name:     "Http StatusCode 202",
			response: &http.Response{StatusCode: http.StatusAccepted},
			result:   false,
		},
		{
			name:     "Http StatusCode 300",
			response: &http.Response{StatusCode: http.StatusMultipleChoices},
			result:   false,
		},
		{
			name:     "Http StatusCode 301",
			response: &http.Response{StatusCode: http.StatusMovedPermanently},
			result:   false,
		},
		{
			name:     "Http StatusCode 400",
			response: &http.Response{StatusCode: http.StatusBadRequest},
			result:   false,
		},
		{
			name:     "Http StatusCode 401",
			response: &http.Response{StatusCode: http.StatusUnauthorized},
			result:   false,
		},
		{
			name:     "Http StatusCode 403",
			response: &http.Response{StatusCode: http.StatusForbidden},
			result:   false,
		},
		{
			name:     "Http StatusCode 404",
			response: &http.Response{StatusCode: http.StatusNotFound},
			result:   true,
		},
		{
			name:     "Http StatusCode 429",
			response: &http.Response{StatusCode: http.StatusTooManyRequests},
			result:   true,
		},
		{
			name:     "Http StatusCode 500",
			response: &http.Response{StatusCode: http.StatusInternalServerError},
			result:   true,
		},
		{
			name:     "Http StatusCode 501",
			response: &http.Response{StatusCode: http.StatusNotImplemented},
			result:   true,
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

	// Create A Handler To Test
	handler := createTestHandler(t, testSubscriberURI, testReplyURI, nil)
	assert.Nil(t, handler.Cleanup(nil))

	// Create A Test Context (Pacify Linter Nil Context Check ;)
	ctx := context.TODO()

	// Execute The Individual Test Cases
	for _, testCase := range filteredTestCases {
		t.Run(testCase.name, func(t *testing.T) {
			result, err := handler.checkRetry(ctx, testCase.response, testCase.err)
			assert.Equal(t, testCase.result, result)
			assert.Nil(t, err)
		})
	}
}

// Verify The Dispatched Message Contains Test Message Contents (Was Not Corrupted)
func verifyDispatchedMessage(t *testing.T, message binding.MessageReader) {
	dispatchedEvent, err := binding.ToEvent(context.TODO(), message)
	assert.NotNil(t, dispatchedEvent)
	assert.Nil(t, err)
	assert.Equal(t, testMsgId, dispatchedEvent.Context.GetID())
	assert.Equal(t, testMsgSource, dispatchedEvent.Context.GetSource())
	assert.Equal(t, testMsgType, dispatchedEvent.Context.GetType())
	eventTypeVersionExtension, err := dispatchedEvent.Context.GetExtension("eventtypeversion")
	assert.Nil(t, err)
	assert.Equal(t, testMsgEventTypeVersion, eventTypeVersionExtension)
	knativeHistoryExtension, err := dispatchedEvent.Context.GetExtension("knativehistory")
	assert.Nil(t, err)
	assert.Equal(t, testMsgKnativeHistory, knativeHistoryExtension)
	assert.Equal(t, testMsgContentType, dispatchedEvent.Context.GetDataContentType())
	assert.Equal(t, testMsgTime, dispatchedEvent.Context.GetTime().Format(time.RFC3339))
	assert.Equal(t, testMsgJsonContentString, string(dispatchedEvent.DataEncoded))
}

// Utility Function Fro Creating A Test DeliverySpec
func createDeliverySpec(deadLetterUri *apis.URL, retry bool) eventingduck.DeliverySpec {

	// Create An Empty DeliverySpec
	deliverySpec := eventingduck.DeliverySpec{}

	// Populate DeadLetter URL If Specified
	if deadLetterUri != nil {
		deliverySpec.DeadLetterSink = &duckv1.Destination{
			URI: deadLetterUri,
		}
	}

	// Populate Retry Config If Specified
	if retry {
		deliverySpec.Retry = &testRetryCount
		deliverySpec.BackoffPolicy = &testBackoffPolicy
		deliverySpec.BackoffDelay = &testBackoffDelay
	}

	// Return The Configured DeliverySpec
	return deliverySpec
}

// Utility Function For Creating New Handler
func createTestHandler(t *testing.T, subscriberURL *apis.URL, replyUrl *apis.URL, delivery *eventingduck.DeliverySpec) *Handler {

	// Test Data
	logger := logtesting.TestLogger(t).Desugar()
	testSubscriber := &eventingduck.SubscriberSpec{
		UID:           testSubscriberUID,
		Generation:    0,
		SubscriberURI: subscriberURL,
		ReplyURI:      replyUrl,
		Delivery:      delivery,
	}

	// Perform The Test Create The Test Handler
	handler := NewHandler(logger, testSubscriber)

	// Verify The Results
	assert.NotNil(t, handler)
	assert.Equal(t, logger, handler.Logger)
	assert.Equal(t, testSubscriber, handler.Subscriber)
	assert.NotNil(t, handler.MessageDispatcher)

	// Return The Handler
	return handler
}

// Utility Function For Creating Valid ConsumerMessages
func createConsumerMessage(t *testing.T) *sarama.ConsumerMessage {

	// Create The ConsumerMessage To Test (Matches What Comes Out Of Knative MessageReceiver)
	consumerMessage := &sarama.ConsumerMessage{
		Headers: []*sarama.RecordHeader{
			{
				Key:   []byte("content-type"),
				Value: []byte(testMsgContentType),
			},
			{
				Key:   []byte("ce_specversion"),
				Value: []byte(testMsgSpecVersion),
			},
			{
				Key:   []byte("ce_time"),
				Value: []byte(testMsgTime),
			},
			{
				Key:   []byte("ce_id"),
				Value: []byte(testMsgId),
			},
			{
				Key:   []byte("ce_source"),
				Value: []byte(testMsgSource),
			},
			{
				Key:   []byte("ce_type"),
				Value: []byte(testMsgType),
			},
			{
				Key:   []byte("ce_eventtypeversion"),
				Value: []byte(testMsgEventTypeVersion),
			},
			{
				Key:   []byte("ce_knativehistory"),
				Value: []byte(testMsgKnativeHistory),
			},
		},
		Timestamp:      time.Now(),
		BlockTimestamp: time.Time{},
		Key:            nil,
		Value:          []byte(testMsgJsonContentString),
		Topic:          testTopic,
		Partition:      testPartition,
		Offset:         testOffset,
	}

	// Quick Run Through Of CloudEvents SDK Binding Message Conversion To Ensure Validity
	assert.Equal(t, binding.EncodingBinary, kafkasaramaprotocol.NewMessageFromConsumerMessage(consumerMessage).ReadEncoding())

	// Return The Test ConsumerMessage
	return consumerMessage
}
