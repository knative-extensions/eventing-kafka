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
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	kafkasaramaprotocol "github.com/cloudevents/sdk-go/protocol/kafka_sarama/v2"
	"github.com/cloudevents/sdk-go/v2/binding"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"k8s.io/apimachinery/pkg/types"
	eventingduck "knative.dev/eventing/pkg/apis/duck/v1"
	"knative.dev/eventing/pkg/channel"
	"knative.dev/eventing/pkg/kncloudevents"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	logtesting "knative.dev/pkg/logging/testing"

	dispatchertesting "knative.dev/eventing-kafka/pkg/channel/distributed/dispatcher/testing"
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
	testConsumerGroupId  = fmt.Sprintf("kafka.%s", testSubscriberUID)
)

// Test The NewHandler() Functionality
func TestNewHandler(t *testing.T) {
	assert.NotNil(t, createTestHandler)
}

type HandleTestCase struct {
	only              bool
	name              string
	destinationUri    *apis.URL
	replyUri          *apis.URL
	deadLetterUri     *apis.URL
	dispatchErr       error
	retry             bool
	expectMarkMessage bool
}

// Test The Handler's Handle() Functionality
func TestHandle(t *testing.T) {

	// Define The HandleTestCases
	testCases := []HandleTestCase{
		{
			name:              "Complete Subscriber Configuration",
			destinationUri:    testSubscriberURI,
			replyUri:          testReplyURI,
			deadLetterUri:     testDeadLetterURI,
			retry:             true,
			expectMarkMessage: true,
		},
		{
			name:              "No Subscriber URL",
			replyUri:          testReplyURI,
			deadLetterUri:     testDeadLetterURI,
			retry:             true,
			expectMarkMessage: true,
		},
		{
			name:              "No Reply URL",
			destinationUri:    testSubscriberURI,
			deadLetterUri:     testDeadLetterURI,
			retry:             true,
			expectMarkMessage: true,
		},
		{
			name:              "No DeadLetter URL",
			destinationUri:    testSubscriberURI,
			replyUri:          testReplyURI,
			retry:             true,
			expectMarkMessage: true,
		},
		{
			name:              "No Retry",
			destinationUri:    testSubscriberURI,
			replyUri:          testReplyURI,
			deadLetterUri:     testDeadLetterURI,
			retry:             false,
			expectMarkMessage: true,
		},
		{
			name:              "Empty Subscriber Configuration",
			retry:             false,
			expectMarkMessage: true,
		},
		{
			name:              "Context Canceled",
			destinationUri:    testSubscriberURI,
			replyUri:          testReplyURI,
			deadLetterUri:     testDeadLetterURI,
			retry:             true,
			dispatchErr:       context.Canceled,
			expectMarkMessage: false,
		},
	}

	// Filter To Those With "only" Flag (If Any Specified)
	filteredTestCases := make([]HandleTestCase, 0)
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
			performHandleTest(t, testCase)
		})
	}
}

func TestSetReady(t *testing.T) {
	handler := createTestHandler(t, testSubscriberURI, testReplyURI, nil)
	handler.SetReady(1, true)
}

func TestGetConsumerGroup(t *testing.T) {
	handler := createTestHandler(t, testSubscriberURI, testReplyURI, nil)
	actualConsumerGroupId := handler.GetConsumerGroup()
	assert.Equal(t, testConsumerGroupId, actualConsumerGroupId)
}

// Test One Permutation Of The Handler's Handle() Functionality
func performHandleTest(t *testing.T, testCase HandleTestCase) {

	// Initialize Destination As Specified
	var destinationUrl *url.URL
	if testCase.destinationUri != nil {
		destinationUrl = testCase.destinationUri.URL()
	}

	// Initialize Reply As Specified
	var replyUrl *url.URL
	if testCase.replyUri != nil {
		replyUrl = testCase.replyUri.URL()
	}

	// Initialize DeadLetter As Specified
	var deadLetterUrl *url.URL
	if testCase.deadLetterUri != nil {
		deadLetterUrl = testCase.deadLetterUri.URL()
	}

	// Create The Specified DeliverySpec
	deliverySpec := createDeliverySpec(testCase.deadLetterUri, testCase.retry)

	// Create The Expected RetryConfig
	var retryConfig kncloudevents.RetryConfig
	if testCase.retry {
		var err error
		retryConfig, err = kncloudevents.RetryConfigFromDeliverySpec(deliverySpec)
		assert.NotNil(t, retryConfig)
		assert.Nil(t, err)
	}

	// Create Mocks For Testing
	mockMessageDispatcher := dispatchertesting.NewMockMessageDispatcher(t, nil, destinationUrl, replyUrl, deadLetterUrl, &retryConfig, testCase.dispatchErr)

	// Mock The newMessageDispatcherWrapper Function (And Restore Post-Test)
	newMessageDispatcherWrapperPlaceholder := newMessageDispatcherWrapper
	newMessageDispatcherWrapper = func(logger *zap.Logger) channel.MessageDispatcher {
		return mockMessageDispatcher
	}
	defer func() { newMessageDispatcherWrapper = newMessageDispatcherWrapperPlaceholder }()

	// Create The Handler To Test
	handler := createTestHandler(t, testCase.destinationUri, testCase.replyUri, &deliverySpec)

	// Perform The Test
	consumerMessage := createConsumerMessage(t)
	result, err := handler.Handle(context.TODO(), consumerMessage)

	// Verify The Results
	assert.Nil(t, err)
	assert.Equal(t, testCase.expectMarkMessage, result)
	assert.NotNil(t, mockMessageDispatcher.Message())
	verifyDispatchedMessage(t, mockMessageDispatcher.Message())
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
	handler := NewHandler(logger, testConsumerGroupId, testSubscriber)

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

func Test_executionInfoWrapper(t *testing.T) {
	for _, testCase := range []struct {
		name string
		info *channel.DispatchExecutionInfo
		want string
	}{
		{
			name: "Empty Info",
			info: &channel.DispatchExecutionInfo{},
			want: `"Time":0,"ResponseCode":0,"Body":""`,
		},
		{
			name: "Short Body",
			info: &channel.DispatchExecutionInfo{
				Time:         123 * time.Microsecond,
				ResponseCode: 200,
				ResponseBody: []byte("shortBody"),
			},
			want: `"Time":123000,"ResponseCode":200,"Body":"shortBody"`,
		},
		{
			name: "Long Body (max length)",
			info: &channel.DispatchExecutionInfo{
				Time:         1234 * time.Microsecond,
				ResponseCode: 500,
				ResponseBody: []byte(strings.Repeat("tencharstr", 50)),
			},
			want: `"Time":1234000,"ResponseCode":500,"Body":"` + strings.Repeat("tencharstr", 50) + `"`,
		},
		{
			name: "Long Body (truncated)",
			info: &channel.DispatchExecutionInfo{
				Time:         12345 * time.Microsecond,
				ResponseCode: 500,
				ResponseBody: []byte(strings.Repeat("tencharstr", 51)),
			},
			want: `"Time":12345000,"ResponseCode":500,"Body":"` + strings.Repeat("tencharstr", 50) + `..."`,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			zapField := zap.Any("test", executionInfoWrapper{testCase.info})
			buffer, err := zapcore.NewJSONEncoder(zapcore.EncoderConfig{}).EncodeEntry(zapcore.Entry{}, []zapcore.Field{zapField})
			assert.Nil(t, err)
			// The buffer contains more than just the encoded field, but this isn't supposed to be testing "the zapcore library"
			// so just see if the JSON string has what we're expecting in the middle of it
			assert.Contains(t, buffer.String(), testCase.want)
		})
	}
}
