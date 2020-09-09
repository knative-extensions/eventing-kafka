package dispatcher

import (
	"context"
	"errors"
	"github.com/Shopify/sarama"
	kafkasaramaprotocol "github.com/cloudevents/sdk-go/protocol/kafka_sarama/v2"
	"github.com/cloudevents/sdk-go/v2/binding"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/types"
	dispatchertesting "knative.dev/eventing-kafka/pkg/channel/distributed/dispatcher/testing"
	eventingduck "knative.dev/eventing/pkg/apis/duck/v1"
	"knative.dev/eventing/pkg/channel"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	logtesting "knative.dev/pkg/logging/testing"
	"net/url"
	"testing"
	"time"
)

// Test Data
const (
	testSubscriberUID        = types.UID("123")
	testExponentialBackoff   = true
	testInitialRetryInterval = int64(111)
	testMaxRetryTime         = int64(22)
	testSubscriberURIString  = "https://www.foo.bar/test/path"
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
	testSubscriberURL    = testSubscriberURI.URL()
	testDeadLetterURI, _ = apis.ParseURL(testDeadLetterURIString)
	testMsgTime          = time.Now().UTC().Format(time.RFC3339)
)

// Test The NewHandler() Functionality
func TestNewHandler(t *testing.T) {
	assert.NotNil(t, createTestHandler)
}

// Test The Handler's Setup() Functionality
func TestHandlerSetup(t *testing.T) {
	handler := createTestHandler(t, testSubscriberURI, nil)
	assert.Nil(t, handler.Setup(nil))
}

// Test The Handler's Cleanup() Functionality
func TestHandlerCleanup(t *testing.T) {
	handler := createTestHandler(t, testSubscriberURI, nil)
	assert.Nil(t, handler.Cleanup(nil))
}

// Test The Handler's ConsumeClaim() Functionality
func TestHandlerConsumeClaim(t *testing.T) {
	performHandlerConsumeClaimTest(t, nil, nil, false)                                                         // Vanilla Success
	performHandlerConsumeClaimTest(t, nil, errors.New("test error 300"), false)                                // 300 StatusCode No Retry
	performHandlerConsumeClaimTest(t, nil, errors.New("test error 400"), true)                                 // 400 StatusCode Retry
	performHandlerConsumeClaimTest(t, nil, errors.New("test error 404"), true)                                 // 404 StatusCode Retry
	performHandlerConsumeClaimTest(t, nil, errors.New("test error 429"), true)                                 // 429 StatusCode Retry
	performHandlerConsumeClaimTest(t, nil, errors.New("test error 500"), true)                                 // 500 StatusCode Retry
	performHandlerConsumeClaimTest(t, nil, errors.New("test error without status code"), true)                 // No StatusCode Retry
	performHandlerConsumeClaimTest(t, nil, errors.New("test error 300 with multiple status codes 200"), false) // Multiple StatusCode Retry
	performHandlerConsumeClaimTest(t, testDeadLetterURI, errors.New("test error 400"), true)                   // DeadLetterQueue Success After Retry
}

// Test One Permutation Of The Handler's ConsumeClaim() Functionality
func performHandlerConsumeClaimTest(t *testing.T, deadLetterURL *apis.URL, err error, retryExpected bool) {

	// Create The ConsumerMessage To Test
	consumerMessage := createConsumerMessage(t)

	// Setup Any Error Responses
	errorResponses := make(map[url.URL]error)
	if err != nil {
		errorResponses[*testSubscriberURL] = err
	}

	// Create Mocks For Testing
	mockConsumerGroupSession := dispatchertesting.NewMockConsumerGroupSession(t)
	mockConsumerGroupClaim := dispatchertesting.NewMockConsumerGroupClaim(t)
	mockMessageDispatcher := dispatchertesting.NewMockMessageDispatcher(t, errorResponses)

	// Mock The newMessageDispatcherWrapper Function (And Restore Post-Test)
	newMessageDispatcherWrapperPlaceholder := newMessageDispatcherWrapper
	newMessageDispatcherWrapper = func(logger *zap.Logger) channel.MessageDispatcher {
		return mockMessageDispatcher
	}
	defer func() { newMessageDispatcherWrapper = newMessageDispatcherWrapperPlaceholder }()

	// Create The Handler To Test
	handler := createTestHandler(t, testSubscriberURI, deadLetterURL)

	// Background Start Consuming Claims
	go func() {
		err := handler.ConsumeClaim(mockConsumerGroupSession, mockConsumerGroupClaim)
		assert.Nil(t, err)
	}()

	// Perform The Test (Add ConsumerMessages To Claims)
	mockConsumerGroupClaim.MessageChan <- consumerMessage

	// Wait For Message To Be Marked As Complete
	markedMessage := <-mockConsumerGroupSession.MarkMessageChan

	// Close The Mock ConsumerGroupClaim Message Channel To Complete/Exit Handler's ConsumeClaim()
	close(mockConsumerGroupClaim.MessageChan)

	// Verify The Results (CloudEvent Was Dispatched & ConsumerMessage Was Marked)
	assert.Equal(t, consumerMessage, markedMessage)
	if deadLetterURL != nil {
		assert.Len(t, mockMessageDispatcher.DispatchedMessages, 2)
	} else {
		assert.Len(t, mockMessageDispatcher.DispatchedMessages, 1)
	}

	// Verify Normal Message Processing (Including Retry)
	dispatchedMessages := mockMessageDispatcher.DispatchedMessages[*testSubscriberURL]
	if retryExpected {
		assert.Len(t, dispatchedMessages, 4)
	} else {
		assert.Len(t, dispatchedMessages, 1)
	}
	for _, dispatchedMessage := range dispatchedMessages {
		verifyDispatchedMessage(t, dispatchedMessage)
	}

	// Verify Any DeadLetter Processing
	if deadLetterURL != nil {
		deadLetterMessages := mockMessageDispatcher.DispatchedMessages[*deadLetterURL.URL()]
		assert.Len(t, deadLetterMessages, 1)
		verifyDispatchedMessage(t, deadLetterMessages[0])
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

// Utility Function For Creating New Handler
func createTestHandler(t *testing.T, subscriberURL *apis.URL, deadLetterURL *apis.URL) *Handler {

	// Test Data
	logger := logtesting.TestLogger(t).Desugar()
	testSubscriber := &eventingduck.SubscriberSpec{
		UID:           testSubscriberUID,
		Generation:    0,
		SubscriberURI: subscriberURL,
		ReplyURI:      nil,
		Delivery: &eventingduck.DeliverySpec{
			DeadLetterSink: &duckv1.Destination{
				URI: deadLetterURL,
			},
		},
	}

	// Perform The Test Create The Test Handler
	handler := NewHandler(logger, testSubscriber, testExponentialBackoff, testInitialRetryInterval, testMaxRetryTime)

	// Verify The Results
	assert.NotNil(t, handler)
	assert.Equal(t, logger, handler.Logger)
	assert.Equal(t, testSubscriber, handler.Subscriber)
	assert.NotNil(t, handler.MessageDispatcher)
	assert.Equal(t, true, handler.ExponentialBackoff)
	assert.Equal(t, testInitialRetryInterval, handler.InitialRetryInterval)
	assert.Equal(t, testMaxRetryTime, handler.MaxRetryTime)

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
