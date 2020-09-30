package testing

import (
	"context"
	"net/http"
	"net/url"
	"testing"

	"github.com/Shopify/sarama"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/stretchr/testify/assert"
	"knative.dev/eventing/pkg/channel"
	"knative.dev/eventing/pkg/kncloudevents"
)

//
// Mock ConsumerGroupClaim Implementation
//

// Verify The Mock MessageDispatcher Implements The Interface
var _ channel.MessageDispatcher = &MockMessageDispatcher{}

// Define The Mock MessageDispatcher
type MockMessageDispatcher struct {
	t                  *testing.T
	errorResponses     map[url.URL]error
	DispatchedMessages map[url.URL][]cloudevents.Message
}

// Mock MessageDispatcher Constructor
func NewMockMessageDispatcher(t *testing.T, errorResponses map[url.URL]error) MockMessageDispatcher {
	if errorResponses == nil {
		errorResponses = make(map[url.URL]error)
	}
	return MockMessageDispatcher{
		t:                  t,
		errorResponses:     errorResponses,
		DispatchedMessages: make(map[url.URL][]cloudevents.Message),
	}
}

func (m MockMessageDispatcher) DispatchMessage(ctx context.Context, message cloudevents.Message, additionalHeaders http.Header, destination *url.URL, reply *url.URL, deadLetter *url.URL) error {
	return m.DispatchMessageWithRetries(ctx, message, additionalHeaders, destination, reply, deadLetter, nil)
}

func (m MockMessageDispatcher) DispatchMessageWithRetries(ctx context.Context, message cloudevents.Message, additionalHeaders http.Header, destination *url.URL, reply *url.URL, deadLetter *url.URL, config *kncloudevents.RetryConfig) error {
	assert.NotNil(m.t, ctx)
	assert.NotNil(m.t, message)
	assert.NotNil(m.t, destination)
	// TODO assert.NotNil(m.t, config)

	if deadLetter != nil {
		m.trackMessage(deadLetter, message)
		return m.errorResponses[*deadLetter]
	} else {
		m.trackMessage(destination, message)
		return m.errorResponses[*destination]
	}
}

// Utility Function For Tracking Messages Against Their URLs
func (m MockMessageDispatcher) trackMessage(url *url.URL, message cloudevents.Message) {
	messages := m.DispatchedMessages[*url]
	if messages == nil {
		messages = make([]cloudevents.Message, 0)
	}
	m.DispatchedMessages[*url] = append(messages, message)
}

//
// Mock ConsumerGroupSession Implementation
//

// Verify The Mock ConsumerGroupSession Implements The Interface
var _ sarama.ConsumerGroupSession = &MockConsumerGroupSession{}

// Define The Mock ConsumerGroupSession
type MockConsumerGroupSession struct {
	t               *testing.T
	MarkMessageChan chan *sarama.ConsumerMessage
}

// Mock ConsumerGroupSession Constructor
func NewMockConsumerGroupSession(t *testing.T) MockConsumerGroupSession {
	return MockConsumerGroupSession{t: t, MarkMessageChan: make(chan *sarama.ConsumerMessage)}
}

func (m MockConsumerGroupSession) Claims() map[string][]int32 {
	panic("implement me")
}

func (m MockConsumerGroupSession) MemberID() string {
	panic("implement me")
}

func (m MockConsumerGroupSession) GenerationID() int32 {
	panic("implement me")
}

func (m MockConsumerGroupSession) MarkOffset(topic string, partition int32, offset int64, metadata string) {
	panic("implement me")
}

func (m MockConsumerGroupSession) ResetOffset(topic string, partition int32, offset int64, metadata string) {
	panic("implement me")
}

func (m MockConsumerGroupSession) MarkMessage(msg *sarama.ConsumerMessage, metadata string) {
	m.MarkMessageChan <- msg
	assert.Empty(m.t, metadata)
}

func (m MockConsumerGroupSession) Context() context.Context {
	panic("implement me")
}

func (m MockConsumerGroupSession) Commit() {
	panic("implement me")
}

//
// Mock ConsumerGroupClaim Implementation
//

// Verify The Mock ConsumerGroupClaim Implements The Interface
var _ sarama.ConsumerGroupClaim = &MockConsumerGroupClaim{}

// Define The Mock ConsumerGroupSession
type MockConsumerGroupClaim struct {
	t           *testing.T
	MessageChan chan *sarama.ConsumerMessage
}

// Mock ConsumerGroupClaim Constructor
func NewMockConsumerGroupClaim(t *testing.T) MockConsumerGroupClaim {
	return MockConsumerGroupClaim{t: t, MessageChan: make(chan *sarama.ConsumerMessage)}
}

func (m MockConsumerGroupClaim) Topic() string {
	panic("implement me")
}

func (m MockConsumerGroupClaim) Partition() int32 {
	panic("implement me")
}

func (m MockConsumerGroupClaim) InitialOffset() int64 {
	panic("implement me")
}

func (m MockConsumerGroupClaim) HighWaterMarkOffset() int64 {
	panic("implement me")
}

func (m MockConsumerGroupClaim) Messages() <-chan *sarama.ConsumerMessage {
	return m.MessageChan
}
