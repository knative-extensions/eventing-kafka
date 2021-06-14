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

package testing

import (
	"context"
	"net/http"
	"net/url"
	"sync"
	"testing"

	"github.com/Shopify/sarama"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/binding"
	"github.com/stretchr/testify/assert"
	"knative.dev/eventing/pkg/channel"
	"knative.dev/eventing/pkg/kncloudevents"
)

//
// Mock MessageDispatcher Implementation
//

// Verify The Mock MessageDispatcher Implements The Interface
var _ channel.MessageDispatcher = &MockMessageDispatcher{}

// Define The Mock MessageDispatcher
type MockMessageDispatcher struct {
	t                      *testing.T
	expectedHeaders        http.Header
	expectedDestinationUrl *url.URL
	expectedReplyUrl       *url.URL
	expectedDeadLetterUrl  *url.URL
	expectedRetryConfig    *kncloudevents.RetryConfig
	message                cloudevents.Message
	response               error
}

// Mock MessageDispatcher Constructor
func NewMockMessageDispatcher(t *testing.T,
	headers http.Header,
	destinationUrl *url.URL,
	replyUrl *url.URL,
	deadLetterUrl *url.URL,
	retryConfig *kncloudevents.RetryConfig,
	response error) *MockMessageDispatcher {

	// Create & Return The Specified Mock MessageDispatcher Instance
	return &MockMessageDispatcher{
		t:                      t,
		expectedHeaders:        headers,
		expectedDestinationUrl: destinationUrl,
		expectedReplyUrl:       replyUrl,
		expectedDeadLetterUrl:  deadLetterUrl,
		expectedRetryConfig:    retryConfig,
		response:               response,
	}
}

func (m *MockMessageDispatcher) DispatchMessage(ctx context.Context, message cloudevents.Message, additionalHeaders http.Header, destination *url.URL, reply *url.URL, deadLetter *url.URL) (*channel.DispatchExecutionInfo, error) {
	panic("implement me")
}

func (m *MockMessageDispatcher) DispatchMessageWithRetries(ctx context.Context, message cloudevents.Message, headers http.Header, destinationUrl *url.URL, replyUrl *url.URL, deadLetterUrl *url.URL, retryConfig *kncloudevents.RetryConfig, transformers ...binding.Transformer) (*channel.DispatchExecutionInfo, error) {

	// Validate The Expected Args
	assert.NotNil(m.t, ctx)
	assert.NotNil(m.t, message)
	assert.Equal(m.t, m.expectedHeaders, headers)
	assert.Equal(m.t, m.expectedDestinationUrl, destinationUrl)
	assert.Equal(m.t, m.expectedReplyUrl, replyUrl)
	assert.Equal(m.t, m.expectedDeadLetterUrl, deadLetterUrl)
	assert.Equal(m.t, m.expectedRetryConfig.RetryMax, retryConfig.RetryMax)

	// Track The Received Message
	m.message = message

	// Return The Desired Error Response
	return &channel.DispatchExecutionInfo{}, m.response
}

func (m *MockMessageDispatcher) Message() cloudevents.Message {
	return m.message
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

	// These fields must be pointers because the ConsumeClaim function copies the ConsumerGroupSession struct
	// and we need the values to be preserved across the threads in order to test them.
	markMessageCalled *bool
	markMessageMutex  *sync.Mutex // Prevent data race while reading MarkMessageCalled
}

// Mock ConsumerGroupSession Constructor
func NewMockConsumerGroupSession(t *testing.T) MockConsumerGroupSession {
	return MockConsumerGroupSession{
		t:                 t,
		MarkMessageChan:   make(chan *sarama.ConsumerMessage),
		markMessageCalled: new(bool),
		markMessageMutex:  new(sync.Mutex),
	}
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
	m.markMessageMutex.Lock()
	*m.markMessageCalled = true
	m.markMessageMutex.Unlock()
	m.MarkMessageChan <- msg
	assert.Empty(m.t, metadata)
}

func (m MockConsumerGroupSession) MarkMessageCalled() bool {
	m.markMessageMutex.Lock()
	defer m.markMessageMutex.Unlock()
	return *m.markMessageCalled
}

func (m MockConsumerGroupSession) Context() context.Context {
	return context.TODO()
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
