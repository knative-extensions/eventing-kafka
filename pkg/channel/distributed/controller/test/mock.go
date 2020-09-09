package test

import (
	"context"
	"github.com/Shopify/sarama"
	kafkaadmin "knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/admin"
)

//
// Mock Kafka AdminClient
//

// Verify The Mock AdminClient Implements The KafkaAdminClient Interface
var _ kafkaadmin.AdminClientInterface = &MockAdminClient{}

// Mock Kafka AdminClient Implementation
type MockAdminClient struct {
	closeCalled         bool
	createTopicsCalled  bool
	deleteTopicsCalled  bool
	MockCreateTopicFunc func(context.Context, string, *sarama.TopicDetail) *sarama.TopicError
	MockDeleteTopicFunc func(context.Context, string) *sarama.TopicError
}

// Mock Kafka AdminClient CreateTopic() Function - Calls Custom CreateTopic() If Specified, Otherwise Returns Success
func (m *MockAdminClient) CreateTopic(ctx context.Context, topicName string, topicDetail *sarama.TopicDetail) *sarama.TopicError {
	m.createTopicsCalled = true
	if m.MockCreateTopicFunc != nil {
		return m.MockCreateTopicFunc(ctx, topicName, topicDetail)
	}
	errMsg := "mock CreateTopic() success"
	return &sarama.TopicError{Err: sarama.ErrNoError, ErrMsg: &errMsg}
}

// Check On Calls To CreateTopics()
func (m *MockAdminClient) CreateTopicsCalled() bool {
	return m.createTopicsCalled
}

// Mock Kafka AdminClient DeleteTopic() Function - Calls Custom DeleteTopic() If Specified, Otherwise Returns Success
func (m *MockAdminClient) DeleteTopic(ctx context.Context, topicName string) *sarama.TopicError {
	m.deleteTopicsCalled = true
	if m.MockDeleteTopicFunc != nil {
		return m.MockDeleteTopicFunc(ctx, topicName)
	}
	errMsg := "mock DeleteTopic() success"
	return &sarama.TopicError{Err: sarama.ErrNoError, ErrMsg: &errMsg}
}

// Check On Calls To DeleteTopics()
func (m *MockAdminClient) DeleteTopicsCalled() bool {
	return m.deleteTopicsCalled
}

// Mock Kafka AdminClient Close Function - NoOp
func (m *MockAdminClient) Close() error {
	m.closeCalled = true
	return nil
}

// Check On Calls To Close()
func (m *MockAdminClient) CloseCalled() bool {
	return m.closeCalled
}

// Mock Kafka Secret Name Function - Return Test Data
func (m *MockAdminClient) GetKafkaSecretName(_ string) string {
	return KafkaSecretName
}
