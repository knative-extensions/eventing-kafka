package test

import (
	"context"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	kafkaadmin "github.com/kyma-incubator/knative-kafka/pkg/common/kafka/admin"
	kafkaconsumer "github.com/kyma-incubator/knative-kafka/pkg/common/kafka/consumer"
)

//
// Mock Confluent AdminClient
//

// Verify The Mock AdminClient Implements The KafkaAdminClient Interface
var _ kafkaadmin.AdminClientInterface = &MockAdminClient{}

// Mock Kafka AdminClient Implementation
type MockAdminClient struct {
	createTopicsCalled  bool
	deleteTopicsCalled  bool
	MockCreateTopicFunc func(context.Context, []kafka.TopicSpecification, ...kafka.CreateTopicsAdminOption) ([]kafka.TopicResult, error)
	MockDeleteTopicFunc func(context.Context, []string, ...kafka.DeleteTopicsAdminOption) ([]kafka.TopicResult, error)
}

// Mock Kafka AdminClient CreateTopics Function - Calls Custom CreateTopics If Specified, Otherwise Returns Success
func (m *MockAdminClient) CreateTopics(ctx context.Context, topics []kafka.TopicSpecification, options ...kafka.CreateTopicsAdminOption) (result []kafka.TopicResult, err error) {
	m.createTopicsCalled = true
	if m.MockCreateTopicFunc != nil {
		return m.MockCreateTopicFunc(ctx, topics, options...)
	}
	return []kafka.TopicResult{}, nil
}

// Check On Calls To CreateTopics()
func (m *MockAdminClient) CreateTopicsCalled() bool {
	return m.createTopicsCalled
}

// Mock Kafka AdminClient DeleteTopics Function - Calls Custom DeleteTopics If Specified, Otherwise Returns Success
func (m *MockAdminClient) DeleteTopics(ctx context.Context, topics []string, options ...kafka.DeleteTopicsAdminOption) (result []kafka.TopicResult, err error) {
	m.deleteTopicsCalled = true
	if m.MockDeleteTopicFunc != nil {
		return m.MockDeleteTopicFunc(ctx, topics, options...)
	}
	return []kafka.TopicResult{}, nil
}

// Check On Calls To DeleteTopics()
func (m *MockAdminClient) DeleteTopicsCalled() bool {
	return m.deleteTopicsCalled
}

// Mock Kafka AdminClient Close Function - NoOp
func (m *MockAdminClient) Close() {
	return
}

// Mock Kafka Secret Name Function - Return Test Data
func (m *MockAdminClient) GetKafkaSecretName(topicName string) string {
	return KafkaSecretName
}

//
// Mock ConsumerInterface Implementation
//

var _ kafkaconsumer.ConsumerInterface = &MockConsumer{}

type MockConsumer struct {
	events             chan kafka.Event
	eventsChanEnable   bool
	readerTermChan     chan bool
	rebalanceCb        kafka.RebalanceCb
	appReassigned      bool
	appRebalanceEnable bool // config setting
	closed             bool
}

func (mc *MockConsumer) StoreOffsets(offsets []kafka.TopicPartition) (storedOffsets []kafka.TopicPartition, err error) {
	return nil, nil
}

func (mc *MockConsumer) Commit() ([]kafka.TopicPartition, error) {
	return nil, nil
}

func (mc *MockConsumer) QueryWatermarkOffsets(topic string, partition int32, timeoutMs int) (low, high int64, err error) {
	return 0, 0, nil
}

func (mc *MockConsumer) OffsetsForTimes(times []kafka.TopicPartition, timeoutMs int) (offsets []kafka.TopicPartition, err error) {
	return nil, nil
}

func (mc *MockConsumer) GetMetadata(topic *string, allTopics bool, timeoutMs int) (*kafka.Metadata, error) {
	metadata := kafka.Metadata{
		Topics: map[string]kafka.TopicMetadata{
			TopicName: {
				Partitions: []kafka.PartitionMetadata{{}, {}},
			},
		},
	}
	return &metadata, nil
}

func (mc *MockConsumer) CommitOffsets(offsets []kafka.TopicPartition) ([]kafka.TopicPartition, error) {
	return nil, nil
}

func (mc *MockConsumer) Poll(timeout int) kafka.Event {
	return <-mc.events
}

func (mc *MockConsumer) CommitMessage(*kafka.Message) ([]kafka.TopicPartition, error) {
	return nil, nil
}

func (mc *MockConsumer) Close() error {
	mc.closed = true
	close(mc.events)
	return nil
}

func (mc *MockConsumer) Subscribe(topic string, rebalanceCb kafka.RebalanceCb) error {
	return nil
}

func (mc *MockConsumer) sendMessage(message kafka.Event) {
	mc.events <- message
}
