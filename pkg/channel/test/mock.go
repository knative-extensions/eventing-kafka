package test

import (
	"errors"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	kafkav1alpha1 "knative.dev/eventing-contrib/kafka/channel/pkg/apis/messaging/v1alpha1"
	kafkalisters "knative.dev/eventing-contrib/kafka/channel/pkg/client/listers/messaging/v1alpha1"
	kafkaproducer "knative.dev/eventing-kafka/pkg/common/kafka/producer"
)

//
// Mock Confluent Producer
//

var _ kafkaproducer.ProducerInterface = &MockProducer{}

type MockProducer struct {
	config              *kafka.ConfigMap
	produce             chan *kafka.Message
	events              chan kafka.Event
	testResponseMessage *kafka.Message
	closed              bool
}

func NewMockProducer(topicName string) *MockProducer {

	testResponseMessage := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topicName,
			Partition: 1,
			Offset:    -1,
			Error:     nil,
		},
	}

	return &MockProducer{
		config:              nil,
		produce:             make(chan *kafka.Message, 1),
		events:              make(chan kafka.Event, 1),
		testResponseMessage: testResponseMessage,
		closed:              false,
	}
}

// String returns a human readable name for a Producer instance
func (p *MockProducer) String() string {
	return ""
}

func (p *MockProducer) Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error {
	p.produce <- msg

	// Write back to the deliveryChan has to be done in a separate goroutine
	go func() {
		deliveryChan <- p.testResponseMessage
	}()

	return nil
}

func (p *MockProducer) Events() chan kafka.Event {
	return p.events
}

func (p *MockProducer) ProduceChannel() chan *kafka.Message {
	return p.produce
}

func (p *MockProducer) Len() int {
	return 0
}

func (p *MockProducer) Flush(timeoutMs int) int {
	return 0
}

func (p *MockProducer) Close() {
	close(p.events)
	close(p.produce)
	p.closed = true
}

func (p *MockProducer) Closed() bool {
	return p.closed
}

//
// Mock KafkaChannel Lister
//

var _ kafkalisters.KafkaChannelLister = &MockKafkaChannelLister{}

type MockKafkaChannelLister struct {
	name      string
	namespace string
	exists    bool
	ready     corev1.ConditionStatus
	err       bool
}

func NewMockKafkaChannelLister(name string, namespace string, exists bool, ready corev1.ConditionStatus, err bool) MockKafkaChannelLister {
	return MockKafkaChannelLister{
		name:      name,
		namespace: namespace,
		exists:    exists,
		ready:     ready,
		err:       err,
	}
}

func (m MockKafkaChannelLister) List(selector labels.Selector) (ret []*kafkav1alpha1.KafkaChannel, err error) {
	panic("implement me")
}

func (m MockKafkaChannelLister) KafkaChannels(namespace string) kafkalisters.KafkaChannelNamespaceLister {
	return NewMockKafkaChannelNamespaceLister(m.name, namespace, m.exists, m.ready, m.err)
}

//
// Mock KafkaChannel NamespaceLister
//

var _ kafkalisters.KafkaChannelNamespaceLister = &MockKafkaChannelNamespaceLister{}

type MockKafkaChannelNamespaceLister struct {
	name      string
	namespace string
	exists    bool
	ready     corev1.ConditionStatus
	err       bool
}

func NewMockKafkaChannelNamespaceLister(name string, namespace string, exists bool, ready corev1.ConditionStatus, err bool) MockKafkaChannelNamespaceLister {
	return MockKafkaChannelNamespaceLister{
		name:      name,
		namespace: namespace,
		exists:    exists,
		ready:     ready,
		err:       err,
	}
}

func (m MockKafkaChannelNamespaceLister) List(selector labels.Selector) (ret []*kafkav1alpha1.KafkaChannel, err error) {
	panic("implement me")
}

func (m MockKafkaChannelNamespaceLister) Get(name string) (*kafkav1alpha1.KafkaChannel, error) {
	if m.err {
		return nil, k8serrors.NewInternalError(errors.New("expected Unit Test error from MockKafkaChannelNamespaceLister"))
	} else if m.exists {
		return CreateKafkaChannel(m.name, m.namespace, m.ready), nil
	} else {
		return nil, k8serrors.NewNotFound(kafkav1alpha1.Resource("KafkaChannel"), name)
	}
}
