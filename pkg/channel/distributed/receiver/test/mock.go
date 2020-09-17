package test

import (
	"errors"
	"github.com/Shopify/sarama"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	kafkav1beta1 "knative.dev/eventing-contrib/kafka/channel/pkg/apis/messaging/v1beta1"
	kafkalisters "knative.dev/eventing-contrib/kafka/channel/pkg/client/listers/messaging/v1beta1"
)

//
// Mock Kafka Producer
//

var _ sarama.SyncProducer = &MockSyncProducer{}

type MockSyncProducer struct {
	producerMessages chan sarama.ProducerMessage
	offset           int64
	closed           bool
}

func NewMockSyncProducer() *MockSyncProducer {
	return &MockSyncProducer{
		producerMessages: make(chan sarama.ProducerMessage, 1),
		closed:           false,
	}
}

func (p *MockSyncProducer) SendMessage(msg *sarama.ProducerMessage) (partition int32, offset int64, err error) {
	p.producerMessages <- *msg
	p.offset = p.offset + 1
	return 1, p.offset, nil
}

func (p *MockSyncProducer) SendMessages(_ []*sarama.ProducerMessage) error {
	// Not Currently In Use - No Need To Mock
	return nil
}

func (p *MockSyncProducer) GetMessage() sarama.ProducerMessage {
	return <-p.producerMessages
}

func (p *MockSyncProducer) Close() error {
	p.closed = true
	close(p.producerMessages)
	return nil
}

func (p *MockSyncProducer) Closed() bool {
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

func (m MockKafkaChannelLister) List(_ labels.Selector) (ret []*kafkav1beta1.KafkaChannel, err error) {
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

func (m MockKafkaChannelNamespaceLister) List(_ labels.Selector) (ret []*kafkav1beta1.KafkaChannel, err error) {
	panic("implement me")
}

func (m MockKafkaChannelNamespaceLister) Get(name string) (*kafkav1beta1.KafkaChannel, error) {
	if m.err {
		return nil, k8serrors.NewInternalError(errors.New("expected Unit Test error from MockKafkaChannelNamespaceLister"))
	} else if m.exists {
		return CreateKafkaChannel(m.name, m.namespace, m.ready), nil
	} else {
		return nil, k8serrors.NewNotFound(kafkav1beta1.Resource("KafkaChannel"), name)
	}
}
