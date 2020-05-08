package controller

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/scheme"
	clientgotesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/record"
	"knative.dev/eventing-contrib/kafka/channel/pkg/apis/messaging/v1alpha1"
	"knative.dev/eventing-contrib/kafka/channel/pkg/client/clientset/versioned"
	kafkaconsumer "knative.dev/eventing-kafka/pkg/common/kafka/consumer"
	"knative.dev/eventing-kafka/pkg/dispatcher/dispatcher"
	dispatchertesting "knative.dev/eventing-kafka/pkg/dispatcher/testing"
	reconciletesting "knative.dev/eventing-kafka/pkg/dispatcher/testing"
	"knative.dev/pkg/controller"
	logtesting "knative.dev/pkg/logging/testing"
	. "knative.dev/pkg/reconciler/testing"
	reconcilertesting "knative.dev/pkg/reconciler/testing"
	"sync"
	"testing"
	"time"
)

const (
	testNS = "test-namespace"
	kcName = "test-kc"

	testBrokers                 = "TestBrokers"
	testTopic                   = "TestTopic"
	testOffset                  = "latest"
	testPollTimeoutMillis       = 500 // Not Used By Mock Consumer ; )
	testOffsetCommitCount       = 2
	testOffsetCommitDuration    = 50 * time.Millisecond // Small Durations For Testing!
	testOffsetCommitDurationMin = 50 * time.Millisecond // Small Durations For Testing!
	testUsername                = "TestUsername"
	testPassword                = "TestPassword"
	testExponentialBackoff      = false
	testInitialRetryInterval    = 500
	testMaxRetryTime            = 5000
)

func init() {
	// Add types to scheme
	_ = v1alpha1.AddToScheme(scheme.Scheme)
}

func TestAllCases(t *testing.T) {
	kcKey := testNS + "/" + kcName

	table := reconcilertesting.TableTest{
		{
			Name: "bad workqueue key",
			// Make sure Reconcile handles bad keys.
			Key: "too/many/parts",
		},
		{
			Name: "key not found",
			// Make sure Reconcile handles good keys that don't exist.
			Key: "foo/not-found",
		},
		{
			Name: "not our channel, so should be ignored",
			Key:  "foo/bar",
			Objects: []runtime.Object{
				reconciletesting.NewKafkaChannel("bar", "foo", reconciletesting.WithInitKafkaChannelConditions),
			},
			WantErr: false,
		},
		{
			Name: "channel not ready, should error out",
			Objects: []runtime.Object{
				reconciletesting.NewKafkaChannel(kcName, testNS, reconciletesting.WithInitKafkaChannelConditions),
			},
			Key:     kcKey,
			WantErr: true,
		},
		{
			Name: "channel ready, add subscriber",
			Objects: []runtime.Object{
				reconciletesting.NewKafkaChannel(kcName, testNS,
					reconciletesting.WithInitKafkaChannelConditions,
					reconciletesting.WithKafkaChannelAddress("http://foobar"),
					reconciletesting.WithKafkaChannelReady,
					reconciletesting.WithSubscriber("1", "http://foobar")),
			},
			Key:     kcKey,
			WantErr: false,
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{{
				Object: reconciletesting.NewKafkaChannel(kcName, testNS,
					reconciletesting.WithInitKafkaChannelConditions,
					reconciletesting.WithKafkaChannelReady,
					reconciletesting.WithKafkaChannelAddress("http://foobar"),
					reconciletesting.WithSubscriber("1", "http://foobar"),
					reconciletesting.WithSubscriberReady("1"),
				),
			}},
			WantEvents: []string{
				Eventf(corev1.EventTypeNormal, channelReconciled, "KafkaChannel Reconciled"),
			},
		},
		{
			Name: "channel ready, 1 subscriber ready, add a 2nd one",
			Objects: []runtime.Object{
				reconciletesting.NewKafkaChannel(kcName, testNS,
					reconciletesting.WithInitKafkaChannelConditions,
					reconciletesting.WithKafkaChannelAddress("http://channel"),
					reconciletesting.WithKafkaChannelReady,
					reconciletesting.WithSubscriber("1", "http://foobar"),
					reconciletesting.WithSubscriber("2", "http://foobar2"),
					reconciletesting.WithSubscriberReady("1")),
			},
			Key:     kcKey,
			WantErr: false,
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{{
				Object: reconciletesting.NewKafkaChannel(kcName, testNS,
					reconciletesting.WithInitKafkaChannelConditions,
					reconciletesting.WithKafkaChannelReady,
					reconciletesting.WithKafkaChannelAddress("http://channel"),
					reconciletesting.WithSubscriber("1", "http://foobar"),
					reconciletesting.WithSubscriber("2", "http://foobar2"),
					reconciletesting.WithSubscriberReady("1"),
					reconciletesting.WithSubscriberReady("2"),
				),
			}},
			WantEvents: []string{
				Eventf(corev1.EventTypeNormal, channelReconciled, "KafkaChannel Reconciled"),
			},
		},
	}

	table.Test(t, reconciletesting.MakeFactory(func(listers *dispatchertesting.Listers, kafkaClient versioned.Interface, eventRecorder record.EventRecorder) controller.Reconciler {
		return &Reconciler{
			Logger:               logtesting.TestLogger(t).Desugar(),
			kafkachannelInformer: nil,
			kafkachannelLister:   listers.GetKafkaChannelLister(),
			dispatcher:           NewTestDispatcher(t, kcKey),
			Recorder:             eventRecorder,
			KafkaClientSet:       kafkaClient,
		}
	}))

	// Pause to let async go processes finish logging :(
	time.Sleep(1 * time.Second)
}

func NewTestDispatcher(t *testing.T, channelKey string) *dispatcher.Dispatcher {

	// Create A Test Logger
	logger := logtesting.TestLogger(t).Desugar()

	// Replace The NewClient Wrapper To Provide Mock Consumer & Defer Reset
	newConsumerWrapperPlaceholder := kafkaconsumer.NewConsumerWrapper
	kafkaconsumer.NewConsumerWrapper = func(configMap *kafka.ConfigMap) (kafkaconsumer.ConsumerInterface, error) {
		return NewMockConsumer(), nil
	}
	defer func() { kafkaconsumer.NewConsumerWrapper = newConsumerWrapperPlaceholder }()

	// Create A New Dispatcher
	dispatcherConfig := dispatcher.DispatcherConfig{
		Logger:                      logger,
		Brokers:                     testBrokers,
		Topic:                       testTopic,
		Offset:                      testOffset,
		PollTimeoutMillis:           testPollTimeoutMillis,
		OffsetCommitCount:           testOffsetCommitCount,
		OffsetCommitDuration:        testOffsetCommitDuration,
		OffsetCommitDurationMinimum: testOffsetCommitDurationMin,
		Username:                    testUsername,
		Password:                    testPassword,
		ChannelKey:                  channelKey,
		ExponentialBackoff:          testExponentialBackoff,
		InitialRetryInterval:        testInitialRetryInterval,
		MaxRetryTime:                testMaxRetryTime,
	}
	testDispatcher := dispatcher.NewDispatcher(dispatcherConfig)
	return testDispatcher
}

var _ kafkaconsumer.ConsumerInterface = &MockConsumer{}

func NewMockConsumer() *MockConsumer {
	return &MockConsumer{
		events:       make(chan kafka.Event),
		commits:      make(map[int32]kafka.Offset),
		offsets:      make(map[int32]kafka.Offset),
		offsetsMutex: &sync.Mutex{},
		closed:       false,
	}
}

type MockConsumer struct {
	events             chan kafka.Event
	commits            map[int32]kafka.Offset
	offsets            map[int32]kafka.Offset
	offsetsMutex       *sync.Mutex
	eventsChanEnable   bool
	readerTermChan     chan bool
	rebalanceCb        kafka.RebalanceCb
	appReassigned      bool
	appRebalanceEnable bool // config setting
	closed             bool
}

func (mc *MockConsumer) OffsetsForTimes(times []kafka.TopicPartition, timeoutMs int) (offsets []kafka.TopicPartition, err error) {
	return nil, nil
}

func (mc *MockConsumer) GetMetadata(topic *string, allTopics bool, timeoutMs int) (*kafka.Metadata, error) {
	return nil, nil
}

func (mc *MockConsumer) CommitOffsets(offsets []kafka.TopicPartition) ([]kafka.TopicPartition, error) {
	return nil, nil
}

func (mc *MockConsumer) QueryWatermarkOffsets(topic string, partition int32, timeoutMs int) (low, high int64, err error) {
	return 0, 0, nil
}

func (mc *MockConsumer) Poll(timeout int) kafka.Event {
	// Non-Blocking Event Forwarding (Timeout Ignored)
	select {
	case event := <-mc.events:
		return event
	default:
		return nil
	}
}

func (mc *MockConsumer) CommitMessage(message *kafka.Message) ([]kafka.TopicPartition, error) {
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

func (mc *MockConsumer) getCommits() map[int32]kafka.Offset {
	return mc.commits
}

func (mc *MockConsumer) Assignment() (partitions []kafka.TopicPartition, err error) {
	return nil, nil
}

func (mc *MockConsumer) Commit() ([]kafka.TopicPartition, error) {
	mc.offsetsMutex.Lock()
	for partition, offset := range mc.offsets {
		mc.commits[partition] = offset
	}
	mc.offsetsMutex.Unlock()
	return nil, nil
}

func (mc *MockConsumer) StoreOffsets(offsets []kafka.TopicPartition) (storedOffsets []kafka.TopicPartition, err error) {
	mc.offsetsMutex.Lock()
	for _, partition := range offsets {
		mc.offsets[partition.Partition] = partition.Offset
	}
	mc.offsetsMutex.Unlock()
	return nil, nil
}
