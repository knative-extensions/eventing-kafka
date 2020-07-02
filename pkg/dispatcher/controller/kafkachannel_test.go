package controller

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/scheme"
	clientgotesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/record"
	"knative.dev/eventing-contrib/kafka/channel/pkg/apis/messaging/v1alpha1"
	"knative.dev/eventing-contrib/kafka/channel/pkg/client/clientset/versioned"
	"knative.dev/eventing-kafka/pkg/dispatcher/dispatcher"
	dispatchertesting "knative.dev/eventing-kafka/pkg/dispatcher/testing"
	reconciletesting "knative.dev/eventing-kafka/pkg/dispatcher/testing"
	eventingduck "knative.dev/eventing/pkg/apis/duck/v1alpha1"
	"knative.dev/pkg/controller"
	logtesting "knative.dev/pkg/logging/testing"
	. "knative.dev/pkg/reconciler/testing"
	reconcilertesting "knative.dev/pkg/reconciler/testing"
	"testing"
	"time"
)

const (
	testNS = "test-namespace"
	kcName = "test-kc"
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
			logger:               logtesting.TestLogger(t).Desugar(),
			channelKey:           kcKey,
			kafkachannelInformer: nil,
			kafkachannelLister:   listers.GetKafkaChannelLister(),
			dispatcher:           NewMockDispatcher(t),
			recorder:             eventRecorder,
			kafkaClientSet:       kafkaClient,
		}
	}))

	// Pause to let async go processes finish logging :(
	time.Sleep(1 * time.Second)
}

//
// Mock Dispatcher Implementation
//

// Verify The Mock MessageDispatcher Implements The Interface
var _ dispatcher.Dispatcher = &MockDispatcher{}

// Define The Mock Dispatcher
type MockDispatcher struct {
	t *testing.T
}

// Mock Dispatcher Constructor
func NewMockDispatcher(t *testing.T) MockDispatcher {
	return MockDispatcher{t: t}
}

func (m MockDispatcher) Shutdown() {
}

func (m MockDispatcher) UpdateSubscriptions(subscriberSpecs []eventingduck.SubscriberSpec) map[eventingduck.SubscriberSpec]error {
	return nil
}
