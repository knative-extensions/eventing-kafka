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

package controller

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	clientgotesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/record"
	eventingduck "knative.dev/eventing/pkg/apis/duck/v1"
	"knative.dev/pkg/controller"
	kncontroller "knative.dev/pkg/controller"
	logtesting "knative.dev/pkg/logging/testing"
	. "knative.dev/pkg/reconciler/testing"
	reconcilertesting "knative.dev/pkg/reconciler/testing"

	"knative.dev/eventing-kafka/pkg/apis/messaging/v1beta1"
	commonenv "knative.dev/eventing-kafka/pkg/channel/distributed/common/env"
	"knative.dev/eventing-kafka/pkg/channel/distributed/dispatcher/constants"
	"knative.dev/eventing-kafka/pkg/channel/distributed/dispatcher/dispatcher"
	reconciletesting "knative.dev/eventing-kafka/pkg/channel/distributed/dispatcher/testing"
	"knative.dev/eventing-kafka/pkg/client/clientset/versioned"
	fakeclientset "knative.dev/eventing-kafka/pkg/client/clientset/versioned/fake"
	"knative.dev/eventing-kafka/pkg/client/informers/externalversions"
	"knative.dev/eventing-kafka/pkg/common/consumer"
)

const (
	testNS = "test-namespace"
	kcName = "test-kc"
)

func init() {
	// Add types to scheme
	_ = v1beta1.AddToScheme(scheme.Scheme)
}

// Test The NewController() Functionality
func TestNewController(t *testing.T) {

	for _, testCase := range []struct {
		name          string
		managerEvents chan consumer.ManagerEvent
	}{
		{
			name:          "With manager events channel",
			managerEvents: make(chan consumer.ManagerEvent),
		},
		{
			name: "Nil manager events channel",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			// Test Data
			logger := logtesting.TestLogger(t).Desugar()
			channelKey := "TestChannelKey"
			mockDispatcher := &MockDispatcher{}
			fakeKafkaChannelClientSet := fakeclientset.NewSimpleClientset()
			fakeK8sClientSet := fake.NewSimpleClientset()
			populateEnvironmentVariables(t)
			kafkaInformerFactory := externalversions.NewSharedInformerFactory(fakeKafkaChannelClientSet, kncontroller.DefaultResyncPeriod)
			kafkaChannelInformer := kafkaInformerFactory.Messaging().V1beta1().KafkaChannels()
			stopChan := make(chan struct{})

			// Perform The Test
			c := NewController(context.TODO(), logger, channelKey, mockDispatcher, kafkaChannelInformer, fakeK8sClientSet, fakeKafkaChannelClientSet, stopChan, testCase.managerEvents)

			// Verify Results
			assert.NotNil(t, c)

			// Close The Channels
			close(stopChan)
			if testCase.managerEvents != nil {
				close(testCase.managerEvents)
			}

			// Let the channel loops finish
			time.Sleep(50 * time.Millisecond)

			mockDispatcher.AssertExpectations(t)
		})
	}
}

// Test the processManagerEvents function
func TestProcessManagerEvents(t *testing.T) {
	mockDispatcher := &MockDispatcher{}
	mockReconciler := &MockReconciler{}
	logger := logtesting.TestLogger(t).Desugar()
	fakeKafkaChannelClientSet := fakeclientset.NewSimpleClientset()
	populateEnvironmentVariables(t)
	kafkaInformerFactory := externalversions.NewSharedInformerFactory(fakeKafkaChannelClientSet, kncontroller.DefaultResyncPeriod)
	kafkaChannelInformer := kafkaInformerFactory.Messaging().V1beta1().KafkaChannels()
	reconciler := &Reconciler{
		logger:               logger,
		dispatcher:           mockDispatcher,
		kafkachannelInformer: kafkaChannelInformer.Informer(),
		kafkachannelLister:   kafkaChannelInformer.Lister(),
		kafkaClientSet:       fakeKafkaChannelClientSet,
		impl:                 controller.NewContext(context.TODO(), mockReconciler, controller.ControllerOptions{Logger: logger.Sugar()}),
	}
	events := make(chan consumer.ManagerEvent)

	assert.NotNil(t, reconciler.processManagerEvents(nil))
	reconciler.channelKey = "invalid/channel/key"
	assert.NotNil(t, reconciler.processManagerEvents(events))
	reconciler.channelKey = "test-namespace/test-name"
	assert.Nil(t, reconciler.processManagerEvents(events))

	// Send all of the supported event types to the events channel
	events <- consumer.ManagerEvent{Event: consumer.GroupCreated, GroupId: "test-group-id"}
	events <- consumer.ManagerEvent{Event: consumer.GroupStopped, GroupId: "test-group-id"}
	events <- consumer.ManagerEvent{Event: consumer.GroupStarted, GroupId: "test-group-id"}
	events <- consumer.ManagerEvent{Event: consumer.GroupClosed, GroupId: "test-group-id"}

	// Send an unexpected event type to the events channel
	events <- consumer.ManagerEvent{Event: consumer.EventIndex(9999), GroupId: "test-group-id"}

	// Close The Channels
	close(events)

	// Let the channel loops finish
	time.Sleep(50 * time.Millisecond)

	mockDispatcher.AssertExpectations(t)
	mockReconciler.AssertExpectations(t)
}

// Test KafkaChannel Controller Reconciliation
func TestAllCases(t *testing.T) {
	kcKey := testNS + "/" + kcName

	table := reconcilertesting.TableTest{
		{
			Name: "bad workqueue key", // Make sure Reconcile handles bad keys.
			Key:  "too/many/parts",
		},
		{
			Name: "key not found", // Make sure Reconcile handles good keys that don't exist.
			Key:  "foo/not-found",
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
			Name: "channel ready, no change",
			Objects: []runtime.Object{
				reconciletesting.NewKafkaChannel(kcName, testNS,
					reconciletesting.WithInitKafkaChannelConditions,
					reconciletesting.WithKafkaChannelAddress("http://foobar"),
					reconciletesting.WithKafkaChannelReady,
					reconciletesting.WithSubscriber("1", "http://foobar"),
					reconciletesting.WithSubscriberReady("1")),
			},
			Key:     kcKey,
			WantErr: false,
			WantEvents: []string{
				Eventf(corev1.EventTypeNormal, channelReconciled, "KafkaChannel Reconciled"),
			},
		}, {
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
		{
			Name: "channel ready, 1 subscriber ready, stopped, add 2nd one",
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
					reconciletesting.WithSubscriberNotReady("1", constants.GroupStoppedMessage),
					reconciletesting.WithSubscriberReady("2"),
				),
			}},
			WantEvents: []string{
				Eventf(corev1.EventTypeNormal, channelReconciled, "KafkaChannel Reconciled"),
			},
			OtherTestData: map[string]interface{}{
				"status": consumer.SubscriberStatusMap{types.UID("1"): consumer.SubscriberStatus{Stopped: true}},
			},
		},
		{
			Name: "channel ready, 1 subscriber ready, failed, add 2nd one",
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
			WantErr: true,
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{{
				Object: reconciletesting.NewKafkaChannel(kcName, testNS,
					reconciletesting.WithInitKafkaChannelConditions,
					reconciletesting.WithKafkaChannelReady,
					reconciletesting.WithKafkaChannelAddress("http://channel"),
					reconciletesting.WithSubscriber("1", "http://foobar"),
					reconciletesting.WithSubscriber("2", "http://foobar2"),
					reconciletesting.WithSubscriberNotReady("1", "test error"),
					reconciletesting.WithSubscriberReady("2"),
				),
			}},
			WantEvents: []string{
				Eventf(corev1.EventTypeWarning, channelReconcileFailed, "KafkaChannel Reconciliation Failed: some kafka subscribers failed to subscribe"),
			},
			OtherTestData: map[string]interface{}{
				"status": consumer.SubscriberStatusMap{types.UID("1"): consumer.SubscriberStatus{Error: fmt.Errorf("test error")}},
			},
		},
	}

	table.Test(t, reconciletesting.MakeFactory(func(listers *reconciletesting.Listers,
		kafkaClient versioned.Interface,
		eventRecorder record.EventRecorder,
		status consumer.SubscriberStatusMap,
	) controller.Reconciler {
		mockDispatcher := &MockDispatcher{}
		mockDispatcher.On("UpdateSubscriptions", mock.Anything, mock.Anything, mock.Anything).Return(status)
		return &Reconciler{
			logger:               logtesting.TestLogger(t).Desugar(),
			channelKey:           kcKey,
			kafkachannelInformer: nil,
			kafkachannelLister:   listers.GetKafkaChannelLister(),
			dispatcher:           mockDispatcher,
			recorder:             eventRecorder,
			kafkaClientSet:       kafkaClient,
		}
	}))

	// Pause to let async go processes finish logging :(
	time.Sleep(1 * time.Second)
}

// Utility Function For Populating Required Environment Variables For Testing
func populateEnvironmentVariables(t *testing.T) {
	// Most of these are not actually used, but they need to exist or the GetEnvironment call will fail
	assert.Nil(t, os.Setenv(commonenv.MetricsDomainEnvVarKey, "testMetricsDomain"))
	assert.Nil(t, os.Setenv(commonenv.MetricsPortEnvVarKey, "6789"))
	assert.Nil(t, os.Setenv(commonenv.ResyncPeriodMinutesEnvVarKey, "3600"))
	assert.Nil(t, os.Setenv(commonenv.PodNameEnvVarKey, "testPodName"))
	assert.Nil(t, os.Setenv(commonenv.ContainerNameEnvVarKey, "testContainerName"))
	assert.Nil(t, os.Setenv(commonenv.HealthPortEnvVarKey, "5678"))
	assert.Nil(t, os.Setenv(commonenv.KafkaSecretNameEnvVarKey, "testKafkaSecretName"))
	assert.Nil(t, os.Setenv(commonenv.KafkaSecretNamespaceEnvVarKey, "testKafkaSecretNamespace"))
	assert.Nil(t, os.Setenv(commonenv.KafkaTopicEnvVarKey, "testKafkaTopic"))
	assert.Nil(t, os.Setenv(commonenv.ChannelKeyEnvVarKey, "testChannelKey"))
	assert.Nil(t, os.Setenv(commonenv.ServiceNameEnvVarKey, "testServiceName"))
}

//
// Mock Dispatcher Implementation
//

// Verify The Mock MessageDispatcher Implements The Interface
var _ dispatcher.Dispatcher = &MockDispatcher{}

// Define The Mock Dispatcher
type MockDispatcher struct {
	mock.Mock
}

func (m *MockDispatcher) Shutdown() {
	m.Called()
}

func (m *MockDispatcher) UpdateSubscriptions(ctx context.Context, ref types.NamespacedName, subscriberSpecs []eventingduck.SubscriberSpec) consumer.SubscriberStatusMap {
	args := m.Called(ctx, ref, subscriberSpecs)
	return args.Get(0).(consumer.SubscriberStatusMap)
}

func (m *MockDispatcher) SecretChanged(ctx context.Context, secret *corev1.Secret) {
	m.Called(ctx, secret)
}

//
// Mock Reconciler Implementation
//

// Verify The Mock Reconciler Implements The Interface
var _ controller.Reconciler = &MockReconciler{}

// Define The Mock Dispatcher
type MockReconciler struct {
	mock.Mock
}

func (m *MockReconciler) Reconcile(ctx context.Context, key string) error {
	return m.Called(ctx, key).Error(0)
}
