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
	"os"
	"testing"
	"time"

	"knative.dev/eventing-kafka/pkg/common/consumer"

	commonenv "knative.dev/eventing-kafka/pkg/channel/distributed/common/env"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	clientgotesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/record"
	"knative.dev/eventing-kafka/pkg/apis/messaging/v1beta1"
	"knative.dev/eventing-kafka/pkg/channel/distributed/dispatcher/dispatcher"
	reconciletesting "knative.dev/eventing-kafka/pkg/channel/distributed/dispatcher/testing"
	"knative.dev/eventing-kafka/pkg/client/clientset/versioned"
	fakeclientset "knative.dev/eventing-kafka/pkg/client/clientset/versioned/fake"
	"knative.dev/eventing-kafka/pkg/client/informers/externalversions"
	eventingduck "knative.dev/eventing/pkg/apis/duck/v1"
	"knative.dev/pkg/controller"
	kncontroller "knative.dev/pkg/controller"
	logtesting "knative.dev/pkg/logging/testing"
	. "knative.dev/pkg/reconciler/testing"
	reconcilertesting "knative.dev/pkg/reconciler/testing"
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

	// Test Data
	logger := logtesting.TestLogger(t).Desugar()
	channelKey := "TestChannelKey"
	mockDispatcher := NewMockDispatcher(t)
	fakeKafkaChannelClientSet := fakeclientset.NewSimpleClientset()
	fakeK8sClientSet := fake.NewSimpleClientset()
	populateEnvironmentVariables(t)
	kafkaInformerFactory := externalversions.NewSharedInformerFactory(fakeKafkaChannelClientSet, kncontroller.DefaultResyncPeriod)
	kafkaChannelInformer := kafkaInformerFactory.Messaging().V1beta1().KafkaChannels()
	stopChan := make(chan struct{})
	eventsChan := make(chan consumer.ManagerEvent)

	// Perform The Test
	c := NewController(logger, channelKey, mockDispatcher, kafkaChannelInformer, fakeK8sClientSet, fakeKafkaChannelClientSet, stopChan, eventsChan)

	// Verify Results
	assert.NotNil(t, c)

	// Close The
	close(stopChan)
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

	table.Test(t, reconciletesting.MakeFactory(func(listers *reconciletesting.Listers, kafkaClient versioned.Interface, eventRecorder record.EventRecorder) controller.Reconciler {
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
	t *testing.T
}

// Mock Dispatcher Constructor
func NewMockDispatcher(t *testing.T) MockDispatcher {
	return MockDispatcher{t: t}
}

func (m MockDispatcher) Shutdown() {
}

func (m MockDispatcher) UpdateSubscriptions(_ []eventingduck.SubscriberSpec) (map[eventingduck.SubscriberSpec]error, map[eventingduck.SubscriberSpec]struct{}) {
	return nil, nil
}

func (m MockDispatcher) SecretChanged(_ context.Context, _ *corev1.Secret) {}
