/*
Copyright 2019 The Knative Authors

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
	"testing"

	"github.com/Shopify/sarama"
	"go.uber.org/zap"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/scheme"
	clientgotesting "k8s.io/client-go/testing"

	"knative.dev/eventing-kafka/pkg/apis/messaging/v1beta1"
	"knative.dev/eventing-kafka/pkg/channel/consolidated/reconciler/controller/resources"
	reconcilertesting "knative.dev/eventing-kafka/pkg/channel/consolidated/reconciler/testing"
	. "knative.dev/eventing-kafka/pkg/channel/consolidated/utils"
	fakekafkaclient "knative.dev/eventing-kafka/pkg/client/injection/client/fake"
	"knative.dev/eventing-kafka/pkg/client/injection/reconciler/messaging/v1beta1/kafkachannel"
	eventingduckv1 "knative.dev/eventing/pkg/apis/duck/v1"
	eventingClient "knative.dev/eventing/pkg/client/injection/client"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/kmeta"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/network"
	. "knative.dev/pkg/reconciler/testing"
)

const (
	testNS                       = "test-namespace"
	testDispatcherserviceAccount = "kafka-ch-dispatcher"
	testConfigMapHash            = "deadbeef"
	kcName                       = "test-kc"
	testDispatcherImage          = "test-image"
	channelServiceAddress        = "test-kc-kn-channel.test-namespace.svc.cluster.local"
	brokerName                   = "test-broker"
	finalizerName                = "kafkachannels.messaging.knative.dev"
	sub1UID                      = "2f9b5e8e-deb6-11e8-9f32-f2801f1b9fd1"
	sub2UID                      = "34c5aec8-deb6-11e8-9f32-f2801f1b9fd1"
	twoSubscribersPatch          = `[{"op":"add","path":"/status/subscribers","value":[{"observedGeneration":1,"ready":"True","uid":"2f9b5e8e-deb6-11e8-9f32-f2801f1b9fd1"},{"observedGeneration":2,"ready":"True","uid":"34c5aec8-deb6-11e8-9f32-f2801f1b9fd1"}]}]`
)

var (
	finalizerUpdatedEvent = Eventf(corev1.EventTypeNormal, "FinalizerUpdate", `Updated "test-kc" finalizers`)
)

func init() {
	// Add types to scheme
	_ = v1beta1.AddToScheme(scheme.Scheme)
	_ = duckv1.AddToScheme(scheme.Scheme)
}

func TestAllCases(t *testing.T) {
	kcKey := testNS + "/" + kcName
	table := TableTest{
		{
			Name: "bad workqueue key",
			// Make sure Reconcile handles bad keys.
			Key: "too/many/parts",
		}, {
			Name: "key not found",
			// Make sure Reconcile handles good keys that don't exist.
			Key: "foo/not-found",
		}, {
			Name: "deleting",
			Key:  kcKey,
			Objects: []runtime.Object{
				reconcilertesting.NewKafkaChannel(kcName, testNS,
					reconcilertesting.WithInitKafkaChannelConditions,
					reconcilertesting.WithKafkaChannelDeleted)},
			WantErr: false,
			WantEvents: []string{
				Eventf(corev1.EventTypeNormal, "KafkaChannelReconciled", `KafkaChannel reconciled: "test-namespace/test-kc"`),
			},
		}, {
			Name: "deployment does not exist, automatically created and patching finalizers",
			Key:  kcKey,
			Objects: []runtime.Object{
				reconcilertesting.NewKafkaChannel(kcName, testNS,
					reconcilertesting.WithInitKafkaChannelConditions,
					reconcilertesting.WithKafkaChannelTopicReady()),
			},
			WantErr: true,
			WantCreates: []runtime.Object{
				makeDeployment(),
				makeService(),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{{
				Object: reconcilertesting.NewKafkaChannel(kcName, testNS,
					reconcilertesting.WithInitKafkaChannelConditions,
					reconcilertesting.WithKafkaChannelConfigReady(),
					reconcilertesting.WithKafkaChannelTopicReady(),
					reconcilertesting.WithKafkaChannelServiceReady(),
					reconcilertesting.WithKafkaChannelEndpointsNotReady("DispatcherEndpointsDoesNotExist", "Dispatcher Endpoints does not exist")),
			}},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(testNS, kcName),
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
				Eventf(corev1.EventTypeNormal, dispatcherDeploymentCreated, "Dispatcher deployment created"),
				Eventf(corev1.EventTypeNormal, dispatcherServiceCreated, "Dispatcher service created"),
				Eventf(corev1.EventTypeWarning, "InternalError", `endpoints "kafka-ch-dispatcher" not found`),
			},
		}, {
			Name: "Service does not exist, automatically created",
			Key:  kcKey,
			Objects: []runtime.Object{
				makeReadyDeployment(),
				reconcilertesting.NewKafkaChannel(kcName, testNS,
					reconcilertesting.WithKafkaFinalizer(finalizerName)),
			},
			WantErr: true,
			WantCreates: []runtime.Object{
				makeService(),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{{
				Object: reconcilertesting.NewKafkaChannel(kcName, testNS,
					reconcilertesting.WithInitKafkaChannelConditions,
					reconcilertesting.WithKafkaFinalizer(finalizerName),
					reconcilertesting.WithKafkaChannelConfigReady(),
					reconcilertesting.WithKafkaChannelTopicReady(),
					reconcilertesting.WithKafkaChannelDeploymentReady(),
					reconcilertesting.WithKafkaChannelServiceReady(),
					reconcilertesting.WithKafkaChannelEndpointsNotReady("DispatcherEndpointsDoesNotExist", "Dispatcher Endpoints does not exist")),
			}},
			WantEvents: []string{
				Eventf(corev1.EventTypeNormal, dispatcherServiceCreated, "Dispatcher service created"),
				Eventf(corev1.EventTypeWarning, "InternalError", `endpoints "kafka-ch-dispatcher" not found`),
			},
		}, {
			Name: "Endpoints does not exist",
			Key:  kcKey,
			Objects: []runtime.Object{
				makeReadyDeployment(),
				makeService(),
				reconcilertesting.NewKafkaChannel(kcName, testNS,
					reconcilertesting.WithKafkaFinalizer(finalizerName)),
			},
			WantErr: true,
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{{
				Object: reconcilertesting.NewKafkaChannel(kcName, testNS,
					reconcilertesting.WithInitKafkaChannelConditions,
					reconcilertesting.WithKafkaFinalizer(finalizerName),
					reconcilertesting.WithKafkaChannelConfigReady(),
					reconcilertesting.WithKafkaChannelTopicReady(),
					reconcilertesting.WithKafkaChannelDeploymentReady(),
					reconcilertesting.WithKafkaChannelServiceReady(),
					reconcilertesting.WithKafkaChannelEndpointsNotReady("DispatcherEndpointsDoesNotExist", "Dispatcher Endpoints does not exist"),
				),
			}},
			WantEvents: []string{
				Eventf(corev1.EventTypeWarning, "InternalError", `endpoints "kafka-ch-dispatcher" not found`),
			},
		}, {
			Name: "Endpoints not ready",
			Key:  kcKey,
			Objects: []runtime.Object{
				makeReadyDeployment(),
				makeService(),
				makeEmptyEndpoints(),
				reconcilertesting.NewKafkaChannel(kcName, testNS,
					reconcilertesting.WithKafkaFinalizer(finalizerName)),
			},
			WantErr: true,
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{{
				Object: reconcilertesting.NewKafkaChannel(kcName, testNS,
					reconcilertesting.WithInitKafkaChannelConditions,
					reconcilertesting.WithKafkaFinalizer(finalizerName),
					reconcilertesting.WithKafkaChannelConfigReady(),
					reconcilertesting.WithKafkaChannelTopicReady(),
					reconcilertesting.WithKafkaChannelDeploymentReady(),
					reconcilertesting.WithKafkaChannelServiceReady(),
					reconcilertesting.WithKafkaChannelEndpointsNotReady("DispatcherEndpointsNotReady", "There are no endpoints ready for Dispatcher service"),
				),
			}},
			WantEvents: []string{
				Eventf(corev1.EventTypeWarning, "InternalError", `there are no endpoints ready for Dispatcher service kafka-ch-dispatcher`),
			},
		}, {
			Name: "Works, creates new channel",
			Key:  kcKey,
			Objects: []runtime.Object{
				makeReadyDeployment(),
				makeService(),
				makeReadyEndpoints(),
				reconcilertesting.NewKafkaChannel(kcName, testNS,
					reconcilertesting.WithKafkaFinalizer(finalizerName)),
			},
			WantErr: false,
			WantCreates: []runtime.Object{
				makeChannelService(reconcilertesting.NewKafkaChannel(kcName, testNS)),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{{
				Object: reconcilertesting.NewKafkaChannel(kcName, testNS,
					reconcilertesting.WithInitKafkaChannelConditions,
					reconcilertesting.WithKafkaFinalizer(finalizerName),
					reconcilertesting.WithKafkaChannelConfigReady(),
					reconcilertesting.WithKafkaChannelTopicReady(),
					reconcilertesting.WithKafkaChannelDeploymentReady(),
					reconcilertesting.WithKafkaChannelServiceReady(),
					reconcilertesting.WithKafkaChannelEndpointsReady(),
					reconcilertesting.WithKafkaChannelChannelServiceReady(),
					reconcilertesting.WithKafkaChannelAddress(channelServiceAddress),
				),
			}},
			WantEvents: []string{
				Eventf(corev1.EventTypeNormal, "KafkaChannelReconciled", `KafkaChannel reconciled: "test-namespace/test-kc"`),
			},
		}, {
			Name: "Works, channel exists",
			Key:  kcKey,
			Objects: []runtime.Object{
				makeReadyDeployment(),
				makeService(),
				makeReadyEndpoints(),
				reconcilertesting.NewKafkaChannel(kcName, testNS,
					reconcilertesting.WithKafkaChannelSubscribers(subscribers()),
					reconcilertesting.WithKafkaFinalizer(finalizerName)),
				makeChannelService(reconcilertesting.NewKafkaChannel(kcName, testNS)),
			},
			WantErr: false,
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{{
				Object: reconcilertesting.NewKafkaChannel(kcName, testNS,
					reconcilertesting.WithKafkaChannelSubscribers(subscribers()),
					reconcilertesting.WithInitKafkaChannelConditions,
					reconcilertesting.WithKafkaFinalizer(finalizerName),
					reconcilertesting.WithKafkaChannelConfigReady(),
					reconcilertesting.WithKafkaChannelTopicReady(),
					reconcilertesting.WithKafkaChannelDeploymentReady(),
					reconcilertesting.WithKafkaChannelServiceReady(),
					reconcilertesting.WithKafkaChannelEndpointsReady(),
					reconcilertesting.WithKafkaChannelChannelServiceReady(),
					reconcilertesting.WithKafkaChannelAddress(channelServiceAddress),
				),
			}},
			WantEvents: []string{
				Eventf(corev1.EventTypeNormal, "KafkaChannelReconciled", `KafkaChannel reconciled: "test-namespace/test-kc"`),
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				makePatch(testNS, kcName, twoSubscribersPatch),
			},
		}, {
			Name: "channel exists, not owned by us",
			Key:  kcKey,
			Objects: []runtime.Object{
				makeReadyDeployment(),
				makeService(),
				makeReadyEndpoints(),
				reconcilertesting.NewKafkaChannel(kcName, testNS,
					reconcilertesting.WithKafkaFinalizer(finalizerName)),
				makeChannelServiceNotOwnedByUs(),
			},
			WantErr: true,
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{{
				Object: reconcilertesting.NewKafkaChannel(kcName, testNS,
					reconcilertesting.WithInitKafkaChannelConditions,
					reconcilertesting.WithKafkaFinalizer(finalizerName),
					reconcilertesting.WithKafkaChannelConfigReady(),
					reconcilertesting.WithKafkaChannelTopicReady(),
					reconcilertesting.WithKafkaChannelDeploymentReady(),
					reconcilertesting.WithKafkaChannelServiceReady(),
					reconcilertesting.WithKafkaChannelEndpointsReady(),
					reconcilertesting.WithKafkaChannelChannelServicetNotReady("ChannelServiceFailed", "Channel Service failed: kafkachannel: test-namespace/test-kc does not own Service: \"test-kc-kn-channel\""),
				),
			}},
			WantEvents: []string{
				Eventf(corev1.EventTypeWarning, "InternalError", `kafkachannel: test-namespace/test-kc does not own Service: "test-kc-kn-channel"`),
			},
		}, {
			Name: "channel does not exist, fails to create",
			Key:  kcKey,
			Objects: []runtime.Object{
				makeReadyDeployment(),
				makeService(),
				makeReadyEndpoints(),
				reconcilertesting.NewKafkaChannel(kcName, testNS,
					reconcilertesting.WithKafkaFinalizer(finalizerName)),
			},
			WantErr: true,
			WithReactors: []clientgotesting.ReactionFunc{
				InduceFailure("create", "Services"),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{{
				Object: reconcilertesting.NewKafkaChannel(kcName, testNS,
					reconcilertesting.WithInitKafkaChannelConditions,
					reconcilertesting.WithKafkaFinalizer(finalizerName),
					reconcilertesting.WithKafkaChannelConfigReady(),
					reconcilertesting.WithKafkaChannelTopicReady(),
					reconcilertesting.WithKafkaChannelDeploymentReady(),
					reconcilertesting.WithKafkaChannelServiceReady(),
					reconcilertesting.WithKafkaChannelEndpointsReady(),
					reconcilertesting.WithKafkaChannelChannelServicetNotReady("ChannelServiceFailed", "Channel Service failed: inducing failure for create services"),
				),
			}},
			WantCreates: []runtime.Object{
				makeChannelService(reconcilertesting.NewKafkaChannel(kcName, testNS)),
			},
			WantEvents: []string{
				Eventf(corev1.EventTypeWarning, "InternalError", "inducing failure for create services"),
			},
			// TODO add UTs for topic creation and deletion.
		},
	}

	table.Test(t, reconcilertesting.MakeFactory(func(ctx context.Context, listers *reconcilertesting.Listers, cmw configmap.Watcher) controller.Reconciler {

		r := &Reconciler{
			systemNamespace:          testNS,
			dispatcherImage:          testDispatcherImage,
			dispatcherServiceAccount: testDispatcherserviceAccount,
			kafkaConfigMapHash:       testConfigMapHash,
			kafkaConfig: &KafkaConfig{
				Brokers: []string{brokerName},
			},
			kafkachannelLister: listers.GetKafkaChannelLister(),
			// TODO fix
			kafkachannelInformer: nil,
			deploymentLister:     listers.GetDeploymentLister(),
			serviceLister:        listers.GetServiceLister(),
			endpointsLister:      listers.GetEndpointsLister(),
			kafkaClusterAdmin:    &mockClusterAdmin{},
			kafkaClientSet:       fakekafkaclient.Get(ctx),
			KubeClientSet:        kubeclient.Get(ctx),
			EventingClientSet:    eventingClient.Get(ctx),
			statusManager: &fakeStatusManager{
				FakeIsReady: func(ctx context.Context, ch v1beta1.KafkaChannel,
					sub eventingduckv1.SubscriberSpec) (bool, error) {
					return true, nil
				},
			},
		}
		return kafkachannel.NewReconciler(ctx, logging.FromContext(ctx), r.kafkaClientSet, listers.GetKafkaChannelLister(), controller.GetEventRecorder(ctx), r)
	}, zap.L()))
}

func TestTopicExists(t *testing.T) {
	kcKey := testNS + "/" + kcName
	row := TableRow{
		Name: "Works, topic already exists",
		Key:  kcKey,
		Objects: []runtime.Object{
			makeReadyDeployment(),
			makeService(),
			makeReadyEndpoints(),
			reconcilertesting.NewKafkaChannel(kcName, testNS,
				reconcilertesting.WithKafkaFinalizer(finalizerName)),
		},
		WantErr: false,
		WantCreates: []runtime.Object{
			makeChannelService(reconcilertesting.NewKafkaChannel(kcName, testNS)),
		},
		WantStatusUpdates: []clientgotesting.UpdateActionImpl{{
			Object: reconcilertesting.NewKafkaChannel(kcName, testNS,
				reconcilertesting.WithInitKafkaChannelConditions,
				reconcilertesting.WithKafkaFinalizer(finalizerName),
				reconcilertesting.WithKafkaChannelConfigReady(),
				reconcilertesting.WithKafkaChannelTopicReady(),
				reconcilertesting.WithKafkaChannelDeploymentReady(),
				reconcilertesting.WithKafkaChannelServiceReady(),
				reconcilertesting.WithKafkaChannelEndpointsReady(),
				reconcilertesting.WithKafkaChannelChannelServiceReady(),
				reconcilertesting.WithKafkaChannelAddress(channelServiceAddress),
			),
		}},
		WantEvents: []string{
			Eventf(corev1.EventTypeNormal, "KafkaChannelReconciled", `KafkaChannel reconciled: "test-namespace/test-kc"`),
		},
	}

	row.Test(t, reconcilertesting.MakeFactory(func(ctx context.Context, listers *reconcilertesting.Listers, cmw configmap.Watcher) controller.Reconciler {

		r := &Reconciler{
			systemNamespace:          testNS,
			dispatcherImage:          testDispatcherImage,
			dispatcherServiceAccount: testDispatcherserviceAccount,
			kafkaConfigMapHash:       testConfigMapHash,
			kafkaConfig: &KafkaConfig{
				Brokers: []string{brokerName},
			},
			kafkachannelLister: listers.GetKafkaChannelLister(),
			// TODO fix
			kafkachannelInformer: nil,
			deploymentLister:     listers.GetDeploymentLister(),
			serviceLister:        listers.GetServiceLister(),
			endpointsLister:      listers.GetEndpointsLister(),
			kafkaClusterAdmin: &mockClusterAdmin{
				mockCreateTopicFunc: func(topic string, detail *sarama.TopicDetail, validateOnly bool) error {
					errMsg := sarama.ErrTopicAlreadyExists.Error()
					return &sarama.TopicError{
						Err:    sarama.ErrTopicAlreadyExists,
						ErrMsg: &errMsg,
					}
				},
			},
			kafkaClientSet:    fakekafkaclient.Get(ctx),
			KubeClientSet:     kubeclient.Get(ctx),
			EventingClientSet: eventingClient.Get(ctx),
			statusManager: &fakeStatusManager{
				FakeIsReady: func(ctx context.Context, channel v1beta1.KafkaChannel,
					spec eventingduckv1.SubscriberSpec) (bool, error) {
					return true, nil
				},
			},
		}
		return kafkachannel.NewReconciler(ctx, logging.FromContext(ctx), r.kafkaClientSet, listers.GetKafkaChannelLister(), controller.GetEventRecorder(ctx), r)
	}, zap.L()))
}

func TestDeploymentUpdatedOnImageChange(t *testing.T) {
	kcKey := testNS + "/" + kcName
	row := TableRow{
		Name: "Works, topic already exists",
		Key:  kcKey,
		Objects: []runtime.Object{
			makeDeploymentWithImageAndReplicas("differentimage", 1),
			makeService(),
			makeReadyEndpoints(),
			reconcilertesting.NewKafkaChannel(kcName, testNS,
				reconcilertesting.WithKafkaFinalizer(finalizerName)),
		},
		WantErr: false,
		WantCreates: []runtime.Object{
			makeChannelService(reconcilertesting.NewKafkaChannel(kcName, testNS)),
		},
		WantUpdates: []clientgotesting.UpdateActionImpl{{
			Object: makeDeployment(),
		}},
		WantStatusUpdates: []clientgotesting.UpdateActionImpl{{
			Object: reconcilertesting.NewKafkaChannel(kcName, testNS,
				reconcilertesting.WithInitKafkaChannelConditions,
				reconcilertesting.WithKafkaFinalizer(finalizerName),
				reconcilertesting.WithKafkaChannelConfigReady(),
				reconcilertesting.WithKafkaChannelTopicReady(),
				//				reconcilekafkatesting.WithKafkaChannelDeploymentReady(),
				reconcilertesting.WithKafkaChannelServiceReady(),
				reconcilertesting.WithKafkaChannelEndpointsReady(),
				reconcilertesting.WithKafkaChannelChannelServiceReady(),
				reconcilertesting.WithKafkaChannelAddress(channelServiceAddress),
			),
		}},
		WantEvents: []string{
			Eventf(corev1.EventTypeNormal, dispatcherDeploymentUpdated, "Dispatcher deployment updated"),
			Eventf(corev1.EventTypeNormal, "KafkaChannelReconciled", `KafkaChannel reconciled: "test-namespace/test-kc"`),
		},
	}

	row.Test(t, reconcilertesting.MakeFactory(func(ctx context.Context, listers *reconcilertesting.Listers, cmw configmap.Watcher) controller.Reconciler {

		r := &Reconciler{
			systemNamespace:          testNS,
			dispatcherImage:          testDispatcherImage,
			dispatcherServiceAccount: testDispatcherserviceAccount,
			kafkaConfigMapHash:       testConfigMapHash,
			kafkaConfig: &KafkaConfig{
				Brokers: []string{brokerName},
			},
			kafkachannelLister: listers.GetKafkaChannelLister(),
			// TODO fix
			kafkachannelInformer: nil,
			deploymentLister:     listers.GetDeploymentLister(),
			serviceLister:        listers.GetServiceLister(),
			endpointsLister:      listers.GetEndpointsLister(),
			kafkaClusterAdmin: &mockClusterAdmin{
				mockCreateTopicFunc: func(topic string, detail *sarama.TopicDetail, validateOnly bool) error {
					errMsg := sarama.ErrTopicAlreadyExists.Error()
					return &sarama.TopicError{
						Err:    sarama.ErrTopicAlreadyExists,
						ErrMsg: &errMsg,
					}
				},
			},
			kafkaClientSet:    fakekafkaclient.Get(ctx),
			KubeClientSet:     kubeclient.Get(ctx),
			EventingClientSet: eventingClient.Get(ctx),
			statusManager: &fakeStatusManager{
				FakeIsReady: func(ctx context.Context, channel v1beta1.KafkaChannel,
					spec eventingduckv1.SubscriberSpec) (bool, error) {
					return true, nil
				},
			},
		}
		return kafkachannel.NewReconciler(ctx, logging.FromContext(ctx), r.kafkaClientSet, listers.GetKafkaChannelLister(), controller.GetEventRecorder(ctx), r)
	}, zap.L()))
}

func TestDeploymentZeroReplicas(t *testing.T) {
	kcKey := testNS + "/" + kcName
	row := TableRow{
		Name: "Works, topic already exists",
		Key:  kcKey,
		Objects: []runtime.Object{
			makeDeploymentWithImageAndReplicas(testDispatcherImage, 0),
			makeService(),
			makeReadyEndpoints(),
			reconcilertesting.NewKafkaChannel(kcName, testNS,
				reconcilertesting.WithKafkaFinalizer(finalizerName)),
		},
		WantErr: false,
		WantCreates: []runtime.Object{
			makeChannelService(reconcilertesting.NewKafkaChannel(kcName, testNS)),
		},
		WantUpdates: []clientgotesting.UpdateActionImpl{{
			Object: makeDeployment(),
		}},
		WantStatusUpdates: []clientgotesting.UpdateActionImpl{{
			Object: reconcilertesting.NewKafkaChannel(kcName, testNS,
				reconcilertesting.WithInitKafkaChannelConditions,
				reconcilertesting.WithKafkaFinalizer(finalizerName),
				reconcilertesting.WithKafkaChannelConfigReady(),
				reconcilertesting.WithKafkaChannelTopicReady(),
				//				reconcilekafkatesting.WithKafkaChannelDeploymentReady(),
				reconcilertesting.WithKafkaChannelServiceReady(),
				reconcilertesting.WithKafkaChannelEndpointsReady(),
				reconcilertesting.WithKafkaChannelChannelServiceReady(),
				reconcilertesting.WithKafkaChannelAddress(channelServiceAddress),
			),
		}},
		WantEvents: []string{
			Eventf(corev1.EventTypeNormal, dispatcherDeploymentUpdated, "Dispatcher deployment updated"),
			Eventf(corev1.EventTypeNormal, "KafkaChannelReconciled", `KafkaChannel reconciled: "test-namespace/test-kc"`),
		},
	}

	row.Test(t, reconcilertesting.MakeFactory(func(ctx context.Context, listers *reconcilertesting.Listers, cmw configmap.Watcher) controller.Reconciler {

		r := &Reconciler{
			systemNamespace:          testNS,
			dispatcherImage:          testDispatcherImage,
			dispatcherServiceAccount: testDispatcherserviceAccount,
			kafkaConfigMapHash:       testConfigMapHash,
			kafkaConfig: &KafkaConfig{
				Brokers: []string{brokerName},
			},
			kafkachannelLister: listers.GetKafkaChannelLister(),
			// TODO fix
			kafkachannelInformer: nil,
			deploymentLister:     listers.GetDeploymentLister(),
			serviceLister:        listers.GetServiceLister(),
			endpointsLister:      listers.GetEndpointsLister(),
			kafkaClusterAdmin: &mockClusterAdmin{
				mockCreateTopicFunc: func(topic string, detail *sarama.TopicDetail, validateOnly bool) error {
					errMsg := sarama.ErrTopicAlreadyExists.Error()
					return &sarama.TopicError{
						Err:    sarama.ErrTopicAlreadyExists,
						ErrMsg: &errMsg,
					}
				},
			},
			kafkaClientSet:    fakekafkaclient.Get(ctx),
			KubeClientSet:     kubeclient.Get(ctx),
			EventingClientSet: eventingClient.Get(ctx),
			statusManager: &fakeStatusManager{
				FakeIsReady: func(ctx context.Context, channel v1beta1.KafkaChannel,
					spec eventingduckv1.SubscriberSpec) (bool, error) {
					return true, nil
				},
			},
		}
		return kafkachannel.NewReconciler(ctx, logging.FromContext(ctx), r.kafkaClientSet, listers.GetKafkaChannelLister(), controller.GetEventRecorder(ctx), r)
	}, zap.L()))
}

func TestDeploymentMoreThanOneReplicas(t *testing.T) {
	kcKey := testNS + "/" + kcName
	row := TableRow{
		Name: "Works, topic already exists",
		Key:  kcKey,
		Objects: []runtime.Object{
			makeDeploymentWithImageAndReplicas(testDispatcherImage, 3),
			makeService(),
			makeReadyEndpoints(),
			reconcilertesting.NewKafkaChannel(kcName, testNS,
				reconcilertesting.WithKafkaFinalizer(finalizerName)),
		},
		WantErr: false,
		WantCreates: []runtime.Object{
			makeChannelService(reconcilertesting.NewKafkaChannel(kcName, testNS)),
		},
		WantUpdates: []clientgotesting.UpdateActionImpl{},
		WantStatusUpdates: []clientgotesting.UpdateActionImpl{{
			Object: reconcilertesting.NewKafkaChannel(kcName, testNS,
				reconcilertesting.WithInitKafkaChannelConditions,
				reconcilertesting.WithKafkaFinalizer(finalizerName),
				reconcilertesting.WithKafkaChannelConfigReady(),
				reconcilertesting.WithKafkaChannelTopicReady(),
				reconcilertesting.WithKafkaChannelServiceReady(),
				reconcilertesting.WithKafkaChannelEndpointsReady(),
				reconcilertesting.WithKafkaChannelChannelServiceReady(),
				reconcilertesting.WithKafkaChannelAddress(channelServiceAddress),
			),
		}},
		WantEvents: []string{
			Eventf(corev1.EventTypeNormal, "KafkaChannelReconciled", `KafkaChannel reconciled: "test-namespace/test-kc"`),
		},
	}

	row.Test(t, reconcilertesting.MakeFactory(func(ctx context.Context, listers *reconcilertesting.Listers, cmw configmap.Watcher) controller.Reconciler {

		r := &Reconciler{
			systemNamespace:          testNS,
			dispatcherImage:          testDispatcherImage,
			dispatcherServiceAccount: testDispatcherserviceAccount,
			kafkaConfigMapHash:       testConfigMapHash,
			kafkaConfig: &KafkaConfig{
				Brokers: []string{brokerName},
			},
			kafkachannelLister: listers.GetKafkaChannelLister(),
			// TODO fix
			kafkachannelInformer: nil,
			deploymentLister:     listers.GetDeploymentLister(),
			serviceLister:        listers.GetServiceLister(),
			endpointsLister:      listers.GetEndpointsLister(),
			kafkaClusterAdmin: &mockClusterAdmin{
				mockCreateTopicFunc: func(topic string, detail *sarama.TopicDetail, validateOnly bool) error {
					errMsg := sarama.ErrTopicAlreadyExists.Error()
					return &sarama.TopicError{
						Err:    sarama.ErrTopicAlreadyExists,
						ErrMsg: &errMsg,
					}
				},
			},
			kafkaClientSet:    fakekafkaclient.Get(ctx),
			KubeClientSet:     kubeclient.Get(ctx),
			EventingClientSet: eventingClient.Get(ctx),
			statusManager: &fakeStatusManager{
				FakeIsReady: func(ctx context.Context, channel v1beta1.KafkaChannel,
					spec eventingduckv1.SubscriberSpec) (bool, error) {
					return true, nil
				},
			},
		}
		return kafkachannel.NewReconciler(ctx, logging.FromContext(ctx), r.kafkaClientSet, listers.GetKafkaChannelLister(), controller.GetEventRecorder(ctx), r)
	}, zap.L()))
}

func TestDeploymentUpdatedOnConfigMapHashChange(t *testing.T) {
	kcKey := testNS + "/" + kcName
	row := TableRow{
		Name: "ConfigMap hash changed, dispatcher updated",
		Key:  kcKey,
		Objects: []runtime.Object{
			makeDeploymentWithConfigMapHash("toBeUpdated"),
			makeService(),
			makeReadyEndpoints(),
			reconcilertesting.NewKafkaChannel(kcName, testNS,
				reconcilertesting.WithKafkaFinalizer(finalizerName)),
		},
		WantErr: false,
		WantCreates: []runtime.Object{
			makeChannelService(reconcilertesting.NewKafkaChannel(kcName, testNS)),
		},
		WantUpdates: []clientgotesting.UpdateActionImpl{{
			Object: makeDeploymentWithConfigMapHash(testConfigMapHash),
		}},
		WantStatusUpdates: []clientgotesting.UpdateActionImpl{{
			Object: reconcilertesting.NewKafkaChannel(kcName, testNS,
				reconcilertesting.WithInitKafkaChannelConditions,
				reconcilertesting.WithKafkaFinalizer(finalizerName),
				reconcilertesting.WithKafkaChannelConfigReady(),
				reconcilertesting.WithKafkaChannelTopicReady(),
				//				reconcilekafkatesting.WithKafkaChannelDeploymentReady(),
				reconcilertesting.WithKafkaChannelServiceReady(),
				reconcilertesting.WithKafkaChannelEndpointsReady(),
				reconcilertesting.WithKafkaChannelChannelServiceReady(),
				reconcilertesting.WithKafkaChannelAddress(channelServiceAddress),
			),
		}},
		WantEvents: []string{
			Eventf(corev1.EventTypeNormal, dispatcherDeploymentUpdated, "Dispatcher deployment updated"),
			Eventf(corev1.EventTypeNormal, "KafkaChannelReconciled", `KafkaChannel reconciled: "test-namespace/test-kc"`),
		},
	}

	row.Test(t, reconcilertesting.MakeFactory(func(ctx context.Context, listers *reconcilertesting.Listers, cmw configmap.Watcher) controller.Reconciler {

		r := &Reconciler{
			systemNamespace:          testNS,
			dispatcherImage:          testDispatcherImage,
			dispatcherServiceAccount: testDispatcherserviceAccount,
			kafkaConfigMapHash:       testConfigMapHash,
			kafkaConfig: &KafkaConfig{
				Brokers: []string{brokerName},
			},
			kafkachannelLister: listers.GetKafkaChannelLister(),
			// TODO fix
			kafkachannelInformer: nil,
			deploymentLister:     listers.GetDeploymentLister(),
			serviceLister:        listers.GetServiceLister(),
			endpointsLister:      listers.GetEndpointsLister(),
			kafkaClusterAdmin: &mockClusterAdmin{
				mockCreateTopicFunc: func(topic string, detail *sarama.TopicDetail, validateOnly bool) error {
					errMsg := sarama.ErrTopicAlreadyExists.Error()
					return &sarama.TopicError{
						Err:    sarama.ErrTopicAlreadyExists,
						ErrMsg: &errMsg,
					}
				},
			},
			kafkaClientSet:    fakekafkaclient.Get(ctx),
			KubeClientSet:     kubeclient.Get(ctx),
			EventingClientSet: eventingClient.Get(ctx),
			statusManager: &fakeStatusManager{
				FakeIsReady: func(ctx context.Context, channel v1beta1.KafkaChannel,
					spec eventingduckv1.SubscriberSpec) (bool, error) {
					return true, nil
				},
			},
		}
		return kafkachannel.NewReconciler(ctx, logging.FromContext(ctx), r.kafkaClientSet, listers.GetKafkaChannelLister(), controller.GetEventRecorder(ctx), r)
	}, zap.L()))
}

type mockClusterAdmin struct {
	mockCreateTopicFunc func(topic string, detail *sarama.TopicDetail, validateOnly bool) error
	mockDeleteTopicFunc func(topic string) error
}

func (ca *mockClusterAdmin) AlterPartitionReassignments(topic string, assignment [][]int32) error {
	return nil
}

func (ca *mockClusterAdmin) ListPartitionReassignments(topics string, partitions []int32) (topicStatus map[string]map[int32]*sarama.PartitionReplicaReassignmentsStatus, err error) {
	return nil, nil
}

func (ca *mockClusterAdmin) DescribeLogDirs(brokers []int32) (map[int32][]sarama.DescribeLogDirsResponseDirMetadata, error) {
	return nil, nil
}

func (ca *mockClusterAdmin) CreateTopic(topic string, detail *sarama.TopicDetail, validateOnly bool) error {
	if ca.mockCreateTopicFunc != nil {
		return ca.mockCreateTopicFunc(topic, detail, validateOnly)
	}
	return nil
}

func (ca *mockClusterAdmin) Close() error {
	return nil
}

func (ca *mockClusterAdmin) DeleteTopic(topic string) error {
	if ca.mockDeleteTopicFunc != nil {
		return ca.mockDeleteTopicFunc(topic)
	}
	return nil
}

func (ca *mockClusterAdmin) DescribeTopics(topics []string) (metadata []*sarama.TopicMetadata, err error) {
	return nil, nil
}

func (ca *mockClusterAdmin) ListTopics() (map[string]sarama.TopicDetail, error) {
	return nil, nil
}

func (ca *mockClusterAdmin) CreatePartitions(topic string, count int32, assignment [][]int32, validateOnly bool) error {
	return nil
}

func (ca *mockClusterAdmin) DeleteRecords(topic string, partitionOffsets map[int32]int64) error {
	return nil
}

func (ca *mockClusterAdmin) DescribeConfig(resource sarama.ConfigResource) ([]sarama.ConfigEntry, error) {
	return nil, nil
}

func (ca *mockClusterAdmin) AlterConfig(resourceType sarama.ConfigResourceType, name string, entries map[string]*string, validateOnly bool) error {
	return nil
}

func (ca *mockClusterAdmin) CreateACL(resource sarama.Resource, acl sarama.Acl) error {
	return nil
}

func (ca *mockClusterAdmin) ListAcls(filter sarama.AclFilter) ([]sarama.ResourceAcls, error) {
	return nil, nil
}

func (ca *mockClusterAdmin) DeleteACL(filter sarama.AclFilter, validateOnly bool) ([]sarama.MatchingAcl, error) {
	return nil, nil
}

func (ca *mockClusterAdmin) ListConsumerGroups() (map[string]string, error) {
	cgs := map[string]string{
		fmt.Sprintf("kafka.%s.%s.%s", kcName, testNS, sub1UID): "consumer",
		fmt.Sprintf("kafka.%s.%s.%s", kcName, testNS, sub2UID): "consumer",
	}
	return cgs, nil
}

func (ca *mockClusterAdmin) DescribeConsumerGroups(groups []string) ([]*sarama.GroupDescription, error) {
	return nil, nil
}

func (ca *mockClusterAdmin) ListConsumerGroupOffsets(group string, topicPartitions map[string][]int32) (*sarama.OffsetFetchResponse, error) {
	return &sarama.OffsetFetchResponse{}, nil
}

func (ca *mockClusterAdmin) DescribeCluster() (brokers []*sarama.Broker, controllerID int32, err error) {
	return nil, 0, nil
}

// Delete a consumer group.
func (ca *mockClusterAdmin) DeleteConsumerGroup(group string) error {
	return nil
}

var _ sarama.ClusterAdmin = (*mockClusterAdmin)(nil)

func makeDeploymentWithParams(image string, replicas int32, configMapHash string) *appsv1.Deployment {
	args := resources.DispatcherArgs{
		DispatcherNamespace: testNS,
		Image:               image,
		Replicas:            replicas,
		ServiceAccount:      testDispatcherserviceAccount,
		ConfigMapHash:       configMapHash,
	}
	return resources.NewDispatcherBuilder().WithArgs(&args).Build()
}

func makeDeploymentWithImageAndReplicas(image string, replicas int32) *appsv1.Deployment {
	return makeDeploymentWithParams(image, replicas, testConfigMapHash)
}

func makeDeploymentWithConfigMapHash(configMapHash string) *appsv1.Deployment {
	return makeDeploymentWithParams(testDispatcherImage, 1, configMapHash)
}

func makeDeployment() *appsv1.Deployment {
	return makeDeploymentWithImageAndReplicas(testDispatcherImage, 1)
}

func makeReadyDeployment() *appsv1.Deployment {
	d := makeDeployment()
	d.Status.Conditions = []appsv1.DeploymentCondition{{Type: appsv1.DeploymentAvailable, Status: corev1.ConditionTrue}}
	return d
}

func makeService() *corev1.Service {
	return resources.MakeDispatcherService(testNS)
}

func makeChannelService(nc *v1beta1.KafkaChannel) *corev1.Service {
	return &corev1.Service{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Service",
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: testNS,
			Name:      fmt.Sprintf("%s-kn-channel", kcName),
			Labels: map[string]string{
				resources.MessagingRoleLabel: resources.MessagingRole,
			},
			OwnerReferences: []metav1.OwnerReference{
				*kmeta.NewControllerRef(nc),
			},
		},
		Spec: corev1.ServiceSpec{
			Type:         corev1.ServiceTypeExternalName,
			ExternalName: network.GetServiceHostname(dispatcherName, testNS),
		},
	}
}

func makeChannelServiceNotOwnedByUs() *corev1.Service {
	return &corev1.Service{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Service",
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: testNS,
			Name:      fmt.Sprintf("%s-kn-channel", kcName),
			Labels: map[string]string{
				resources.MessagingRoleLabel: resources.MessagingRole,
			},
		},
		Spec: corev1.ServiceSpec{
			Type:         corev1.ServiceTypeExternalName,
			ExternalName: network.GetServiceHostname(dispatcherName, testNS),
		},
	}
}

func makeEmptyEndpoints() *corev1.Endpoints {
	return &corev1.Endpoints{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Endpoints",
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: testNS,
			Name:      dispatcherName,
		},
	}
}

func makeReadyEndpoints() *corev1.Endpoints {
	e := makeEmptyEndpoints()
	e.Subsets = []corev1.EndpointSubset{{Addresses: []corev1.EndpointAddress{{IP: "1.1.1.1"}}}}
	return e
}

func patchFinalizers(namespace, name string) clientgotesting.PatchActionImpl {
	action := clientgotesting.PatchActionImpl{}
	action.Name = name
	action.Namespace = namespace
	patch := `{"metadata":{"finalizers":["` + finalizerName + `"],"resourceVersion":""}}`
	action.Patch = []byte(patch)
	return action
}

func subscribers() []eventingduckv1.SubscriberSpec {

	return []eventingduckv1.SubscriberSpec{{
		UID:           sub1UID,
		Generation:    1,
		SubscriberURI: apis.HTTP("call1"),
		ReplyURI:      apis.HTTP("sink2"),
	}, {
		UID:           sub2UID,
		Generation:    2,
		SubscriberURI: apis.HTTP("call2"),
		ReplyURI:      apis.HTTP("sink2"),
	}}
}

type fakeStatusManager struct {
	FakeIsReady func(context.Context, v1beta1.KafkaChannel, eventingduckv1.SubscriberSpec) (bool, error)
}

func (m *fakeStatusManager) IsReady(ctx context.Context, ch v1beta1.KafkaChannel, sub eventingduckv1.SubscriberSpec) (bool, error) {
	return m.FakeIsReady(ctx, ch, sub)
}

func (m *fakeStatusManager) CancelProbing(sub eventingduckv1.SubscriberSpec) {
	//do nothing
}

func (m *fakeStatusManager) CancelPodProbing(pod corev1.Pod) {
	//do nothing
}

func makePatch(namespace, name, patch string) clientgotesting.PatchActionImpl {
	return clientgotesting.PatchActionImpl{
		ActionImpl: clientgotesting.ActionImpl{
			Namespace: namespace,
		},
		Name:  name,
		Patch: []byte(patch),
	}
}
