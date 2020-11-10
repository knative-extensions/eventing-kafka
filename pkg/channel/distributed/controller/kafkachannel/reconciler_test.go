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

package kafkachannel

import (
	"context"
	"sync"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/scheme"
	clientgotesting "k8s.io/client-go/testing"
	kafkav1beta1 "knative.dev/eventing-kafka/pkg/apis/messaging/v1beta1"
	kafkaadmin "knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/admin"
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/event"
	controllertesting "knative.dev/eventing-kafka/pkg/channel/distributed/controller/testing"
	fakekafkaclient "knative.dev/eventing-kafka/pkg/client/injection/client/fake"
	kafkachannelreconciler "knative.dev/eventing-kafka/pkg/client/injection/reconciler/messaging/v1beta1/kafkachannel"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"
	logtesting "knative.dev/pkg/logging/testing"
	. "knative.dev/pkg/reconciler/testing"
)

// Initialization - Add types to scheme
func init() {
	_ = kafkav1beta1.AddToScheme(scheme.Scheme)
	_ = duckv1.AddToScheme(scheme.Scheme)
}

// Test The Reconciler's SetKafkaAdminClient() Functionality
func TestSetKafkaAdminClient(t *testing.T) {

	// Test Data
	clientType := kafkaadmin.Kafka

	// Create A Test Logger
	logger := logtesting.TestLogger(t).Desugar()

	// Create A Couple Of Mock AdminClients
	mockAdminClient1 := &controllertesting.MockAdminClient{}
	mockAdminClient2 := &controllertesting.MockAdminClient{}

	// Mock The Creation Of Kafka ClusterAdmin
	newKafkaAdminClientWrapperPlaceholder := kafkaadmin.NewKafkaAdminClientWrapper
	kafkaadmin.NewKafkaAdminClientWrapper = func(ctx context.Context, saramaConfig *sarama.Config, clientId string, namespace string) (kafkaadmin.AdminClientInterface, error) {
		return mockAdminClient2, nil
	}
	defer func() {
		kafkaadmin.NewKafkaAdminClientWrapper = newKafkaAdminClientWrapperPlaceholder
	}()

	// Create A Reconciler To Test
	reconciler := &Reconciler{
		logger:          logger,
		adminClientType: clientType,
		adminClient:     mockAdminClient1,
	}

	// Perform The Test
	reconciler.SetKafkaAdminClient(context.TODO())

	// Verify Results
	assert.True(t, mockAdminClient1.CloseCalled())
	assert.NotNil(t, reconciler.adminClient)
	assert.Equal(t, mockAdminClient2, reconciler.adminClient)
}

// Test The Reconciler's ClearKafkaAdminClient() Functionality
func TestClearKafkaAdminClient(t *testing.T) {

	// Test Data
	clientType := kafkaadmin.Kafka

	// Create A Test Logger
	logger := logtesting.TestLogger(t).Desugar()

	// Create A Mock AdminClient
	mockAdminClient := &controllertesting.MockAdminClient{}

	// Create A Reconciler To Test
	reconciler := &Reconciler{
		logger:          logger,
		adminClientType: clientType,
		adminClient:     mockAdminClient,
	}

	// Perform The Test
	reconciler.ClearKafkaAdminClient()

	// Verify Results
	assert.True(t, mockAdminClient.CloseCalled())
}

// Test The Reconcile Functionality
func TestReconcile(t *testing.T) {

	//
	// Define The KafkaChannel Reconciler Test Cases
	//
	// Note - Knative testing framework assumes ALL actions will be in the same Namespace
	//        as the Key so we have to set SkipNamespaceValidation in all tests!
	//
	// Note - Knative reconciler framework expects Events (not errors) from ReconcileKind()
	//        so WantErr is only for higher level failures in the injected Reconcile() function.
	//
	tableTest := TableTest{

		//
		// Top Level Use Cases
		//

		{
			Name: "Bad KafkaChannel Key",
			Key:  "too/many/parts",
		},
		{
			Name: "KafkaChannel Key Not Found",
			Key:  "foo/not-found",
		},

		//
		// Full Reconciliation
		//

		{
			Name:                    "Complete Reconciliation Success",
			SkipNamespaceValidation: true,
			Key:                     controllertesting.KafkaChannelKey,
			Objects: []runtime.Object{
				controllertesting.NewKafkaChannel(controllertesting.WithInitializedConditions),
			},
			WantCreates: []runtime.Object{
				controllertesting.NewKafkaChannelService(),
				controllertesting.NewKafkaChannelDispatcherService(),
				controllertesting.NewKafkaChannelDispatcherDeployment(),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: controllertesting.NewKafkaChannel(
						controllertesting.WithAddress,
						controllertesting.WithInitializedConditions,
						controllertesting.WithKafkaChannelServiceReady,
						controllertesting.WithDispatcherDeploymentReady,
						controllertesting.WithTopicReady,
					),
				},
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				controllertesting.NewKafkaChannelLabelUpdate(
					controllertesting.NewKafkaChannel(
						controllertesting.WithFinalizer,
						controllertesting.WithMetaData,
						controllertesting.WithAddress,
						controllertesting.WithInitializedConditions,
						controllertesting.WithKafkaChannelServiceReady,
						controllertesting.WithDispatcherDeploymentReady,
						controllertesting.WithTopicReady,
					),
				),
			},
			WantPatches: []clientgotesting.PatchActionImpl{controllertesting.NewFinalizerPatchActionImpl()},
			WantEvents: []string{
				controllertesting.NewKafkaChannelFinalizerUpdateEvent(),
				controllertesting.NewKafkaChannelSuccessfulReconciliationEvent(),
			},
		},

		//
		// KafkaChannel Deletion (Finalizer)
		//

		{
			Name: "Finalize Deleted KafkaChannel With Dispatcher",
			Key:  controllertesting.KafkaChannelKey,
			Objects: []runtime.Object{
				controllertesting.NewKafkaChannel(
					controllertesting.WithInitializedConditions,
					controllertesting.WithLabels,
					controllertesting.WithDeletionTimestamp,
				),
				controllertesting.NewKafkaChannelDispatcherService(),
				controllertesting.NewKafkaChannelDispatcherDeployment(),
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				controllertesting.NewServiceUpdateActionImpl(controllertesting.NewKafkaChannelDispatcherService(controllertesting.WithoutFinalizersService)),
				controllertesting.NewDeploymentUpdateActionImpl(controllertesting.NewKafkaChannelDispatcherDeployment(controllertesting.WithoutFinalizersDeployment)),
			},
			WantDeletes: []clientgotesting.DeleteActionImpl{
				controllertesting.NewServiceDeleteActionImpl(controllertesting.NewKafkaChannelDispatcherService(controllertesting.WithoutFinalizersService)),
				controllertesting.NewDeploymentDeleteActionImpl(controllertesting.NewKafkaChannelDispatcherDeployment(controllertesting.WithoutFinalizersDeployment)),
			},
			WantEvents: []string{
				controllertesting.NewKafkaChannelSuccessfulFinalizedEvent(),
			},
		},
		{
			Name: "Finalize Deleted KafkaChannel Without Dispatcher",
			Key:  controllertesting.KafkaChannelKey,
			Objects: []runtime.Object{
				controllertesting.NewKafkaChannel(
					controllertesting.WithInitializedConditions,
					controllertesting.WithLabels,
					controllertesting.WithDeletionTimestamp,
				),
			},
			WantEvents: []string{
				controllertesting.NewKafkaChannelSuccessfulFinalizedEvent(),
			},
		},
		{
			Name:                    "Finalize Deleted KafkaChannel Errors(Delete)",
			SkipNamespaceValidation: true,
			Key:                     controllertesting.KafkaChannelKey,
			Objects: []runtime.Object{
				controllertesting.NewKafkaChannel(
					controllertesting.WithInitializedConditions,
					controllertesting.WithLabels,
					controllertesting.WithDeletionTimestamp,
				),
				controllertesting.NewKafkaChannelDispatcherService(),
				controllertesting.NewKafkaChannelDispatcherDeployment(),
			},
			WithReactors: []clientgotesting.ReactionFunc{
				InduceFailure("delete", "Services"),
				InduceFailure("delete", "Deployments"),
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				controllertesting.NewServiceUpdateActionImpl(controllertesting.NewKafkaChannelDispatcherService(controllertesting.WithoutFinalizersService)),
				controllertesting.NewDeploymentUpdateActionImpl(controllertesting.NewKafkaChannelDispatcherDeployment(controllertesting.WithoutFinalizersDeployment)),
			},
			WantDeletes: []clientgotesting.DeleteActionImpl{
				controllertesting.NewServiceDeleteActionImpl(controllertesting.NewKafkaChannelDispatcherService(controllertesting.WithoutFinalizersService)),
				controllertesting.NewDeploymentDeleteActionImpl(controllertesting.NewKafkaChannelDispatcherDeployment(controllertesting.WithoutFinalizersDeployment)),
			},
			WantErr: true,
			WantEvents: []string{
				Eventf(corev1.EventTypeWarning, event.DispatcherServiceFinalizationFailed.String(), "Failed To Finalize Dispatcher Service: inducing failure for delete services"),
				Eventf(corev1.EventTypeWarning, event.DispatcherDeploymentFinalizationFailed.String(), "Failed To Finalize Dispatcher Deployment: inducing failure for delete deployments"),
				controllertesting.NewKafkaChannelFailedFinalizationEvent(),
			},
		},

		//
		// KafkaChannel Service
		//

		{
			Name:                    "Reconcile Missing KafkaChannel Service Success",
			SkipNamespaceValidation: true,
			Key:                     controllertesting.KafkaChannelKey,
			Objects: []runtime.Object{
				controllertesting.NewKafkaChannel(
					controllertesting.WithFinalizer,
					controllertesting.WithMetaData,
					controllertesting.WithAddress,
					controllertesting.WithInitializedConditions,
					controllertesting.WithKafkaChannelServiceReady,
					controllertesting.WithDispatcherDeploymentReady,
					controllertesting.WithTopicReady,
				),
				controllertesting.NewKafkaChannelDispatcherService(),
				controllertesting.NewKafkaChannelDispatcherDeployment(),
			},
			WantCreates: []runtime.Object{controllertesting.NewKafkaChannelService()},
			WantEvents:  []string{controllertesting.NewKafkaChannelSuccessfulReconciliationEvent()},
		},
		{
			Name:                    "Reconcile Missing KafkaChannel Service Error(Create)",
			SkipNamespaceValidation: true,
			Key:                     controllertesting.KafkaChannelKey,
			Objects: []runtime.Object{
				controllertesting.NewKafkaChannel(
					controllertesting.WithFinalizer,
					controllertesting.WithAddress,
					controllertesting.WithInitializedConditions,
					controllertesting.WithKafkaChannelServiceReady,
					controllertesting.WithDispatcherDeploymentReady,
					controllertesting.WithTopicReady,
				),
				controllertesting.NewKafkaChannelDispatcherService(),
				controllertesting.NewKafkaChannelDispatcherDeployment(),
			},
			WithReactors: []clientgotesting.ReactionFunc{InduceFailure("create", "Services")},
			WantErr:      true,
			WantCreates:  []runtime.Object{controllertesting.NewKafkaChannelService()},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: controllertesting.NewKafkaChannel(
						controllertesting.WithFinalizer,
						controllertesting.WithAddress,
						controllertesting.WithInitializedConditions,
						controllertesting.WithKafkaChannelServiceFailed,
						controllertesting.WithDispatcherDeploymentReady,
						controllertesting.WithTopicReady,
					),
				},
			},
			WantEvents: []string{
				Eventf(corev1.EventTypeWarning, event.KafkaChannelServiceReconciliationFailed.String(), "Failed To Reconcile KafkaChannel Service: inducing failure for create services"),
				controllertesting.NewKafkaChannelFailedReconciliationEvent(),
			},
		},
		{
			Name:                    "Reconcile KafkaChannel Service With Deletion Timestamp",
			SkipNamespaceValidation: true,
			Key:                     controllertesting.KafkaChannelKey,
			Objects: []runtime.Object{
				controllertesting.NewKafkaChannel(
					controllertesting.WithFinalizer,
					controllertesting.WithMetaData,
					controllertesting.WithAddress,
					controllertesting.WithInitializedConditions,
					controllertesting.WithKafkaChannelServiceReady,
					controllertesting.WithDispatcherDeploymentReady,
					controllertesting.WithTopicReady,
				),
				controllertesting.NewKafkaChannelService(controllertesting.WithDeletionTimestampService),
				controllertesting.NewKafkaChannelReceiverService(),
				controllertesting.NewKafkaChannelReceiverDeployment(),
				controllertesting.NewKafkaChannelDispatcherService(),
				controllertesting.NewKafkaChannelDispatcherDeployment(),
			},
			WantErr: true,
			WantEvents: []string{
				Eventf(corev1.EventTypeWarning, event.KafkaChannelServiceReconciliationFailed.String(), "Failed To Reconcile KafkaChannel Service: encountered KafkaChannel Service with DeletionTimestamp kafkachannel-namespace/kafkachannel-name-kn-channel - potential race condition"),
				controllertesting.NewKafkaChannelFailedReconciliationEvent(),
			},
		},

		//
		// KafkaChannel Dispatcher Service
		//

		{
			Name:                    "Reconcile Missing Dispatcher Service Success",
			SkipNamespaceValidation: true,
			Key:                     controllertesting.KafkaChannelKey,
			Objects: []runtime.Object{
				controllertesting.NewKafkaChannel(
					controllertesting.WithFinalizer,
					controllertesting.WithMetaData,
					controllertesting.WithAddress,
					controllertesting.WithInitializedConditions,
					controllertesting.WithKafkaChannelServiceReady,
					controllertesting.WithReceiverServiceReady,
					controllertesting.WithReceiverDeploymentReady,
					controllertesting.WithDispatcherDeploymentReady,
					controllertesting.WithTopicReady,
				),
				controllertesting.NewKafkaChannelService(),
				controllertesting.NewKafkaChannelReceiverService(),
				controllertesting.NewKafkaChannelReceiverDeployment(),
				controllertesting.NewKafkaChannelDispatcherDeployment(),
			},
			WantCreates: []runtime.Object{controllertesting.NewKafkaChannelDispatcherService()},
			WantEvents:  []string{controllertesting.NewKafkaChannelSuccessfulReconciliationEvent()},
		},
		{
			Name:                    "Reconcile Missing Dispatcher Service Error(Create)",
			SkipNamespaceValidation: true,
			Key:                     controllertesting.KafkaChannelKey,
			Objects: []runtime.Object{
				controllertesting.NewKafkaChannel(
					controllertesting.WithFinalizer,
					controllertesting.WithMetaData,
					controllertesting.WithAddress,
					controllertesting.WithInitializedConditions,
					controllertesting.WithKafkaChannelServiceReady,
					controllertesting.WithReceiverServiceReady,
					controllertesting.WithReceiverDeploymentReady,
					controllertesting.WithDispatcherDeploymentReady,
					controllertesting.WithTopicReady,
				),
				controllertesting.NewKafkaChannelService(),
				controllertesting.NewKafkaChannelReceiverService(),
				controllertesting.NewKafkaChannelReceiverDeployment(),
				controllertesting.NewKafkaChannelDispatcherDeployment(),
			},
			WithReactors:      []clientgotesting.ReactionFunc{InduceFailure("create", "Services")},
			WantErr:           true,
			WantCreates:       []runtime.Object{controllertesting.NewKafkaChannelDispatcherService()},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				// Note - Not currently tracking status for the Dispatcher Service since it is only for Prometheus
			},
			WantEvents: []string{
				Eventf(corev1.EventTypeWarning, event.DispatcherServiceReconciliationFailed.String(), "Failed To Reconcile Dispatcher Service: inducing failure for create services"),
				controllertesting.NewKafkaChannelFailedReconciliationEvent(),
			},
		},
		{
			Name:                    "Reconcile Dispatcher Service With Deletion Timestamp And Finalizer",
			SkipNamespaceValidation: true,
			Key:                     controllertesting.KafkaChannelKey,
			Objects: []runtime.Object{
				controllertesting.NewKafkaChannel(
					controllertesting.WithFinalizer,
					controllertesting.WithMetaData,
					controllertesting.WithAddress,
					controllertesting.WithInitializedConditions,
					controllertesting.WithKafkaChannelServiceReady,
					controllertesting.WithDispatcherDeploymentReady,
					controllertesting.WithTopicReady,
				),
				controllertesting.NewKafkaChannelService(),
				controllertesting.NewKafkaChannelReceiverService(),
				controllertesting.NewKafkaChannelReceiverDeployment(),
				controllertesting.NewKafkaChannelDispatcherService(controllertesting.WithDeletionTimestampService),
				controllertesting.NewKafkaChannelDispatcherDeployment(),
			},
			WantErr: false,
			WantEvents: []string{
				controllertesting.NewKafkaChannelSuccessfulReconciliationEvent(),
			},
		},
		{
			Name:                    "Reconcile Dispatcher Service With Deletion Timestamp And Missing Finalizer",
			SkipNamespaceValidation: true,
			Key:                     controllertesting.KafkaChannelKey,
			Objects: []runtime.Object{
				controllertesting.NewKafkaChannel(
					controllertesting.WithFinalizer,
					controllertesting.WithMetaData,
					controllertesting.WithAddress,
					controllertesting.WithInitializedConditions,
					controllertesting.WithKafkaChannelServiceReady,
					controllertesting.WithDispatcherDeploymentReady,
					controllertesting.WithTopicReady,
				),
				controllertesting.NewKafkaChannelService(),
				controllertesting.NewKafkaChannelReceiverService(),
				controllertesting.NewKafkaChannelReceiverDeployment(),
				controllertesting.NewKafkaChannelDispatcherService(controllertesting.WithoutFinalizersService, controllertesting.WithDeletionTimestampService),
				controllertesting.NewKafkaChannelDispatcherDeployment(),
			},
			WantErr: false,
			WantEvents: []string{
				controllertesting.NewKafkaChannelSuccessfulReconciliationEvent(),
			},
		},

		//
		// KafkaChannel Dispatcher Deployment
		//

		{
			Name:                    "Reconcile Missing Dispatcher Deployment Success",
			SkipNamespaceValidation: true,
			Key:                     controllertesting.KafkaChannelKey,
			Objects: []runtime.Object{
				controllertesting.NewKafkaChannel(
					controllertesting.WithFinalizer,
					controllertesting.WithMetaData,
					controllertesting.WithAddress,
					controllertesting.WithInitializedConditions,
					controllertesting.WithKafkaChannelServiceReady,
					controllertesting.WithReceiverServiceReady,
					controllertesting.WithReceiverDeploymentReady,
					controllertesting.WithDispatcherDeploymentReady,
					controllertesting.WithTopicReady,
				),
				controllertesting.NewKafkaChannelService(),
				controllertesting.NewKafkaChannelReceiverService(),
				controllertesting.NewKafkaChannelReceiverDeployment(),
				controllertesting.NewKafkaChannelDispatcherService(),
			},
			WantCreates: []runtime.Object{controllertesting.NewKafkaChannelDispatcherDeployment()},
			WantEvents:  []string{controllertesting.NewKafkaChannelSuccessfulReconciliationEvent()},
		},
		{
			Name:                    "Reconcile Missing Dispatcher Deployment Error(Create)",
			SkipNamespaceValidation: true,
			Key:                     controllertesting.KafkaChannelKey,
			Objects: []runtime.Object{
				controllertesting.NewKafkaChannel(
					controllertesting.WithFinalizer,
					controllertesting.WithMetaData,
					controllertesting.WithAddress,
					controllertesting.WithInitializedConditions,
					controllertesting.WithKafkaChannelServiceReady,
					controllertesting.WithReceiverServiceReady,
					controllertesting.WithReceiverDeploymentReady,
					controllertesting.WithDispatcherDeploymentReady,
					controllertesting.WithTopicReady,
				),
				controllertesting.NewKafkaChannelService(),
				controllertesting.NewKafkaChannelReceiverService(),
				controllertesting.NewKafkaChannelReceiverDeployment(),
				controllertesting.NewKafkaChannelDispatcherService(),
			},
			WithReactors: []clientgotesting.ReactionFunc{InduceFailure("create", "Deployments")},
			WantErr:      true,
			WantCreates:  []runtime.Object{controllertesting.NewKafkaChannelDispatcherDeployment()},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: controllertesting.NewKafkaChannel(
						controllertesting.WithFinalizer,
						controllertesting.WithMetaData,
						controllertesting.WithAddress,
						controllertesting.WithInitializedConditions,
						controllertesting.WithKafkaChannelServiceReady,
						controllertesting.WithReceiverServiceReady,
						controllertesting.WithReceiverDeploymentReady,
						controllertesting.WithDispatcherFailed,
						controllertesting.WithTopicReady,
					),
				},
			},
			WantEvents: []string{
				Eventf(corev1.EventTypeWarning, event.DispatcherDeploymentReconciliationFailed.String(), "Failed To Reconcile Dispatcher Deployment: inducing failure for create deployments"),
				controllertesting.NewKafkaChannelFailedReconciliationEvent(),
			},
		},
		{
			Name:                    "Reconcile Dispatcher Deployment With Deletion Timestamp And Finalizer",
			SkipNamespaceValidation: true,
			Key:                     controllertesting.KafkaChannelKey,
			Objects: []runtime.Object{
				controllertesting.NewKafkaChannel(
					controllertesting.WithFinalizer,
					controllertesting.WithMetaData,
					controllertesting.WithAddress,
					controllertesting.WithInitializedConditions,
					controllertesting.WithKafkaChannelServiceReady,
					controllertesting.WithDispatcherDeploymentReady,
					controllertesting.WithTopicReady,
				),
				controllertesting.NewKafkaChannelService(),
				controllertesting.NewKafkaChannelReceiverService(),
				controllertesting.NewKafkaChannelReceiverDeployment(),
				controllertesting.NewKafkaChannelDispatcherService(),
				controllertesting.NewKafkaChannelDispatcherDeployment(controllertesting.WithDeletionTimestampDeployment),
			},
			WantErr: false,
			WantEvents: []string{
				controllertesting.NewKafkaChannelSuccessfulReconciliationEvent(),
			},
		},
		{
			Name:                    "Reconcile Dispatcher Deployment With Deletion Timestamp And Missing Finalizer",
			SkipNamespaceValidation: true,
			Key:                     controllertesting.KafkaChannelKey,
			Objects: []runtime.Object{
				controllertesting.NewKafkaChannel(
					controllertesting.WithFinalizer,
					controllertesting.WithMetaData,
					controllertesting.WithAddress,
					controllertesting.WithInitializedConditions,
					controllertesting.WithKafkaChannelServiceReady,
					controllertesting.WithDispatcherDeploymentReady,
					controllertesting.WithTopicReady,
				),
				controllertesting.NewKafkaChannelService(),
				controllertesting.NewKafkaChannelReceiverService(),
				controllertesting.NewKafkaChannelReceiverDeployment(),
				controllertesting.NewKafkaChannelDispatcherService(),
				controllertesting.NewKafkaChannelDispatcherDeployment(controllertesting.WithoutFinalizersDeployment, controllertesting.WithDeletionTimestampDeployment),
			},
			WantErr: false,
			WantEvents: []string{
				controllertesting.NewKafkaChannelSuccessfulReconciliationEvent(),
			},
		},
	}

	// Mock The Common Kafka AdminClient Creation For Test
	newKafkaAdminClientWrapperPlaceholder := kafkaadmin.NewKafkaAdminClientWrapper
	kafkaadmin.NewKafkaAdminClientWrapper = func(ctx context.Context, saramaConfig *sarama.Config, clientId string, namespace string) (kafkaadmin.AdminClientInterface, error) {
		return &controllertesting.MockAdminClient{}, nil
	}
	defer func() {
		kafkaadmin.NewKafkaAdminClientWrapper = newKafkaAdminClientWrapperPlaceholder
	}()

	// Run The TableTest Using The KafkaChannel Reconciler Provided By The Factory
	logger := logtesting.TestLogger(t)
	tableTest.Test(t, controllertesting.MakeFactory(func(ctx context.Context, listers *controllertesting.Listers, cmw configmap.Watcher) controller.Reconciler {
		r := &Reconciler{
			logger:               logging.FromContext(ctx).Desugar(),
			kubeClientset:        kubeclient.Get(ctx),
			adminClientType:      kafkaadmin.Kafka,
			adminClient:          nil,
			environment:          controllertesting.NewEnvironment(),
			config:               controllertesting.NewConfig(),
			kafkachannelLister:   listers.GetKafkaChannelLister(),
			kafkachannelInformer: nil,
			deploymentLister:     listers.GetDeploymentLister(),
			serviceLister:        listers.GetServiceLister(),
			kafkaClientSet:       fakekafkaclient.Get(ctx),
			adminMutex:           &sync.Mutex{},
		}
		return kafkachannelreconciler.NewReconciler(ctx, r.logger.Sugar(), r.kafkaClientSet, listers.GetKafkaChannelLister(), controller.GetEventRecorder(ctx), r)
	}, logger.Desugar()))
}
