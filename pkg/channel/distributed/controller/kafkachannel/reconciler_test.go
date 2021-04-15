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
	"errors"
	"sync"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apitypes "k8s.io/apimachinery/pkg/types"
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/event"
	"knative.dev/pkg/apis/duck"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/scheme"
	clientgotesting "k8s.io/client-go/testing"
	kafkav1beta1 "knative.dev/eventing-kafka/pkg/apis/messaging/v1beta1"
	kafkaadmintesting "knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/admin/testing"
	"knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/admin/types"
	controllertesting "knative.dev/eventing-kafka/pkg/channel/distributed/controller/testing"
	fakekafkaclient "knative.dev/eventing-kafka/pkg/client/injection/client/fake"
	kafkachannelreconciler "knative.dev/eventing-kafka/pkg/client/injection/reconciler/messaging/v1beta1/kafkachannel"
	commontesting "knative.dev/eventing-kafka/pkg/common/testing"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"
	logtesting "knative.dev/pkg/logging/testing"
	. "knative.dev/pkg/reconciler/testing"
)

// Initialization - Add types to scheme
func init() {
	_ = kafkav1beta1.AddToScheme(scheme.Scheme)
	_ = duckv1.AddToScheme(scheme.Scheme)
}

// Test SetKafkaAdminClient() Functionality - Success Case
func TestSetKafkaAdminClient(t *testing.T) {

	// Create A Couple Of Mock AdminClients
	mockAdminClient1 := &controllertesting.MockAdminClient{}
	mockAdminClient2 := &controllertesting.MockAdminClient{}

	// Stub The Creation Of AdminClient
	kafkaadmintesting.StubNewAdminClientFn(kafkaadmintesting.NonValidatingNewAdminClientFn(mockAdminClient2))
	defer kafkaadmintesting.RestoreNewAdminClientFn()

	// Create A Reconciler To Test
	reconciler := &Reconciler{adminClient: mockAdminClient1}

	// Perform The Test
	reconciler.SetKafkaAdminClient(context.TODO())

	// Verify Results
	assert.True(t, mockAdminClient1.CloseCalled())
	assert.NotNil(t, reconciler.adminClient)
	assert.Equal(t, mockAdminClient2, reconciler.adminClient)
}

// Test SetKafkaAdminClient() Functionality - Error Case
func TestSetKafkaAdminClientError(t *testing.T) {

	// Test Data
	err := errors.New("test error - new")

	// Create A Mock AdminClient
	mockAdminClient1 := &controllertesting.MockAdminClient{}

	// Stub The Creation Of AdminClient
	kafkaadmintesting.StubNewAdminClientFn(kafkaadmintesting.ErrorNewAdminClientFn(err))
	defer kafkaadmintesting.RestoreNewAdminClientFn()

	// Create A Reconciler To Test
	reconciler := &Reconciler{adminClient: mockAdminClient1}

	// Perform The Test
	reconciler.SetKafkaAdminClient(context.TODO())

	// Verify Results
	assert.Nil(t, reconciler.adminClient)
}

// Test ClearKafkaAdminClient() Functionality - Success Case
func TestClearKafkaAdminClient(t *testing.T) {

	// Create A Mock AdminClient
	mockAdminClient := &controllertesting.MockAdminClient{}

	// Create A Reconciler To Test
	reconciler := &Reconciler{adminClient: mockAdminClient}

	// Perform The Test
	reconciler.ClearKafkaAdminClient(context.TODO())

	// Verify Results
	assert.True(t, mockAdminClient.CloseCalled())
	assert.Nil(t, reconciler.adminClient)
}

// Test ClearKafkaAdminClient() Functionality - Error Case
func TestClearKafkaAdminClientError(t *testing.T) {

	// Test Data
	err := errors.New("test error - close")

	// Create A Mock AdminClient
	mockAdminClient := &controllertesting.MockAdminClient{}
	mockAdminClient.MockCloseFunc = func() error { return err }

	// Create A Reconciler To Test
	reconciler := &Reconciler{adminClient: mockAdminClient}

	// Perform The Test
	reconciler.ClearKafkaAdminClient(context.TODO())

	// Verify Results
	assert.True(t, mockAdminClient.CloseCalled())
	assert.Nil(t, reconciler.adminClient)
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
	commontesting.SetTestEnvironment(t)
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
		{
			Name:                    "Complete Reconciliation Success, No Dispatcher Resource Requests Or Limits",
			SkipNamespaceValidation: true,
			Key:                     controllertesting.KafkaChannelKey,
			Objects: []runtime.Object{
				controllertesting.NewKafkaChannel(controllertesting.WithInitializedConditions),
			},
			WantCreates: []runtime.Object{
				controllertesting.NewKafkaChannelService(),
				controllertesting.NewKafkaChannelDispatcherService(),
				controllertesting.NewKafkaChannelDispatcherDeployment(controllertesting.WithoutResources),
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
			OtherTestData: map[string]interface{}{
				"configOptions": []controllertesting.KafkaConfigOption{controllertesting.WithNoDispatcherResources},
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

		//
		// Deployment Updating - Repairing Incorrect Or Missing Fields In Existing Deployments
		//

		newDispatcherUpdateTest("No Resources", controllertesting.WithoutResources),
		newDispatcherUpdateTest("Different Name", controllertesting.WithDifferentName),
		newDispatcherUpdateTest("Different Image", controllertesting.WithDifferentImage),
		newDispatcherUpdateTest("Different Command", controllertesting.WithDifferentCommand),
		newDispatcherUpdateTest("Different Args", controllertesting.WithDifferentArgs),
		newDispatcherUpdateTest("Different WorkingDir", controllertesting.WithDifferentWorkingDir),
		newDispatcherUpdateTest("Different Ports", controllertesting.WithDifferentPorts),
		newDispatcherUpdateTest("Different Environment", controllertesting.WithMissingEnvironment),
		newDispatcherUpdateTest("Different Environment", controllertesting.WithDifferentEnvironment),
		newDispatcherUpdateTest("Different VolumeMounts", controllertesting.WithDifferentVolumeMounts),
		newDispatcherUpdateTest("Different VolumeDevices", controllertesting.WithDifferentVolumeDevices),
		newDispatcherUpdateTest("Different LivenessProbe", controllertesting.WithDifferentLivenessProbe),
		newDispatcherUpdateTest("Different ReadinessProbe", controllertesting.WithDifferentReadinessProbe),
		newDispatcherUpdateTest("Missing Labels", controllertesting.WithoutLabels),
		newDispatcherNoUpdateTest("Missing Annotations", controllertesting.WithoutAnnotations), // TODO: When configmap hash is implemented this should be an Update
		newDispatcherNoUpdateTest("Different Lifecycle", controllertesting.WithDifferentLifecycle),
		newDispatcherNoUpdateTest("Different TerminationPath", controllertesting.WithDifferentTerminationPath),
		newDispatcherNoUpdateTest("Different TerminationPolicy", controllertesting.WithDifferentTerminationPolicy),
		newDispatcherNoUpdateTest("Different ImagePullPolicy", controllertesting.WithDifferentImagePullPolicy),
		newDispatcherNoUpdateTest("Different SecurityContext", controllertesting.WithDifferentSecurityContext),
		newDispatcherNoUpdateTest("Different Replicas", controllertesting.WithDifferentReplicas),
		newDispatcherNoUpdateTest("Extra Labels", controllertesting.WithExtraLabels),
		newDispatcherNoUpdateTest("Extra Annotations", controllertesting.WithExtraAnnotations),

		//
		// Deployment Update Failure
		//

		{
			Name: "Existing Dispatcher Deployment, Different Image, Update Error",
			Key:  controllertesting.KafkaChannelKey,
			Objects: []runtime.Object{
				controllertesting.NewKafkaChannel(controllertesting.WithFinalizer),
				controllertesting.NewKafkaChannelService(),
				controllertesting.NewKafkaChannelDispatcherService(),
				controllertesting.NewKafkaChannelDispatcherDeployment(controllertesting.WithDifferentImage),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: controllertesting.NewKafkaChannel(
						controllertesting.WithFinalizer,
						controllertesting.WithAddress,
						controllertesting.WithInitializedConditions,
						controllertesting.WithKafkaChannelServiceReady,
						controllertesting.WithDispatcherUpdateFailed,
						controllertesting.WithTopicReady,
					),
				},
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{{Object: controllertesting.NewKafkaChannelDispatcherDeployment()}},
			WantEvents: []string{
				controllertesting.NewKafkaChannelDispatcherDeploymentUpdateFailedEvent(),
				Eventf(corev1.EventTypeWarning, event.DispatcherDeploymentReconciliationFailed.String(), "Failed To Reconcile Dispatcher Deployment: inducing failure for update deployments"),
				controllertesting.NewKafkaChannelFailedReconciliationEvent(),
			},
			WithReactors: []clientgotesting.ReactionFunc{
				InduceFailure("update", "Deployments"),
			},
			WantErr: true,
		},

		//
		// Service Patching - Repairing Incorrect Or Missing Fields In Existing Services
		//

		newServicePatchTest("Missing Ports", controllertesting.WithoutServicePorts),
		newServicePatchTest("Missing App Label Selector", controllertesting.WithoutServiceSelector),
		newServicePatchTest("Missing Labels", controllertesting.WithoutServiceLabels),
		newServiceNoPatchTest("Extra Labels", controllertesting.WithExtraServiceLabels),
		newServiceNoPatchTest("Different Status", controllertesting.WithDifferentServiceStatus),

		//
		// Service Patch Failure
		//

		newServicePatchFailureTest("Missing Ports", controllertesting.WithoutServicePorts),
		newServicePatchFailureTest("Missing Labels", controllertesting.WithoutServiceLabels),
	}

	// Create A Mock AdminClient
	mockAdminClient := &controllertesting.MockAdminClient{}

	// Stub The Creation Of AdminClient
	kafkaadmintesting.StubNewAdminClientFn(kafkaadmintesting.NonValidatingNewAdminClientFn(mockAdminClient))
	defer kafkaadmintesting.RestoreNewAdminClientFn()

	// Run The TableTest Using The KafkaChannel Reconciler Provided By The Factory
	logger := logtesting.TestLogger(t)
	tableTest.Test(t, controllertesting.MakeFactory(func(ctx context.Context, listers *controllertesting.Listers, cmw configmap.Watcher, configOptions []controllertesting.KafkaConfigOption) controller.Reconciler {
		r := &Reconciler{
			kubeClientset:        kubeclient.Get(ctx),
			adminClientType:      types.Kafka,
			adminClient:          nil,
			environment:          controllertesting.NewEnvironment(),
			config:               controllertesting.NewConfig(configOptions...),
			kafkachannelLister:   listers.GetKafkaChannelLister(),
			kafkachannelInformer: nil,
			deploymentLister:     listers.GetDeploymentLister(),
			serviceLister:        listers.GetServiceLister(),
			kafkaClientSet:       fakekafkaclient.Get(ctx),
			adminMutex:           &sync.Mutex{},
			kafkaBrokers:         controllertesting.KafkaSecretDataValueBrokers,
			kafkaSecret:          controllertesting.KafkaSecretName,
			kafkaUsername:        controllertesting.KafkaSecretDataValueUsername,
			kafkaPassword:        controllertesting.KafkaSecretDataValuePassword,
			kafkaSaslType:        controllertesting.KafkaSecretDataValueSaslType,
		}
		return kafkachannelreconciler.NewReconciler(ctx, logger, r.kafkaClientSet, listers.GetKafkaChannelLister(), controller.GetEventRecorder(ctx), r)
	}, logger.Desugar()))
}

// Utility Functions

// Creates a test that expects a dispatcher deployment update, using the provided options
func newDispatcherUpdateTest(name string, options ...controllertesting.DeploymentOption) TableRow {
	test := newDispatcherBasicTest("Existing Dispatcher Deployment, " + name + ", Update Needed")
	test.Objects = append(test.Objects,
		controllertesting.NewKafkaChannelDispatcherService(),
		controllertesting.NewKafkaChannelDispatcherDeployment(options...))
	test.WantUpdates = append(test.WantUpdates,
		controllertesting.NewDeploymentUpdateActionImpl(controllertesting.NewKafkaChannelDispatcherDeployment()))
	test.WantEvents = append([]string{controllertesting.NewKafkaChannelDispatcherDeploymentUpdatedEvent()},
		test.WantEvents...)
	return test
}

// Creates a test that expects to not have a dispatcher deployment update, using the provided options
func newDispatcherNoUpdateTest(name string, options ...controllertesting.DeploymentOption) TableRow {
	test := newDispatcherBasicTest("Existing Dispatcher Deployment, " + name + ", No Update")
	test.Objects = append(test.Objects,
		controllertesting.NewKafkaChannelDispatcherService(),
		controllertesting.NewKafkaChannelDispatcherDeployment(options...))
	return test
}

// Creates a test that expects a dispatcher service update, using the provided options
func newServicePatchTest(name string, options ...controllertesting.ServiceOption) TableRow {
	newService := controllertesting.NewKafkaChannelDispatcherService()
	existingService := controllertesting.NewKafkaChannelDispatcherService(options...)

	test := newDispatcherBasicTest("Existing Dispatcher Service, " + name + ", Patch Needed")
	test.Objects = append(test.Objects, existingService,
		controllertesting.NewKafkaChannelDispatcherDeployment())

	jsonPatch, _ := duck.CreatePatch(existingService, newService)
	patch, _ := jsonPatch.MarshalJSON()

	test.WantPatches = []clientgotesting.PatchActionImpl{{
		Name:      existingService.Name,
		PatchType: apitypes.JSONPatchType,
		Patch:     patch,
	}}

	test.WantEvents = append([]string{controllertesting.NewKafkaChannelDispatcherServicePatchedEvent()},
		test.WantEvents...)

	// The "WantPatches" part of the table test assumes that a patch is supposed to be for the namespace
	// given by the "Key" field, which, in this case, is the namespace for the KafkaChannel.  This assumption
	// is not correct, so that validation must be skipped here (this is true for the Update commands as well
	// but the table test code does not verify that updates are done in the same namespace for some reason).
	test.SkipNamespaceValidation = true
	return test
}

func newServicePatchFailureTest(name string, options ...controllertesting.ServiceOption) TableRow {
	test := newServicePatchTest(name, options...)
	test.Name = "Existing Dispatcher Service, " + name + ", Patch Error"

	test.WantEvents = []string{
		controllertesting.NewKafkaChannelDispatcherServicePatchFailedEvent(),
		Eventf(corev1.EventTypeWarning, event.DispatcherServiceReconciliationFailed.String(), "Failed To Reconcile Dispatcher Service: inducing failure for patch services"),
		controllertesting.NewKafkaChannelFailedReconciliationEvent(),
	}

	test.WantStatusUpdates = []clientgotesting.UpdateActionImpl{{
		Object: controllertesting.NewKafkaChannel(
			controllertesting.WithFinalizer,
			controllertesting.WithAddress,
			controllertesting.WithInitializedConditions,
			controllertesting.WithKafkaChannelServiceReady,
			controllertesting.WithDispatcherDeploymentReady,
			controllertesting.WithDispatcherServicePatchFailed,
			controllertesting.WithTopicReady,
		),
	}}

	// If the service fails, the other reconcilers are not executed, so no updates
	test.WantUpdates = nil

	test.WithReactors = []clientgotesting.ReactionFunc{InduceFailure("patch", "Services")}
	test.WantErr = true

	return test
}

// Creates a test that expects to not have a dispatcher service patch, using the provided options
func newServiceNoPatchTest(name string, options ...controllertesting.ServiceOption) TableRow {
	test := newDispatcherBasicTest("Existing Dispatcher Service, " + name + ", No Patch")
	test.Objects = append(test.Objects,
		controllertesting.NewKafkaChannelDispatcherService(options...),
		controllertesting.NewKafkaChannelDispatcherDeployment())
	return test
}

// Creates a test that can serve as a common base for other dispatcher tests
func newDispatcherBasicTest(name string) TableRow {
	return TableRow{
		Name: name,
		Key:  controllertesting.KafkaChannelKey,
		Objects: []runtime.Object{
			controllertesting.NewKafkaChannel(controllertesting.WithFinalizer),
			controllertesting.NewKafkaChannelService(),
		},
		WantStatusUpdates: []clientgotesting.UpdateActionImpl{{
			Object: controllertesting.NewKafkaChannel(
				controllertesting.WithFinalizer,
				controllertesting.WithAddress,
				controllertesting.WithInitializedConditions,
				controllertesting.WithKafkaChannelServiceReady,
				controllertesting.WithDispatcherDeploymentReady,
				controllertesting.WithTopicReady,
			),
		}},
		WantUpdates: []clientgotesting.UpdateActionImpl{{
			Object: controllertesting.NewKafkaChannel(
				controllertesting.WithFinalizer,
				controllertesting.WithMetaData,
				controllertesting.WithAddress,
				controllertesting.WithInitializedConditions,
				controllertesting.WithKafkaChannelServiceReady,
				controllertesting.WithDispatcherDeploymentReady,
				controllertesting.WithTopicReady,
			),
		}},
		WantEvents: []string{controllertesting.NewKafkaChannelSuccessfulReconciliationEvent()},
	}
}
