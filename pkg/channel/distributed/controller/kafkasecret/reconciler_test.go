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

package kafkasecret

import (
	"context"
	"testing"

	apitypes "k8s.io/apimachinery/pkg/types"
	"knative.dev/pkg/apis/duck"

	corev1 "k8s.io/api/core/v1"
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/event"

	clientgotesting "k8s.io/client-go/testing"
	commontesting "knative.dev/eventing-kafka/pkg/common/testing"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/scheme"
	kafkav1beta1 "knative.dev/eventing-kafka/pkg/apis/messaging/v1beta1"
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/kafkasecretinjection"
	controllertesting "knative.dev/eventing-kafka/pkg/channel/distributed/controller/testing"
	fakekafkaclient "knative.dev/eventing-kafka/pkg/client/injection/client/fake"
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
			Name: "Bad Secret Key",
			Key:  "too/many/parts",
		},
		{
			Name: "Secret Key Not Found",
			Key:  "foo/not-found",
		},

		//
		// Full Reconciliation
		//

		{
			Name: "Complete Reconciliation Without KafkaChannel",
			Key:  controllertesting.KafkaSecretKey,
			Objects: []runtime.Object{
				controllertesting.NewKafkaSecret(),
			},
			WantCreates: []runtime.Object{
				controllertesting.NewKafkaChannelReceiverService(),
				controllertesting.NewKafkaChannelReceiverDeployment(),
			},
			WantPatches: []clientgotesting.PatchActionImpl{controllertesting.NewKafkaSecretFinalizerPatchActionImpl()},
			WantEvents: []string{
				controllertesting.NewKafkaSecretFinalizerUpdateEvent(),
				controllertesting.NewKafkaSecretSuccessfulReconciliationEvent(),
			},
		},
		{
			Name: "Complete Reconciliation With KafkaChannel",
			Key:  controllertesting.KafkaSecretKey,
			Objects: []runtime.Object{
				controllertesting.NewKafkaSecret(),
				controllertesting.NewKafkaChannel(),
			},
			WantCreates: []runtime.Object{
				controllertesting.NewKafkaChannelReceiverService(),
				controllertesting.NewKafkaChannelReceiverDeployment(),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: controllertesting.NewKafkaChannel(
						controllertesting.WithReceiverServiceReady,
						controllertesting.WithReceiverDeploymentReady,
					),
				},
			},
			WantPatches: []clientgotesting.PatchActionImpl{controllertesting.NewKafkaSecretFinalizerPatchActionImpl()},
			WantEvents: []string{
				controllertesting.NewKafkaSecretFinalizerUpdateEvent(),
				controllertesting.NewKafkaSecretSuccessfulReconciliationEvent(),
			},
		},
		{
			Name: "Complete Reconciliation With KafkaChannel, No Receiver Resource Requests Or Limits",
			Key:  controllertesting.KafkaSecretKey,
			Objects: []runtime.Object{
				controllertesting.NewKafkaSecret(),
				controllertesting.NewKafkaChannel(),
			},
			WantCreates: []runtime.Object{
				controllertesting.NewKafkaChannelReceiverService(),
				controllertesting.NewKafkaChannelReceiverDeployment(controllertesting.WithoutResources),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: controllertesting.NewKafkaChannel(
						controllertesting.WithReceiverServiceReady,
						controllertesting.WithReceiverDeploymentReady,
					),
				},
			},
			WantPatches: []clientgotesting.PatchActionImpl{controllertesting.NewKafkaSecretFinalizerPatchActionImpl()},
			WantEvents: []string{
				controllertesting.NewKafkaSecretFinalizerUpdateEvent(),
				controllertesting.NewKafkaSecretSuccessfulReconciliationEvent(),
			},
			OtherTestData: map[string]interface{}{
				"configOptions": []controllertesting.KafkaConfigOption{controllertesting.WithNoReceiverResources},
			},
		},

		//
		// KafkaChannel Secret Deletion (Finalizer)
		//

		{
			Name: "Finalize Deleted Secret",
			Key:  controllertesting.KafkaSecretKey,
			Objects: []runtime.Object{
				controllertesting.NewKafkaSecret(controllertesting.WithKafkaSecretDeleted),
				controllertesting.NewKafkaChannel(
					controllertesting.WithReceiverServiceReady,
					controllertesting.WithReceiverDeploymentReady,
				),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: controllertesting.NewKafkaChannel(
						controllertesting.WithReceiverServiceFinalized,
						controllertesting.WithReceiverDeploymentFinalized,
					),
				},
			},
			WantEvents: []string{
				controllertesting.NewKafkaSecretSuccessfulFinalizedEvent(),
			},
		},

		//
		// KafkaSecret Receiver Service
		//

		{
			Name: "Reconcile Missing Receiver Service Success",
			Key:  controllertesting.KafkaSecretKey,
			Objects: []runtime.Object{
				controllertesting.NewKafkaSecret(controllertesting.WithKafkaSecretFinalizer),
				controllertesting.NewKafkaChannel(
					controllertesting.WithReceiverServiceReady,
					controllertesting.WithReceiverDeploymentReady,
				),
				controllertesting.NewKafkaChannelReceiverDeployment(),
			},
			WantCreates: []runtime.Object{controllertesting.NewKafkaChannelReceiverService()},
			WantEvents: []string{
				controllertesting.NewKafkaSecretSuccessfulReconciliationEvent(),
			},
		},
		{
			Name: "Reconcile Missing Receiver Service Error(Create)",
			Key:  controllertesting.KafkaSecretKey,
			Objects: []runtime.Object{
				controllertesting.NewKafkaSecret(controllertesting.WithKafkaSecretFinalizer),
				controllertesting.NewKafkaChannel(
					controllertesting.WithReceiverServiceReady,
					controllertesting.WithReceiverDeploymentReady,
				),
				controllertesting.NewKafkaChannelService(),
				controllertesting.NewKafkaChannelReceiverDeployment(),
				controllertesting.NewKafkaChannelDispatcherService(),
				controllertesting.NewKafkaChannelDispatcherDeployment(),
			},
			WithReactors: []clientgotesting.ReactionFunc{InduceFailure("create", "services")},
			WantErr:      true,
			WantCreates:  []runtime.Object{controllertesting.NewKafkaChannelReceiverService()},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: controllertesting.NewKafkaChannel(
						controllertesting.WithReceiverServiceFailed,
						controllertesting.WithReceiverDeploymentReady,
					),
				},
			},
			WantEvents: []string{
				Eventf(corev1.EventTypeWarning, event.ReceiverServiceReconciliationFailed.String(), "Failed To Reconcile Receiver Service: inducing failure for create services"),
				controllertesting.NewKafkaSecretFailedReconciliationEvent(),
			},
		},
		{
			Name: "Reconcile Receiver Service With Deletion Timestamp",
			Key:  controllertesting.KafkaSecretKey,
			Objects: []runtime.Object{
				controllertesting.NewKafkaSecret(controllertesting.WithKafkaSecretFinalizer),
				controllertesting.NewKafkaChannelReceiverService(controllertesting.WithDeletionTimestampService),
				controllertesting.NewKafkaChannelReceiverDeployment(),
			},
			WantErr: true,
			WantEvents: []string{
				Eventf(corev1.EventTypeWarning, event.ReceiverServiceReconciliationFailed.String(), "Failed To Reconcile Receiver Service: encountered Receiver Service with DeletionTimestamp "+controllertesting.KafkaSecretNamespace+"/kafkasecret-name-b9176d5f-receiver - potential race condition"),
				controllertesting.NewKafkaSecretFailedReconciliationEvent(),
			},
		},

		//
		// KafkaSecret Receiver Deployment
		//

		{
			Name: "Reconcile Missing Receiver Deployment Success",
			Key:  controllertesting.KafkaSecretKey,
			Objects: []runtime.Object{
				controllertesting.NewKafkaSecret(controllertesting.WithKafkaSecretFinalizer),
				controllertesting.NewKafkaChannel(
					controllertesting.WithReceiverServiceReady,
					controllertesting.WithReceiverDeploymentReady,
				),
				controllertesting.NewKafkaChannelService(),
				controllertesting.NewKafkaChannelReceiverService(),
			},
			WantCreates: []runtime.Object{controllertesting.NewKafkaChannelReceiverDeployment()},
			WantEvents:  []string{controllertesting.NewKafkaSecretSuccessfulReconciliationEvent()},
		},
		{
			Name: "Reconcile Missing Receiver Deployment Error(Create)",
			Key:  controllertesting.KafkaSecretKey,
			Objects: []runtime.Object{
				controllertesting.NewKafkaSecret(controllertesting.WithKafkaSecretFinalizer),
				controllertesting.NewKafkaChannel(
					controllertesting.WithReceiverServiceReady,
					controllertesting.WithReceiverDeploymentReady,
				),
				controllertesting.NewKafkaChannelService(),
				controllertesting.NewKafkaChannelReceiverService(),
			},
			WithReactors: []clientgotesting.ReactionFunc{InduceFailure("create", "deployments")},
			WantErr:      true,
			WantCreates:  []runtime.Object{controllertesting.NewKafkaChannelReceiverDeployment()},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: controllertesting.NewKafkaChannel(
						controllertesting.WithReceiverServiceReady,
						controllertesting.WithReceiverDeploymentFailed,
					),
				},
			},
			WantEvents: []string{
				Eventf(corev1.EventTypeWarning, event.ReceiverDeploymentReconciliationFailed.String(), "Failed To Reconcile Receiver Deployment: inducing failure for create deployments"),
				controllertesting.NewKafkaSecretFailedReconciliationEvent(),
			},
		},
		{
			Name: "Reconcile Receiver Deployment With Deletion Timestamp",
			Key:  controllertesting.KafkaSecretKey,
			Objects: []runtime.Object{
				controllertesting.NewKafkaSecret(controllertesting.WithKafkaSecretFinalizer),
				controllertesting.NewKafkaChannelReceiverService(),
				controllertesting.NewKafkaChannelReceiverDeployment(controllertesting.WithDeletionTimestampDeployment),
			},
			WantErr: true,
			WantEvents: []string{
				Eventf(corev1.EventTypeWarning, event.ReceiverDeploymentReconciliationFailed.String(), "Failed To Reconcile Receiver Deployment: encountered Receiver Deployment with DeletionTimestamp "+controllertesting.KafkaSecretNamespace+"/kafkasecret-name-b9176d5f-receiver - potential race condition"),
				controllertesting.NewKafkaSecretFailedReconciliationEvent(),
			},
		},
		{
			Name: "Reconcile Receiver Deployment - Redeployment on ConfigMapHash change",
			Key:  controllertesting.KafkaSecretKey,
			Objects: []runtime.Object{
				controllertesting.NewKafkaSecret(controllertesting.WithKafkaSecretFinalizer),
				controllertesting.NewKafkaChannel(
					controllertesting.WithReceiverServiceReady,
					controllertesting.WithReceiverDeploymentReady,
				),
				controllertesting.NewKafkaChannelService(),
				controllertesting.NewKafkaChannelReceiverService(),
				controllertesting.NewKafkaChannelReceiverDeployment(controllertesting.WithConfigMapHash("initial-hash-to-be-overridden-by-controller")),
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				controllertesting.NewDeploymentUpdateActionImpl(controllertesting.NewKafkaChannelReceiverDeployment()),
			},
			WantEvents: []string{
				controllertesting.NewReceiverDeploymentUpdatedEvent(),
				controllertesting.NewKafkaSecretSuccessfulReconciliationEvent(),
			},
		},

		//
		// Deployment Updating - Repairing Incorrect Or Missing Fields In Existing Deployments
		//

		newReceiverUpdateTest("No Resources", controllertesting.WithoutResources),
		newReceiverUpdateTest("Different Name", controllertesting.WithDifferentName),
		newReceiverUpdateTest("Different Image", controllertesting.WithDifferentImage),
		newReceiverUpdateTest("Different Command", controllertesting.WithDifferentCommand),
		newReceiverUpdateTest("Different Args", controllertesting.WithDifferentArgs),
		newReceiverUpdateTest("Different WorkingDir", controllertesting.WithDifferentWorkingDir),
		newReceiverUpdateTest("Different Ports", controllertesting.WithDifferentPorts),
		newReceiverUpdateTest("Different Environment", controllertesting.WithMissingEnvironment),
		newReceiverUpdateTest("Different Environment", controllertesting.WithDifferentEnvironment),
		newReceiverUpdateTest("Different VolumeMounts", controllertesting.WithDifferentVolumeMounts),
		newReceiverUpdateTest("Different VolumeDevices", controllertesting.WithDifferentVolumeDevices),
		newReceiverUpdateTest("Different LivenessProbe", controllertesting.WithDifferentLivenessProbe),
		newReceiverUpdateTest("Different ReadinessProbe", controllertesting.WithDifferentReadinessProbe),
		newReceiverUpdateTest("Missing Labels", controllertesting.WithoutLabels),
		newReceiverUpdateTest("Missing Annotations", controllertesting.WithoutAnnotations),
		newReceiverNoUpdateTest("Different Lifecycle", controllertesting.WithDifferentLifecycle),
		newReceiverNoUpdateTest("Different TerminationPath", controllertesting.WithDifferentTerminationPath),
		newReceiverNoUpdateTest("Different TerminationPolicy", controllertesting.WithDifferentTerminationPolicy),
		newReceiverNoUpdateTest("Different ImagePullPolicy", controllertesting.WithDifferentImagePullPolicy),
		newReceiverNoUpdateTest("Different SecurityContext", controllertesting.WithDifferentSecurityContext),
		newReceiverNoUpdateTest("Different Replicas", controllertesting.WithDifferentReplicas),
		newReceiverNoUpdateTest("Extra Labels", controllertesting.WithExtraLabels),
		newReceiverNoUpdateTest("Extra Annotations", controllertesting.WithExtraAnnotations),

		//
		// Deployment Update Failure
		//

		{
			Name: "Existing Receiver Deployment, Different Image, Update Error",
			Key:  controllertesting.KafkaSecretKey,
			Objects: []runtime.Object{
				controllertesting.NewKafkaSecret(controllertesting.WithKafkaSecretFinalizer),
				controllertesting.NewKafkaChannel(),
				controllertesting.NewKafkaChannelService(),
				controllertesting.NewKafkaChannelReceiverService(),
				controllertesting.NewKafkaChannelReceiverDeployment(controllertesting.WithDifferentImage),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: controllertesting.NewKafkaChannel(
						controllertesting.WithReceiverServiceReady,
						controllertesting.WithReceiverDeploymentUpdateFailed,
					),
				},
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{{Object: controllertesting.NewKafkaChannelReceiverDeployment()}},
			WantEvents: []string{
				controllertesting.NewKafkaSecretReceiverDeploymentUpdateFailedEvent(),
				Eventf(corev1.EventTypeWarning, event.ReceiverDeploymentReconciliationFailed.String(), "Failed To Reconcile Receiver Deployment: inducing failure for update deployments"),
				controllertesting.NewKafkaSecretFailedReconciliationEvent(),
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

	// Run The TableTest Using The KafkaChannel Reconciler Provided By The Factory
	logger := logtesting.TestLogger(t)
	tableTest.Test(t, controllertesting.MakeFactory(func(ctx context.Context, listers *controllertesting.Listers, cmw configmap.Watcher, options map[string]interface{}) controller.Reconciler {

		configOptionsInt, ok := options["configOptions"]
		if !ok || configOptionsInt == nil {
			configOptionsInt = []controllertesting.KafkaConfigOption{}
		}
		configOptions := configOptionsInt.([]controllertesting.KafkaConfigOption)

		r := &Reconciler{
			kubeClientset:      kubeclient.Get(ctx),
			environment:        controllertesting.NewEnvironment(),
			config:             controllertesting.NewConfig(configOptions...),
			kafkaChannelClient: fakekafkaclient.Get(ctx),
			kafkachannelLister: listers.GetKafkaChannelLister(),
			deploymentLister:   listers.GetDeploymentLister(),
			serviceLister:      listers.GetServiceLister(),
			kafkaConfigMapHash: controllertesting.ConfigMapHash,
		}

		return kafkasecretinjection.NewReconciler(ctx, r.kubeClientset.CoreV1(), listers.GetSecretLister(), controller.GetEventRecorder(ctx), r)
	}, logger.Desugar()))
}

// Utility Functions

// Creates a test that expects a receiver deployment update, using the provided options
func newReceiverUpdateTest(name string, options ...controllertesting.DeploymentOption) TableRow {
	test := newReceiverBasicTest("Existing Receiver Deployment, " + name + ", Update Needed")
	test.Objects = append(test.Objects,
		controllertesting.NewKafkaChannelReceiverDeployment(options...),
		controllertesting.NewKafkaChannelReceiverService())
	test.WantUpdates = append(test.WantUpdates,
		controllertesting.NewDeploymentUpdateActionImpl(controllertesting.NewKafkaChannelReceiverDeployment()))
	test.WantEvents = append([]string{controllertesting.NewReceiverDeploymentUpdatedEvent()},
		test.WantEvents...)
	return test
}

// Creates a test that expects to not have a receiver deployment update, using the provided options
func newReceiverNoUpdateTest(name string, options ...controllertesting.DeploymentOption) TableRow {
	test := newReceiverBasicTest("Existing Receiver Deployment, " + name + ", No Update")
	test.Objects = append(test.Objects,
		controllertesting.NewKafkaChannelReceiverDeployment(options...),
		controllertesting.NewKafkaChannelReceiverService())
	return test
}

// Creates a test that expects a receiver service update, using the provided options
func newServicePatchTest(name string, options ...controllertesting.ServiceOption) TableRow {
	newService := controllertesting.NewKafkaChannelReceiverService()
	existingService := controllertesting.NewKafkaChannelReceiverService(options...)

	test := newReceiverBasicTest("Existing Receiver Service, " + name + ", Patch Needed")
	test.Objects = append(test.Objects, existingService,
		controllertesting.NewKafkaChannelReceiverDeployment())

	jsonPatch, _ := duck.CreatePatch(existingService, newService)
	patch, _ := jsonPatch.MarshalJSON()

	test.WantPatches = []clientgotesting.PatchActionImpl{{
		Name:      existingService.Name,
		PatchType: apitypes.JSONPatchType,
		Patch:     patch,
	}}

	test.WantEvents = append([]string{controllertesting.NewReceiverServicePatchedEvent()},
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
	test.Name = "Existing Receiver Service, " + name + ", Patch Error"

	test.WantEvents = []string{
		controllertesting.NewReceiverServicePatchFailedEvent(),
		Eventf(corev1.EventTypeWarning, event.ReceiverServiceReconciliationFailed.String(), "Failed To Reconcile Receiver Service: inducing failure for patch services"),
		controllertesting.NewKafkaChannelFailedReconciliationEvent(),
	}

	test.WantStatusUpdates = []clientgotesting.UpdateActionImpl{{
		Object: controllertesting.NewKafkaChannel(
			controllertesting.WithReceiverDeploymentReady,
			controllertesting.WithReceiverServicePatchFailed,
		),
	}}

	// If the service fails, the other reconcilers are not executed, so no updates
	test.WantUpdates = nil

	test.WithReactors = []clientgotesting.ReactionFunc{InduceFailure("patch", "Services")}
	test.WantErr = true

	return test
}

// Creates a test that expects to not have a receiver service patch, using the provided options
func newServiceNoPatchTest(name string, options ...controllertesting.ServiceOption) TableRow {
	test := newReceiverBasicTest("Existing Receiver Service, " + name + ", No Patch")
	test.Objects = append(test.Objects,
		controllertesting.NewKafkaChannelReceiverService(options...),
		controllertesting.NewKafkaChannelReceiverDeployment())
	return test
}

// Creates a test that can serve as a common base for other receiver deployment tests
func newReceiverBasicTest(name string) TableRow {
	return TableRow{
		Name: name,
		Key:  controllertesting.KafkaSecretKey,
		Objects: []runtime.Object{
			controllertesting.NewKafkaSecret(controllertesting.WithKafkaSecretFinalizer),
			controllertesting.NewKafkaChannel(),
			controllertesting.NewKafkaChannelService(),
		},
		WantStatusUpdates: []clientgotesting.UpdateActionImpl{
			{
				Object: controllertesting.NewKafkaChannel(
					controllertesting.WithReceiverServiceReady,
					controllertesting.WithReceiverDeploymentReady,
				),
			},
		},
		WantEvents: []string{controllertesting.NewKafkaSecretSuccessfulReconciliationEvent()},
	}
}
