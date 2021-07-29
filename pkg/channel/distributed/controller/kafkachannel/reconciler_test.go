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
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	apitypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	clientgotesting "k8s.io/client-go/testing"
	"knative.dev/pkg/apis/duck"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"
	logtesting "knative.dev/pkg/logging/testing"
	. "knative.dev/pkg/reconciler/testing"
	"knative.dev/pkg/system"

	kafkav1beta1 "knative.dev/eventing-kafka/pkg/apis/messaging/v1beta1"
	kafkaadmintesting "knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/admin/testing"
	"knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/admin/types"
	kafkaadminwrapper "knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/admin/wrapper"
	kafkautil "knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/util"
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/event"
	controllertesting "knative.dev/eventing-kafka/pkg/channel/distributed/controller/testing"
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/util"
	fakekafkaclient "knative.dev/eventing-kafka/pkg/client/injection/client/fake"
	kafkachannelreconciler "knative.dev/eventing-kafka/pkg/client/injection/reconciler/messaging/v1beta1/kafkachannel"
	commonconfig "knative.dev/eventing-kafka/pkg/common/config"
	"knative.dev/eventing-kafka/pkg/common/constants"
	commontesting "knative.dev/eventing-kafka/pkg/common/testing"
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
	reconciler := &Reconciler{config: &commonconfig.EventingKafkaConfig{}, adminClient: mockAdminClient1}

	// Perform The Test
	resultErr := reconciler.SetKafkaAdminClient(context.TODO())

	// Verify Results
	assert.Nil(t, resultErr)
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
	reconciler := &Reconciler{config: &commonconfig.EventingKafkaConfig{}, adminClient: mockAdminClient1}

	// Perform The Test
	resultErr := reconciler.SetKafkaAdminClient(context.TODO())

	// Verify Results
	assert.NotNil(t, resultErr)
	assert.Nil(t, reconciler.adminClient)
}

// Test ClearKafkaAdminClient() Functionality - Success Case
func TestClearKafkaAdminClient(t *testing.T) {

	// Create A Mock AdminClient
	mockAdminClient := &controllertesting.MockAdminClient{}

	// Create A Reconciler To Test
	reconciler := &Reconciler{config: &commonconfig.EventingKafkaConfig{}, adminClient: mockAdminClient}

	// Perform The Test
	resultErr := reconciler.ClearKafkaAdminClient(context.TODO())

	// Verify Results
	assert.Nil(t, resultErr)
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
	reconciler := &Reconciler{config: &commonconfig.EventingKafkaConfig{}, adminClient: mockAdminClient}

	// Perform The Test
	resultErr := reconciler.ClearKafkaAdminClient(context.TODO())

	// Verify Results
	assert.NotNil(t, resultErr)
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
		// Stable System (All Objects Created And Ready - No Changes Necessary)
		//

		newStableSystemTest("Stable System - No Changes"),

		//
		// Full Reconciliation (Creates All Necessary Objects)
		//

		newReconciliationTest("Complete Reconciliation Success"),

		newReconciliationTest("Complete Reconciliation Success, No Dispatcher Resource Requests Or Limits",
			wantCreateDispatcherDeployment(controllertesting.WithoutResources),
			withConfig(controllertesting.WithNoDispatcherResources)),

		newReconciliationTest("Complete Reconciliation With KafkaChannel And Dispatcher, No Receiver Resource Requests Or Limits",
			wantCreateReceiverDeployment(controllertesting.WithoutResources),
			withConfig(controllertesting.WithNoReceiverResources)),

		//
		// KafkaChannel Secret Missing (Deletes Receiver Service And/Or Deployment, If Present)
		//

		newDeletedSecretTest("Receiver Service And Deployment", wantDeleteReceiverService, wantDeleteReceiverDeployment),
		newDeletedSecretTest("Receiver Deployment", withoutReceiverService, wantDeleteReceiverDeployment),
		newDeletedSecretTest("Receiver Service", withoutReceiverDeployment, wantDeleteReceiverService),
		newDeletedSecretTest("No Receiver Service Or Deployment", withoutReceiverService, withoutReceiverDeployment),

		//
		// KafkaChannel Deletion (Finalizer)
		//

		newDeletedKafkaChannelTest("With Dispatcher And Receiver"),
		newDeletedKafkaChannelTest("Without Dispatcher", withoutDispatcher, withoutUpdates, withoutDeletes),
		newDeletedKafkaChannelTest("Without Receiver", withoutReceiver),

		newDeletedKafkaChannelTest("With Delete Errors", withFinalEventAndFailures(
			controllertesting.NewKafkaChannelFailedFinalizationEvent(),
			failure{"delete", "Services", event.DispatcherServiceFinalizationFailed.String(), "Failed To Finalize Dispatcher Service"},
			failure{"delete", "Deployments", event.DispatcherDeploymentFinalizationFailed.String(), "Failed To Finalize Dispatcher Deployment"})),

		//
		// KafkaChannel Service
		//

		newStableSystemTest("Reconcile Missing KafkaChannel Service Success", withoutKafkaChannelService, wantCreateKafkaChannelService),

		newStableSystemTest("Reconcile Missing KafkaChannel Service Error(Create)", withoutKafkaChannelService, wantCreateKafkaChannelService,
			replaceStatusUpdates(getReadyKafkaChannel(controllertesting.WithKafkaChannelServiceFailed)),
			withFinalEventAndFailures(
				controllertesting.NewKafkaChannelFailedReconciliationEvent(),
				failure{"create", "Services", event.KafkaChannelServiceReconciliationFailed.String(), "Failed To Reconcile KafkaChannel Service"})),

		newStableSystemTest("Reconcile KafkaChannel Service With Deletion Timestamp",
			withKafkaChannelService(controllertesting.WithDeletionTimestampService),
			withErrorEvent(controllertesting.NewKafkaChannelFailedReconciliationEvent(),
				event.KafkaChannelServiceReconciliationFailed.String(),
				"Failed To Reconcile KafkaChannel Service: encountered KafkaChannel Service with DeletionTimestamp kafkachannel-namespace/kafkachannel-name-kn-channel - potential race condition")),

		//
		// KafkaChannel Dispatcher Service
		//

		newStableSystemTest("Reconcile Missing Dispatcher Service Success", withoutDispatcherService, wantCreateDispatcherService()),

		newStableSystemTest("Reconcile Missing Dispatcher Service Error(Create)", withoutDispatcherService, wantCreateDispatcherService(),
			// Note - Not currently tracking status for the Dispatcher Service since it is only for Prometheus
			withFinalEventAndFailures(
				controllertesting.NewKafkaChannelFailedReconciliationEvent(),
				failure{"create", "Services", event.DispatcherServiceReconciliationFailed.String(), "Failed To Reconcile Dispatcher Service"})),

		newStableSystemTest("Reconcile Missing Dispatcher Service Success", withoutDispatcherService, wantCreateDispatcherService()),

		newStableSystemTest("Reconcile Dispatcher Service With Deletion Timestamp And Finalizer",
			withDispatcherService(controllertesting.WithDeletionTimestampService)),

		newStableSystemTest("Reconcile Dispatcher Service With Deletion Timestamp And Missing Finalizer",
			withDispatcherService(controllertesting.WithoutFinalizersService, controllertesting.WithDeletionTimestampService)),

		//
		// KafkaChannel Dispatcher Deployment
		//

		newStableSystemTest("Reconcile Missing Dispatcher Deployment Success", withoutDispatcherDeployment, wantCreateDispatcherDeployment()),

		newStableSystemTest("Reconcile Missing Dispatcher Deployment Error(Create)", withoutDispatcherDeployment, wantCreateDispatcherDeployment(),
			replaceStatusUpdates(getReadyKafkaChannel(controllertesting.WithDispatcherFailed)),
			withFinalEventAndFailures(
				controllertesting.NewKafkaChannelFailedReconciliationEvent(),
				failure{"create", "Deployments", event.DispatcherDeploymentReconciliationFailed.String(), "Failed To Reconcile Dispatcher Deployment"})),

		newStableSystemTest("Reconcile Dispatcher Deployment With Deletion Timestamp And Finalizer",
			withDispatcherDeployment(controllertesting.WithDeletionTimestampDeployment)),

		newStableSystemTest("Reconcile Dispatcher Deployment With Deletion Timestamp And Missing Finalizer",
			withDispatcherDeployment(controllertesting.WithoutFinalizersDeployment, controllertesting.WithDeletionTimestampDeployment)),

		//
		// KafkaChannel Receiver Service
		//

		newStableSystemTest("Reconcile Missing Receiver Service Success", withoutReceiverService, wantCreateReceiverService()),

		newStableSystemTest("Reconcile Missing Receiver Service Error(Create)", withoutReceiverService, wantCreateReceiverService(),
			replaceStatusUpdates(getReadyKafkaChannel(controllertesting.WithReceiverServiceFailed)),
			withFinalEventAndFailures(
				controllertesting.NewKafkaChannelFailedReconciliationEvent(),
				failure{"create", "Services", event.ReceiverServiceReconciliationFailed.String(), "Failed To Reconcile Receiver Service"})),

		newStableSystemTest("Reconcile Missing Receiver Service Success", withoutReceiverService, wantCreateReceiverService()),

		newStableSystemTest("Reconcile Receiver Service With Deletion Timestamp",
			replaceStatusUpdates(getReadyKafkaChannel(controllertesting.WithReceiverServiceFailedTimestamp)),
			withReceiverService(controllertesting.WithDeletionTimestampService),
			withErrorEvent(controllertesting.NewKafkaChannelFailedReconciliationEvent(),
				event.ReceiverServiceReconciliationFailed.String(),
				"Failed To Reconcile Receiver Service: encountered Receiver Service with DeletionTimestamp eventing-test-ns/kafkasecret-name-b9176d5f-receiver - potential race condition")),

		//
		// KafkaChannel Receiver Deployment
		//

		newStableSystemTest("Reconcile Missing Receiver Deployment Success", withoutReceiverDeployment, wantCreateReceiverDeployment()),

		newStableSystemTest("Reconcile Missing Receiver Deployment Error(Create)", withoutReceiverDeployment, wantCreateReceiverDeployment(),
			replaceStatusUpdates(getReadyKafkaChannel(controllertesting.WithReceiverDeploymentFailed)),
			withFinalEventAndFailures(
				controllertesting.NewKafkaChannelFailedReconciliationEvent(),
				failure{"create", "Deployments", event.ReceiverDeploymentReconciliationFailed.String(), "Failed To Reconcile Receiver Deployment"})),

		newStableSystemTest("Reconcile Receiver Deployment With Deletion Timestamp",
			replaceStatusUpdates(getReadyKafkaChannel(controllertesting.WithReceiverDeploymentFailedTimestamp)),
			withReceiverDeployment(controllertesting.WithDeletionTimestampDeployment),
			withErrorEvent(controllertesting.NewKafkaChannelFailedReconciliationEvent(),
				event.ReceiverDeploymentReconciliationFailed.String(),
				"Failed To Reconcile Receiver Deployment: encountered Receiver Deployment with DeletionTimestamp eventing-test-ns/kafkasecret-name-b9176d5f-receiver - potential race condition")),

		//
		// AdminClient Errors
		//

		newStableSystemTest("Reconciliation AdminClient Error",
			withNewAdminClientFn(
				func(ctx context.Context, brokers []string, config *sarama.Config, adminClientType types.AdminClientType) (types.AdminClientInterface, error) {
					return nil, fmt.Errorf("test-adminclient-error")
				},
			),
			withSingleErrorEvent("InternalError", "test-adminclient-error"),
		),

		newDeletedKafkaChannelTest("Finalization AdminClient Error",
			withoutUpdates,
			withoutDeletes,
			withNewAdminClientFn(
				func(ctx context.Context, brokers []string, config *sarama.Config, adminClientType types.AdminClientType) (types.AdminClientInterface, error) {
					return nil, fmt.Errorf("test-adminclient-error")
				},
			),
			withSingleErrorEvent("InternalError", "test-adminclient-error"),
		),

		//
		// Miscellaneous
		//

		newStableSystemTest("Reconcile Dispatcher Deployment - Redeployment on ConfigMapHash change",
			withDispatcherDeployment(controllertesting.WithConfigMapHash("initial-hash-to-be-overridden-by-controller")),
			wantUpdateDispatcherDeployment),

		newStableSystemTest("Missing KafkaSecret - Error", withoutSecret,
			replaceStatusUpdates(getReadyKafkaChannel(controllertesting.WithKafkaChannelConfigurationFailedNoSecret)),
			withReconcilerOptions(withEmptyKafkaSecret),
			withFinalEventAndFailures(controllertesting.NewKafkaChannelFailedReconciliationEvent())),

		newStableSystemTest("Defaults For Empty KafkaChannel", withKafkaChannel(getReadyKafkaChannel(controllertesting.WithEmptySpec)),
			withoutStatusUpdates),

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
		newDispatcherUpdateTest("Missing Annotations", controllertesting.WithoutAnnotations),
		newDispatcherNoUpdateTest("Different Lifecycle", controllertesting.WithDifferentLifecycle),
		newDispatcherNoUpdateTest("Different TerminationPath", controllertesting.WithDifferentTerminationPath),
		newDispatcherNoUpdateTest("Different TerminationPolicy", controllertesting.WithDifferentTerminationPolicy),
		newDispatcherNoUpdateTest("Different ImagePullPolicy", controllertesting.WithDifferentImagePullPolicy),
		newDispatcherNoUpdateTest("Different SecurityContext", controllertesting.WithDifferentSecurityContext),
		newDispatcherNoUpdateTest("Different Replicas", controllertesting.WithDifferentReplicas),
		newDispatcherNoUpdateTest("Extra Labels", controllertesting.WithExtraLabels),
		newDispatcherNoUpdateTest("Extra Annotations", controllertesting.WithExtraAnnotations),
		newDispatcherNoUpdateTest("Different Volumes", controllertesting.WithDifferentVolumes), // Volume changes without VolumeMount changes do not require an update
		newDispatcherUpdateTest("Different VolumeMounts And Volumes", controllertesting.WithDifferentVolumeMounts, controllertesting.WithDifferentVolumes),

		//
		// Deployment Update Failure
		//

		newStableSystemTest("Existing Dispatcher Deployment, Different Image, Update Error",
			withDispatcherDeployment(controllertesting.WithDifferentImage),
			wantUpdateDispatcherDeployment,
			withFinalEventAndFailures(controllertesting.NewKafkaChannelFailedReconciliationEvent(),
				failure{"update", "Deployments", event.DispatcherDeploymentReconciliationFailed.String(), "Failed To Reconcile Dispatcher Deployment"}),
			replaceStatusUpdates(getReadyKafkaChannel(controllertesting.WithDispatcherUpdateFailed)),
			withFirstEvent(controllertesting.NewKafkaChannelDispatcherDeploymentUpdateFailedEvent())),

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

	// Restore The NewAdminClient Wrapper On Completion
	defer kafkaadmintesting.RestoreNewAdminClientFn()

	// Run The TableTest Using The KafkaChannel Reconciler Provided By The Factory
	logger := logtesting.TestLogger(t)
	tableTest.Test(t, controllertesting.MakeFactory(func(ctx context.Context, listers *controllertesting.Listers, cmw configmap.Watcher, options map[string]interface{}) controller.Reconciler {

		// TODO - NEW
		// Use Table Row AdminClient If Specified, Otherwise Use Valid Success Mock AdminClient
		newAdminClientFnInterface, ok := options["newAdminClientFn"]
		if !ok || newAdminClientFnInterface == nil {
			newAdminClientFnInterface = kafkaadmintesting.NonValidatingNewAdminClientFn(&controllertesting.MockAdminClient{})
		}
		newAdminClientFn := newAdminClientFnInterface.(kafkaadminwrapper.NewAdminClientFnType)
		kafkaadmintesting.StubNewAdminClientFn(newAdminClientFn)

		// Use Table Row ConfigOptions If Specified, Otherwise Empty
		configOptionsInterface, ok := options["configOptions"]
		if !ok || configOptionsInterface == nil {
			configOptionsInterface = []controllertesting.KafkaConfigOption{}
		}
		configOptions := configOptionsInterface.([]controllertesting.KafkaConfigOption)

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
			kafkaConfigMapHash:   controllertesting.ConfigMapHash,
		}

		reconcilerOptions, ok := options["reconcilerOptions"]
		if ok {
			for _, option := range reconcilerOptions.([]reconcilerOption) {
				option(r)
			}
		}

		return kafkachannelreconciler.NewReconciler(ctx, logger, r.kafkaClientSet, listers.GetKafkaChannelLister(), controller.GetEventRecorder(ctx), r)
	}, logger.Desugar()))
}

// Utility Functions

// reconcilerOption Enables Customization Of A Reconciler
type reconcilerOption func(reconciler *Reconciler)

// withEmptyKafkaSecret removes the secret name from a reconciler config
func withEmptyKafkaSecret(reconciler *Reconciler) {
	reconciler.config.Kafka.AuthSecretName = ""
}

// newServiceNoPatchTest creates a test that expects to not have a dispatcher service patch, using the provided options
func newServiceNoPatchTest(name string, options ...controllertesting.ServiceOption) TableRow {
	test := newStableSystemTest("Existing Dispatcher Service, " + name + ", No Patch")
	test.Objects = append(test.Objects,
		controllertesting.NewKafkaChannelDispatcherService(options...),
		controllertesting.NewKafkaChannelDispatcherDeployment())
	return test
}

// testOption Enables Customization Of A TableRow
type testOption func(test *TableRow)

// dispatcherName returns the name of the test dispatcher service and deployment (which is a constant but
// should still be obtained via the DispatcherDnsSafeName function)
func dispatcherName() string {
	return util.DispatcherDnsSafeName(&kafkav1beta1.KafkaChannel{
		ObjectMeta: metav1.ObjectMeta{Namespace: controllertesting.KafkaChannelNamespace, Name: controllertesting.KafkaChannelName},
	})
}

// kafkaChannelServiceName returns the name of the test KafkaChannel (which is a constant but should
// still be obtained via the AppendKafkaChannelServiceNameSuffix function)
func kafkaChannelServiceName() string {
	return kafkautil.AppendKafkaChannelServiceNameSuffix(controllertesting.KafkaChannelName)
}

// withoutKafkaChannelService removes the KafkaChannel service from the Objects field
func withoutKafkaChannelService(test *TableRow) {
	withoutService(test, kafkaChannelServiceName())
}

// withoutSecret removes the secret from the Objects field
func withoutSecret(test *TableRow) {
	for index, object := range test.Objects {
		if _, ok := object.(*corev1.Secret); ok {
			test.Objects = append(test.Objects[:index], test.Objects[index+1:]...)
			return
		}
	}
}

// wantCreateKafkaChannelService adds a new KafkaChannel service to the WantCreates field
func wantCreateKafkaChannelService(test *TableRow) {
	test.WantCreates = append(test.WantCreates, controllertesting.NewKafkaChannelService())
}

// withService returns a function that replaces a named service in the Objects field
// with the one provided.
func withService(name string, newService *corev1.Service) testOption {
	return func(test *TableRow) {
		// Replace the existing service object, if it exists
		for index, object := range test.Objects {
			if service, ok := object.(*corev1.Service); ok && service.Name == name {
				test.Objects[index] = newService
				return
			}
		}
		// Otherwise add a new one
		test.Objects = append(test.WantCreates, newService)
	}
}

// withKafkaChannelService returns a function that replaces the KafkaChannel service in the Objects field
// with one that uses the given options.
func withKafkaChannelService(options ...controllertesting.ServiceOption) testOption {
	return withService(kafkaChannelServiceName(), controllertesting.NewKafkaChannelService(options...))
}

// withDispatcherService returns a function that replaces the Dispatcher service in the Objects field
// with one that uses the given options.
func withDispatcherService(options ...controllertesting.ServiceOption) testOption {
	return withService(dispatcherName(), controllertesting.NewKafkaChannelDispatcherService(options...))
}

// with one that uses the given options.
// withReceiverService returns a function that replaces the Receiver service in the Objects field
func withReceiverService(options ...controllertesting.ServiceOption) testOption {
	return withService(controllertesting.ReceiverServiceName, controllertesting.NewKafkaChannelReceiverService(options...))
}

// withDeployment returns a function that replaces a named deployment in the Objects field
// with the one provided.
func withDeployment(name string, newDeployment *appsv1.Deployment) testOption {
	return func(test *TableRow) {
		// Replace the existing Deployment object, if it exists
		for index, object := range test.Objects {
			if deployment, ok := object.(*appsv1.Deployment); ok && deployment.Name == name {
				test.Objects[index] = newDeployment
				return
			}
		}
		// Otherwise add a new one
		test.Objects = append(test.WantCreates, newDeployment)
	}
}

// withDispatcherDeployment returns a function that replaces the Dispatcher deployment in the Objects field
// with one that uses the given options.
func withDispatcherDeployment(options ...controllertesting.DeploymentOption) testOption {
	return withDeployment(dispatcherName(), controllertesting.NewKafkaChannelDispatcherDeployment(options...))
}

// withReceiverDeployment returns a function that replaces the Receiver deployment in the Objects field
// with one that uses the given options.
func withReceiverDeployment(options ...controllertesting.DeploymentOption) testOption {
	return withDeployment(controllertesting.ReceiverDeploymentName, controllertesting.NewKafkaChannelReceiverDeployment(options...))
}

// wantDeleteReceiverService adds to the WantDeletes field an entry corresponding to a Receiver service
func wantDeleteReceiverService(test *TableRow) {
	test.WantDeletes = append(test.WantDeletes,
		controllertesting.NewServiceDeleteActionImpl(controllertesting.NewKafkaChannelReceiverService(controllertesting.WithoutFinalizersService)))
}

// wantDeleteReceiverDeployment adds to the WantDeletes field an entry corresponding to a Receiver deployment
func wantDeleteReceiverDeployment(test *TableRow) {
	test.WantDeletes = append(test.WantDeletes,
		controllertesting.NewDeploymentDeleteActionImpl(controllertesting.NewKafkaChannelReceiverDeployment(controllertesting.WithoutFinalizersDeployment)))
}

// wantUpdateDispatcherDeployment adds to the WantUpdates and WantEvents fields the entries involved in a Dispatcher deployment update
func wantUpdateDispatcherDeployment(test *TableRow) {
	test.WantUpdates = append(test.WantUpdates, controllertesting.NewDeploymentUpdateActionImpl(controllertesting.NewKafkaChannelDispatcherDeployment()))
	test.WantEvents = append([]string{controllertesting.NewKafkaChannelDispatcherDeploymentUpdatedEvent()}, test.WantEvents...)
}

// wantCreateService returns a function that replaces a named service in the WantCreates field
// with the one provided.
func wantCreateService(name string, newService *corev1.Service) testOption {
	return func(test *TableRow) {
		// Replace the existing service object, if it exists
		for index, object := range test.WantCreates {
			if service, ok := object.(*corev1.Service); ok && service.Name == name {
				test.WantCreates[index] = newService
				return
			}
		}
		// Otherwise add a new one
		test.WantCreates = append(test.WantCreates, newService)
	}
}

// wantCreateDispatcherService returns a function that replaces the Dispatcher service in the WantCreates field
// with one that uses the given options.
func wantCreateDispatcherService(options ...controllertesting.ServiceOption) testOption {
	return wantCreateService(dispatcherName(), controllertesting.NewKafkaChannelDispatcherService(options...))
}

// wantCreateReceiverService returns a function that replaces the Receiver service in the WantCreates field
// with one that uses the given options.
func wantCreateReceiverService(options ...controllertesting.ServiceOption) testOption {
	return wantCreateService(controllertesting.ReceiverServiceName, controllertesting.NewKafkaChannelReceiverService(options...))
}

// wantCreateDeployment returns a function that replaces a named deployment in the WantCreates field
// with the one provided.
func wantCreateDeployment(name string, newDeployment *appsv1.Deployment) testOption {
	return func(test *TableRow) {
		// Replace the existing deployment object, if it exists
		for index, object := range test.WantCreates {
			if deployment, ok := object.(*appsv1.Deployment); ok && deployment.Name == name {
				test.WantCreates[index] = newDeployment
				return
			}
		}
		// Otherwise add a new one
		test.WantCreates = append(test.WantCreates, newDeployment)
	}
}

// wantCreateReceiverDeployment returns a function that replaces the Receiver deployment in the WantCreates field
// with one that uses the given options.
func wantCreateReceiverDeployment(options ...controllertesting.DeploymentOption) testOption {
	return wantCreateDeployment(controllertesting.ReceiverDeploymentName, controllertesting.NewKafkaChannelReceiverDeployment(options...))
}

// wantCreateDispatcherDeployment returns a function that replaces the Dispatcher deployment in the WantCreates field
// with one that uses the given options.
func wantCreateDispatcherDeployment(options ...controllertesting.DeploymentOption) testOption {
	return wantCreateDeployment(dispatcherName(), controllertesting.NewKafkaChannelDispatcherDeployment(options...))
}

// withConfig returns a function that adds to (or creates) the "configOptions" entry in the
// OtherTestData map using the provided options.
func withConfig(option ...controllertesting.KafkaConfigOption) testOption {
	return func(test *TableRow) {
		if test.OtherTestData == nil {
			test.OtherTestData = make(map[string]interface{})
		}
		configOptions, ok := test.OtherTestData["configOptions"].([]controllertesting.KafkaConfigOption)
		if !ok {
			configOptions = []controllertesting.KafkaConfigOption{}
		}
		test.OtherTestData["configOptions"] = append(configOptions, option...)
	}
}

// withReconcilerOptions returns a function that adds to (or creates) the "reconcilerOptions" entry in the
// OtherTestData map using the provided options.
func withReconcilerOptions(options ...reconcilerOption) testOption {
	return func(test *TableRow) {
		if test.OtherTestData == nil {
			test.OtherTestData = make(map[string]interface{})
		}
		reconcilerOptions, ok := test.OtherTestData["reconcilerOptions"].([]reconcilerOption)
		if !ok {
			reconcilerOptions = []reconcilerOption{}
		}
		test.OtherTestData["reconcilerOptions"] = append(reconcilerOptions, options...)
	}
}

// withErrorEvent returns a function that replaces the WantEvents field with two events - one with the given
// errorEvent and errorDescription, followed by the finalEvent.
func withErrorEvent(finalEvent string, errorEvent string, errorDescription string) testOption {
	return func(test *TableRow) {
		test.WantEvents = []string{
			Eventf(corev1.EventTypeWarning, errorEvent, errorDescription),
			finalEvent,
		}
		test.WantErr = true
	}
}

// withSingleErrorEvent returns a function that replaces the WantEvents field with the specified error event.
func withSingleErrorEvent(errorEvent string, errorDescription string) testOption {
	return func(test *TableRow) {
		test.WantEvents = []string{
			Eventf(corev1.EventTypeWarning, errorEvent, errorDescription),
		}
		test.WantErr = true
	}
}

// withFirstEvent returns a function that inserts the given event into the WantEvents slice
func withFirstEvent(event string) testOption {
	return func(test *TableRow) {
		test.WantEvents = append([]string{event}, test.WantEvents...)
	}
}

// failure is a convenience struct used by withFinalEventAndFailures to easily create reactors and "WantEvents" entries
type failure struct {
	Verb     string
	Resource string
	Reason   string
	Message  string
}

// withFinalEventAndFailures returns a function that replaces the WantEvents field with one consisting of the
// failure slices given, and sets the WithReactors field to generate those errors.  The finalEvent string is
// appended to the end of the WantEvents.
func withFinalEventAndFailures(finalEvent string, failures ...failure) testOption {
	return func(test *TableRow) {
		test.WantEvents = nil
		test.WantErr = true
		for _, failure := range failures {
			test.WithReactors = append(test.WithReactors, InduceFailure(failure.Verb, failure.Resource))
			test.WantEvents = append(test.WantEvents,
				Eventf(corev1.EventTypeWarning, failure.Reason, failure.Message+strings.ToLower(": inducing failure for "+failure.Verb+" "+failure.Resource)))
		}
		test.WantEvents = append(test.WantEvents, finalEvent)
	}
}

// withKafkaChannel returns a function that replaces the KafkaChannel in the Objects field with the provided one
func withKafkaChannel(kafkachannel *kafkav1beta1.KafkaChannel) testOption {
	return func(test *TableRow) {
		// Replace the existing KafkaChannel object, if it exists
		for index, object := range test.Objects {
			if _, ok := object.(*kafkav1beta1.KafkaChannel); ok {
				test.Objects[index] = kafkachannel
				return
			}
		}
		// Otherwise add a new one
		test.Objects = append(test.Objects, kafkachannel)
	}
}

// withNewAdminClientFn returns a function that sets the "adminClient" in the TableRow.OtherTestData
func withNewAdminClientFn(newAdminClientFn kafkaadminwrapper.NewAdminClientFnType) testOption {
	return func(test *TableRow) {
		if test.OtherTestData == nil {
			test.OtherTestData = make(map[string]interface{})
		}
		test.OtherTestData["newAdminClientFn"] = newAdminClientFn
	}
}

// getReceiverOnlyKafkaChannel returns a KafkaChannel with all of its non-dispatcher-related ancillary fields
// ready (Receiver service and deployment, KafkaChannel service, Topic, etc.)
func getReceiverOnlyKafkaChannel(options ...controllertesting.KafkaChannelOption) *kafkav1beta1.KafkaChannel {
	channel := controllertesting.NewKafkaChannel(
		controllertesting.WithInitializedConditions,
		controllertesting.WithMetaData,
		controllertesting.WithAddress,
		controllertesting.WithFinalizer,
		controllertesting.WithKafkaChannelServiceReady,
		controllertesting.WithReceiverServiceReady,
		controllertesting.WithReceiverDeploymentReady,
		controllertesting.WithTopicReady,
	)

	for _, option := range options {
		option(channel)
	}

	return channel
}

// getReadyKafkaChannel returns a KafkaChannel with all of its ancillary fields ready (Receiver and Dispatcher
// services and deployments, the KafkaChannel service, Topic, etc.)
func getReadyKafkaChannel(options ...controllertesting.KafkaChannelOption) *kafkav1beta1.KafkaChannel {
	channel := getReceiverOnlyKafkaChannel(
		controllertesting.WithDispatcherDeploymentReady,
	)

	for _, option := range options {
		option(channel)
	}

	return channel
}

// withErrorNoUpdates removes the WantPatches and WantUpdates fields, and sets WantErr to true
func withErrorNoUpdates(test *TableRow) {
	test.WantPatches = nil
	test.WantUpdates = nil
	test.WantErr = true
}

// withoutUpdates removes the WantUpdates field
func withoutUpdates(test *TableRow) {
	test.WantUpdates = nil
}

// withoutStatusUpdates removes the WantStatusUpdates field
func withoutStatusUpdates(test *TableRow) {
	test.WantStatusUpdates = nil
}

// withoutDeletes removes the WantDeletes field
func withoutDeletes(test *TableRow) {
	test.WantDeletes = nil
}

// withoutDeployment removes the named service from the Objects field
func withoutService(test *TableRow, name string) {
	for index, object := range test.Objects {
		if service, ok := object.(*corev1.Service); ok && service.Name == name {
			test.Objects = append(test.Objects[:index], test.Objects[index+1:]...)
			return
		}
	}
}

// withoutReceiverService removes the Receiver service from the Objects field
func withoutReceiverService(test *TableRow) {
	withoutService(test, controllertesting.ReceiverServiceName)
}

// withoutDispatcherService removes the Dispatcher service from the Objects field
func withoutDispatcherService(test *TableRow) {
	withoutService(test, dispatcherName())
}

// withoutDeployment removes the named deployment from the Objects field
func withoutDeployment(test *TableRow, name string) {
	for index, object := range test.Objects {
		if deployment, ok := object.(*appsv1.Deployment); ok && deployment.Name == name {
			test.Objects = append(test.Objects[:index], test.Objects[index+1:]...)
			return
		}
	}
}

// withoutReceiverDeployment removes the Receiver deployment from the Objects field
func withoutReceiverDeployment(test *TableRow) {
	withoutDeployment(test, controllertesting.ReceiverDeploymentName)
}

// withoutDispatcherDeployment removes the Dispatcher deployment from the Objects field
func withoutDispatcherDeployment(test *TableRow) {
	withoutDeployment(test, dispatcherName())
}

// withoutReceiver removes the Receiver service and deployment from the Objects field
func withoutReceiver(test *TableRow) {
	withoutReceiverService(test)
	withoutReceiverDeployment(test)
}

// withoutDispatcher removes the Dispatcher service and deployment from the Objects field
func withoutDispatcher(test *TableRow) {
	withoutDispatcherService(test)
	withoutDispatcherDeployment(test)
}

// replaceStatusUpdates returns a function that swaps out the WantStatusUpdates field with
// a new slice of UpdateActionImpl structs containing the provided KafkaChannels in the Object field
func replaceStatusUpdates(updates ...*kafkav1beta1.KafkaChannel) testOption {
	return func(test *TableRow) {
		test.WantStatusUpdates = []clientgotesting.UpdateActionImpl{}
		for _, update := range updates {
			test.WantStatusUpdates = append(test.WantStatusUpdates, clientgotesting.UpdateActionImpl{Object: update})
		}
	}
}

// replaceEvents returns a function that swaps out the WantEvents field with the provided set.
func replaceEvents(events ...string) testOption {
	return func(test *TableRow) {
		test.WantEvents = events
	}
}

// replaceUpdates returns a function that swaps out the WantUpdates field with the provided set.
func replaceUpdates(updates ...clientgotesting.UpdateActionImpl) testOption {
	return func(test *TableRow) {
		test.WantUpdates = updates
	}
}

// replaceDeletes returns a function that swaps out the WantDeletes field with the provided set.
func replaceDeletes(deletes ...clientgotesting.DeleteActionImpl) testOption {
	return func(test *TableRow) {
		test.WantDeletes = deletes
	}
}

// newDispatcherUpdateTest creates a test that expects a dispatcher deployment update, using the provided options
func newDispatcherUpdateTest(name string, options ...controllertesting.DeploymentOption) TableRow {
	test := newStableSystemTest("Existing Dispatcher Deployment, " + name + ", Update Needed")
	test.Objects = append(test.Objects,
		controllertesting.NewKafkaChannelDispatcherService(),
		controllertesting.NewKafkaChannelDispatcherDeployment(options...))
	test.WantUpdates = append(test.WantUpdates,
		controllertesting.NewDeploymentUpdateActionImpl(controllertesting.NewKafkaChannelDispatcherDeployment()))
	test.WantEvents = append([]string{controllertesting.NewKafkaChannelDispatcherDeploymentUpdatedEvent()},
		test.WantEvents...)
	return test
}

// newDispatcherNoUpdateTest creates a test that expects to not have a dispatcher deployment update, using the provided options
func newDispatcherNoUpdateTest(name string, options ...controllertesting.DeploymentOption) TableRow {
	test := newStableSystemTest("Existing Dispatcher Deployment, " + name + ", No Update")
	test.Objects = append(test.Objects,
		controllertesting.NewKafkaChannelDispatcherService(),
		controllertesting.NewKafkaChannelDispatcherDeployment(options...))
	return test
}

// newServicePatchTest creates a test that expects a dispatcher service update, using the provided options
func newServicePatchTest(name string, options ...controllertesting.ServiceOption) TableRow {
	newService := controllertesting.NewKafkaChannelDispatcherService()
	existingService := controllertesting.NewKafkaChannelDispatcherService(options...)

	test := newStableSystemTest("Existing Dispatcher Service, " + name + ", Patch Needed")
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

	return test
}

// newServicePatchFailureTest creates a new TableRow for a test that has an error during the patch process
func newServicePatchFailureTest(name string, options ...controllertesting.ServiceOption) TableRow {
	test := newServicePatchTest(name, options...)
	test.Name = "Existing Dispatcher Service, " + name + ", Patch Error"

	test.WantEvents = []string{
		controllertesting.NewKafkaChannelDispatcherServicePatchFailedEvent(),
		Eventf(corev1.EventTypeWarning, event.DispatcherServiceReconciliationFailed.String(), "Failed To Reconcile Dispatcher Service: inducing failure for patch services"),
		controllertesting.NewKafkaChannelFailedReconciliationEvent(),
	}

	test.WantStatusUpdates = []clientgotesting.UpdateActionImpl{{Object: getReadyKafkaChannel(controllertesting.WithDispatcherServicePatchFailed)}}

	// If the service fails, the other reconcilers are not executed, so no updates are expected
	test.WantUpdates = nil

	test.WithReactors = []clientgotesting.ReactionFunc{InduceFailure("patch", "Services")}
	test.WantErr = true

	return test
}

// newDeletedKafkaChannelTest creates a TableRow for a test that involves a KafkaChannel that
// is in the process of being deleted (i.e. has a DeletionTimestamp).
func newDeletedKafkaChannelTest(name string, options ...testOption) TableRow {

	service := controllertesting.NewKafkaChannelDispatcherService(controllertesting.WithoutFinalizersService)
	deployment := controllertesting.NewKafkaChannelDispatcherDeployment(controllertesting.WithoutFinalizersDeployment)

	test := newStableSystemTest("Finalize Deleted KafkaChannel "+name,
		withKafkaChannel(controllertesting.NewKafkaChannel(
			controllertesting.WithInitializedConditions,
			controllertesting.WithLabels,
			controllertesting.WithDeletionTimestamp,
		)),
		replaceEvents(controllertesting.NewKafkaChannelSuccessfulFinalizedEvent()),
		replaceUpdates(controllertesting.NewServiceUpdateActionImpl(service), controllertesting.NewDeploymentUpdateActionImpl(deployment)),
		replaceDeletes(controllertesting.NewServiceDeleteActionImpl(service), controllertesting.NewDeploymentDeleteActionImpl(deployment)),
	)

	for _, option := range options {
		option(&test)
	}

	return test
}

// newDeletedSecretTest returns a TableRow for a test that corresponds to a system with Receiver and Dispatcher
// services and deployments, but that has had the Kafka auth secret removed.  Without modifications this test will
// expect a reconciliation failure, as the secret is mandatory.
func newDeletedSecretTest(name string, options ...testOption) TableRow {
	test := newStableSystemTest("Deleted Secret, "+name,
		withoutSecret,
		withErrorNoUpdates,
		replaceStatusUpdates(
			getReadyKafkaChannel(
				controllertesting.WithReceiverServiceFailedNoSecret,
				controllertesting.WithReceiverDeploymentFailedNoSecret,
			),
			getReadyKafkaChannel(
				controllertesting.WithKafkaChannelConfigurationFailedNoSecret,
			),
		),
		replaceEvents(controllertesting.NewKafkaChannelFailedReconciliationEvent()),
	)

	for _, option := range options {
		option(&test)
	}

	return test
}

// newStableSystemTest returns a TableRow for a test that corresponds to a functional system
// of a secret, a KafkaChannel, and the Receiver/Dispatcher services and deployments.  If no modifications
// are made to this test, the only expected result is a "Successful KafkaChannel Reconciliation" event
// (no creates, deletes, patches, etc.)
// This test is intended to be used as the base for other tests that are "A working system, except for ..."
func newStableSystemTest(name string, options ...testOption) TableRow {

	test := TableRow{
		Name: name,
		Key:  controllertesting.KafkaChannelKey,
		Objects: []runtime.Object{
			controllertesting.NewKafkaSecret(),
			getReadyKafkaChannel(),
			controllertesting.NewKafkaChannelService(),
			controllertesting.NewKafkaChannelReceiverService(),
			controllertesting.NewKafkaChannelReceiverDeployment(),
			controllertesting.NewKafkaChannelDispatcherService(),
			controllertesting.NewKafkaChannelDispatcherDeployment(),
		},
		WantEvents:              []string{controllertesting.NewKafkaChannelSuccessfulReconciliationEvent()},
		SkipNamespaceValidation: true,
	}

	for _, option := range options {
		option(&test)
	}

	return test
}

// newReconciliationTest creates a TableRow for a basic test that has only a secret and KafkaChannel,
// and expects all other objects (receiver and dispatcher deployment and services) to be created.
func newReconciliationTest(name string, options ...testOption) TableRow {
	test := TableRow{
		Name: name,
		// The "WantPatches" part of the table test assumes that a patch is supposed to be for the namespace
		// given by the "Key" field, which, in this case, is the namespace for the KafkaChannel.  This assumption
		// is not correct, so that validation must be skipped here (this is true for the Update commands as well
		// but the table test code does not verify that updates are done in the same namespace for some reason).
		SkipNamespaceValidation: true,
		Key:                     controllertesting.KafkaChannelKey,
		Objects:                 controllertesting.NewSecretAndKafkaChannel(controllertesting.WithInitializedConditions),
		WantCreates: []runtime.Object{
			controllertesting.NewKafkaChannelReceiverService(),
			controllertesting.NewKafkaChannelReceiverDeployment(),
			controllertesting.NewKafkaChannelService(),
			controllertesting.NewKafkaChannelDispatcherService(),
			controllertesting.NewKafkaChannelDispatcherDeployment(),
		},
		WantStatusUpdates: []clientgotesting.UpdateActionImpl{
			{
				Object: controllertesting.NewKafkaChannel(
					controllertesting.WithInitializedConditions,
					controllertesting.WithAnnotations,
					controllertesting.WithFinalizer,
					controllertesting.WithReceiverServiceReady,
					controllertesting.WithReceiverDeploymentReady,
					controllertesting.WithTopicReady,
				),
			},
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
		WantPatches: []clientgotesting.PatchActionImpl{
			controllertesting.NewFinalizerPatchActionImpl(),
		},
		WantEvents: []string{
			controllertesting.NewKafkaChannelFinalizerUpdateEvent(),
			controllertesting.NewKafkaChannelSuccessfulReconciliationEvent(),
		},
	}

	for _, option := range options {
		option(&test)
	}

	return test
}

func TestReconciler_updateKafkaConfig(t *testing.T) {

	commontesting.SetTestEnvironment(t)

	ekConfig := strings.Replace(commontesting.TestEKConfig, "kafka:", `kafka:
  authSecretName: `+commontesting.SecretName+`
  authSecretNamespace: `+system.Namespace(), 1)

	tests := []struct {
		name      string
		configMap *corev1.ConfigMap
		hash      string
		user      string
		secretErr bool
		expectErr string
	}{
		{
			name:      "Nil ConfigMap",
			configMap: nil,
			expectErr: "^nil configMap passed to updateKafkaConfig$",
		},
		{
			name:      "Nil ConfigMap Data",
			configMap: &corev1.ConfigMap{},
			expectErr: "^configMap.Data is nil$",
		},
		{
			name:      "Empty Eventing-Kafka YAML (default for authSecretName)",
			configMap: &corev1.ConfigMap{Data: map[string]string{constants.EventingKafkaSettingsConfigKey: ""}},
		},
		{
			name:      "Invalid Eventing-Kafka YAML",
			configMap: &corev1.ConfigMap{Data: map[string]string{constants.EventingKafkaSettingsConfigKey: "\tInvalid"}},
			expectErr: "^ConfigMap's eventing-kafka value could not be converted to an EventingKafkaConfig struct",
		},
		{
			name: "Missing Sarama YAML (creates default sarama.Config)",
			configMap: &corev1.ConfigMap{Data: map[string]string{
				constants.VersionConfigKey:               constants.CurrentConfigVersion,
				constants.EventingKafkaSettingsConfigKey: ekConfig}},
		},
		{
			name: "Error Reading Secret",
			configMap: &corev1.ConfigMap{Data: map[string]string{
				constants.VersionConfigKey:               constants.CurrentConfigVersion,
				constants.EventingKafkaSettingsConfigKey: ekConfig,
				constants.SaramaSettingsConfigKey:        commontesting.OldSaramaConfig}},
			secretErr: true,
		},
		{
			name: "Empty User",
			configMap: &corev1.ConfigMap{Data: map[string]string{
				constants.VersionConfigKey:               constants.CurrentConfigVersion,
				constants.EventingKafkaSettingsConfigKey: ekConfig,
				constants.SaramaSettingsConfigKey:        commontesting.OldSaramaConfig}},
		},
		{
			name: "Non-Empty User",
			configMap: &corev1.ConfigMap{Data: map[string]string{
				constants.VersionConfigKey:               constants.CurrentConfigVersion,
				constants.EventingKafkaSettingsConfigKey: ekConfig,
				constants.SaramaSettingsConfigKey:        commontesting.OldSaramaConfig}},
			user: commontesting.OldAuthUsername,
		},
		{
			name: "Invalid Sarama YAML",
			configMap: &corev1.ConfigMap{Data: map[string]string{
				constants.EventingKafkaSettingsConfigKey: ekConfig,
				constants.SaramaSettingsConfigKey:        "\tInvalid",
			}},
			expectErr: "^failed to extract KafkaVersion from Sarama Config YAML",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			secret := commontesting.GetTestSaramaSecret(
				commontesting.SecretName,
				tt.user,
				commontesting.OldAuthPassword,
				commontesting.OldAuthNamespace,
				commontesting.OldAuthSaslType)
			fakeK8sClient := fake.NewSimpleClientset(secret)
			ctx := context.WithValue(context.TODO(), kubeclient.Key{}, fakeK8sClient)
			if tt.secretErr {
				fakeK8sClient.PrependReactor("get", "Secrets", InduceFailure("get", "Secrets"))
			}
			r := &Reconciler{
				kubeClientset:      fakeK8sClient,
				kafkaConfigMapHash: tt.hash,
			}
			err := r.updateKafkaConfig(ctx, tt.configMap)
			if tt.expectErr == "" {
				assert.Nil(t, err)
				// If no error was expected, verify that the settings in the reconciler were changed to new values
				assert.NotEqual(t, tt.hash, r.kafkaConfigMapHash)
				assert.NotNil(t, r.config)
			} else {
				assert.NotNil(t, err)
				assert.Regexp(t, tt.expectErr, err.Error())
				// If an error occurred, verify that the settings in the reconciler do not change from what they were
				assert.Equal(t, tt.hash, r.kafkaConfigMapHash)
				assert.Nil(t, r.config)
			}
		})
	}

	// Verify that a nil reconciler doesn't panic
	var nilReconciler *Reconciler
	//goland:noinspection GoNilness
	assert.Equal(t, fmt.Errorf("reconciler is nil (possible startup race condition)"), nilReconciler.updateKafkaConfig(context.TODO(), nil))

}
