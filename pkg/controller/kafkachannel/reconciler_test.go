package kafkachannel

import (
	"context"
	"github.com/kyma-incubator/knative-kafka/pkg/controller/event"
	"github.com/kyma-incubator/knative-kafka/pkg/controller/test"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/scheme"
	clientgotesting "k8s.io/client-go/testing"
	kafkav1alpha1 "knative.dev/eventing-contrib/kafka/channel/pkg/apis/messaging/v1alpha1"
	fakekafkaclient "knative.dev/eventing-contrib/kafka/channel/pkg/client/injection/client/fake"
	kafkachannelreconciler "knative.dev/eventing-contrib/kafka/channel/pkg/client/injection/reconciler/messaging/v1alpha1/kafkachannel"
	"knative.dev/eventing/pkg/logging"
	duckv1alpha1 "knative.dev/pkg/apis/duck/v1alpha1"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"
	logtesting "knative.dev/pkg/logging/testing"
	. "knative.dev/pkg/reconciler/testing"
	"testing"
)

// Initialization - Add types to scheme
func init() {
	_ = kafkav1alpha1.AddToScheme(scheme.Scheme)
	_ = duckv1alpha1.AddToScheme(scheme.Scheme)
}

// Test The Reconcile Functionality
func TestReconcile(t *testing.T) {

	// Clear Logs After Testing
	defer logtesting.ClearAll()

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
			Key:                     test.KafkaChannelKey,
			Objects: []runtime.Object{
				test.NewKafkaChannel(test.WithInitializedConditions),
			},
			WantCreates: []runtime.Object{
				test.NewKafkaChannelService(),
				test.NewKafkaChannelDispatcherService(),
				test.NewKafkaChannelDispatcherDeployment(),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: test.NewKafkaChannel(
						test.WithAddress,
						test.WithInitializedConditions,
						test.WithKafkaChannelServiceReady,
						test.WithDispatcherDeploymentReady,
						test.WithTopicReady,
					),
				},
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				test.NewKafkaChannelLabelUpdate(
					test.NewKafkaChannel(
						test.WithFinalizer,
						test.WithLabels,
						test.WithAddress,
						test.WithInitializedConditions,
						test.WithKafkaChannelServiceReady,
						test.WithDispatcherDeploymentReady,
						test.WithTopicReady,
					),
				),
			},
			WantPatches: []clientgotesting.PatchActionImpl{test.NewFinalizerPatchActionImpl()},
			WantEvents: []string{
				test.NewKafkaChannelFinalizerUpdateEvent(),
				test.NewKafkaChannelSuccessfulReconciliationEvent(),
			},
		},

		//
		// KafkaChannel Deletion (Finalizer)
		//

		{
			Name: "Finalize Deleted KafkaChannel",
			Key:  test.KafkaChannelKey,
			Objects: []runtime.Object{
				test.NewKafkaChannel(
					test.WithInitializedConditions,
					test.WithLabels,
					test.WithDeletionTimestamp,
				),
			},
			WantEvents: []string{
				test.NewKafkaChannelSuccessfulFinalizedEvent(),
			},
		},

		//
		// KafkaChannel Service
		//

		{
			Name:                    "Reconcile Missing KafkaChannel Service Success",
			SkipNamespaceValidation: true,
			Key:                     test.KafkaChannelKey,
			Objects: []runtime.Object{
				test.NewKafkaChannel(
					test.WithFinalizer,
					test.WithLabels,
					test.WithAddress,
					test.WithInitializedConditions,
					test.WithKafkaChannelServiceReady,
					test.WithDispatcherDeploymentReady,
					test.WithTopicReady,
				),
				test.NewKafkaChannelDispatcherService(),
				test.NewKafkaChannelDispatcherDeployment(),
			},
			WantCreates: []runtime.Object{test.NewKafkaChannelService()},
			WantEvents:  []string{test.NewKafkaChannelSuccessfulReconciliationEvent()},
		},
		{
			Name:                    "Reconcile Missing KafkaChannel Service Error(Create)",
			SkipNamespaceValidation: true,
			Key:                     test.KafkaChannelKey,
			Objects: []runtime.Object{
				test.NewKafkaChannel(
					test.WithFinalizer,
					test.WithAddress,
					test.WithInitializedConditions,
					test.WithKafkaChannelServiceReady,
					test.WithDispatcherDeploymentReady,
					test.WithTopicReady,
				),
				test.NewKafkaChannelDispatcherService(),
				test.NewKafkaChannelDispatcherDeployment(),
			},
			WithReactors: []clientgotesting.ReactionFunc{InduceFailure("create", "Services")},
			WantErr:      true,
			WantCreates:  []runtime.Object{test.NewKafkaChannelService()},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: test.NewKafkaChannel(
						test.WithFinalizer,
						test.WithAddress,
						test.WithInitializedConditions,
						test.WithKafkaChannelServiceFailed,
						test.WithDispatcherDeploymentReady,
						test.WithTopicReady,
					),
				},
			},
			WantEvents: []string{
				Eventf(corev1.EventTypeWarning, event.KafkaChannelServiceReconciliationFailed.String(), "Failed To Reconcile KafkaChannel Service: inducing failure for create services"),
				test.NewKafkaChannelFailedReconciliationEvent(),
			},
		},

		//
		// KafkaChannel Dispatcher Service
		//

		{
			Name:                    "Reconcile Missing Dispatcher Service Success",
			SkipNamespaceValidation: true,
			Key:                     test.KafkaChannelKey,
			Objects: []runtime.Object{
				test.NewKafkaChannel(
					test.WithFinalizer,
					test.WithLabels,
					test.WithAddress,
					test.WithInitializedConditions,
					test.WithKafkaChannelServiceReady,
					test.WithChannelServiceReady,
					test.WithChannelDeploymentReady,
					test.WithDispatcherDeploymentReady,
					test.WithTopicReady,
				),
				test.NewKafkaChannelService(),
				test.NewKafkaChannelChannelService(),
				test.NewKafkaChannelChannelDeployment(),
				test.NewKafkaChannelDispatcherDeployment(),
			},
			WantCreates: []runtime.Object{test.NewKafkaChannelDispatcherService()},
			WantEvents:  []string{test.NewKafkaChannelSuccessfulReconciliationEvent()},
		},
		{
			Name:                    "Reconcile Missing Dispatcher Service Error(Create)",
			SkipNamespaceValidation: true,
			Key:                     test.KafkaChannelKey,
			Objects: []runtime.Object{
				test.NewKafkaChannel(
					test.WithFinalizer,
					test.WithLabels,
					test.WithAddress,
					test.WithInitializedConditions,
					test.WithKafkaChannelServiceReady,
					test.WithChannelServiceReady,
					test.WithChannelDeploymentReady,
					test.WithDispatcherDeploymentReady,
					test.WithTopicReady,
				),
				test.NewKafkaChannelService(),
				test.NewKafkaChannelChannelService(),
				test.NewKafkaChannelChannelDeployment(),
				test.NewKafkaChannelDispatcherDeployment(),
			},
			WithReactors: []clientgotesting.ReactionFunc{InduceFailure("create", "services")},
			WantErr:      true,
			WantCreates:  []runtime.Object{test.NewKafkaChannelDispatcherService()},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				// Note - Not currently tracking status for the Dispatcher Service since it is only for Prometheus
			},
			WantEvents: []string{
				Eventf(corev1.EventTypeWarning, event.DispatcherServiceReconciliationFailed.String(), "Failed To Reconcile Dispatcher Service: inducing failure for create services"),
				test.NewKafkaChannelFailedReconciliationEvent(),
			},
		},

		//
		// KafkaChannel Dispatcher Deployment
		//

		{
			Name:                    "Reconcile Missing Dispatcher Deployment Success",
			SkipNamespaceValidation: true,
			Key:                     test.KafkaChannelKey,
			Objects: []runtime.Object{
				test.NewKafkaChannel(
					test.WithFinalizer,
					test.WithLabels,
					test.WithAddress,
					test.WithInitializedConditions,
					test.WithKafkaChannelServiceReady,
					test.WithChannelServiceReady,
					test.WithChannelDeploymentReady,
					test.WithDispatcherDeploymentReady,
					test.WithTopicReady,
				),
				test.NewKafkaChannelService(),
				test.NewKafkaChannelChannelService(),
				test.NewKafkaChannelChannelDeployment(),
				test.NewKafkaChannelDispatcherService(),
			},
			WantCreates: []runtime.Object{test.NewKafkaChannelDispatcherDeployment()},
			WantEvents:  []string{test.NewKafkaChannelSuccessfulReconciliationEvent()},
		},
		{
			Name:                    "Reconcile Missing Dispatcher Deployment Error(Create)",
			SkipNamespaceValidation: true,
			Key:                     test.KafkaChannelKey,
			Objects: []runtime.Object{
				test.NewKafkaChannel(
					test.WithFinalizer,
					test.WithLabels,
					test.WithAddress,
					test.WithInitializedConditions,
					test.WithKafkaChannelServiceReady,
					test.WithChannelServiceReady,
					test.WithChannelDeploymentReady,
					test.WithDispatcherDeploymentReady,
					test.WithTopicReady,
				),
				test.NewKafkaChannelService(),
				test.NewKafkaChannelChannelService(),
				test.NewKafkaChannelChannelDeployment(),
				test.NewKafkaChannelDispatcherService(),
			},
			WithReactors: []clientgotesting.ReactionFunc{InduceFailure("create", "deployments")},
			WantErr:      true,
			WantCreates:  []runtime.Object{test.NewKafkaChannelDispatcherDeployment()},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: test.NewKafkaChannel(
						test.WithFinalizer,
						test.WithLabels,
						test.WithAddress,
						test.WithInitializedConditions,
						test.WithKafkaChannelServiceReady,
						test.WithChannelServiceReady,
						test.WithChannelDeploymentReady,
						test.WithDispatcherFailed,
						test.WithTopicReady,
					),
				},
			},
			WantEvents: []string{
				Eventf(corev1.EventTypeWarning, event.DispatcherDeploymentReconciliationFailed.String(), "Failed To Reconcile Dispatcher Deployment: inducing failure for create deployments"),
				test.NewKafkaChannelFailedReconciliationEvent(),
			},
		},
	}

	// Run The TableTest Using The KafkaChannel Reconciler Provided By The Factory
	logger := logtesting.TestLogger(t)
	tableTest.Test(t, test.MakeFactory(func(ctx context.Context, listers *test.Listers, cmw configmap.Watcher) controller.Reconciler {
		r := &Reconciler{
			logger:               logging.FromContext(ctx),
			kubeClientset:        kubeclient.Get(ctx),
			adminClient:          &test.MockAdminClient{},
			environment:          test.NewEnvironment(),
			kafkachannelLister:   listers.GetKafkaChannelLister(),
			kafkachannelInformer: nil,
			deploymentLister:     listers.GetDeploymentLister(),
			serviceLister:        listers.GetServiceLister(),
			kafkaClientSet:       fakekafkaclient.Get(ctx),
		}
		return kafkachannelreconciler.NewReconciler(ctx, r.logger.Sugar(), r.kafkaClientSet, listers.GetKafkaChannelLister(), controller.GetEventRecorder(ctx), r)
	}, logger.Desugar()))
}
