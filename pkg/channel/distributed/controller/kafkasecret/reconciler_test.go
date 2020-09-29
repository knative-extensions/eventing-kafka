package kafkasecret

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/scheme"
	clientgotesting "k8s.io/client-go/testing"
	kafkav1beta1 "knative.dev/eventing-kafka/pkg/apis/messaging/v1beta1"
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/event"
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/kafkasecretinjection"
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/test"
	fakekafkaclient "knative.dev/eventing-kafka/pkg/client/injection/client/fake"
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
			Key:  test.KafkaSecretKey,
			Objects: []runtime.Object{
				test.NewKafkaSecret(),
			},
			WantCreates: []runtime.Object{
				test.NewKafkaChannelChannelService(),
				test.NewKafkaChannelChannelDeployment(),
			},
			WantPatches: []clientgotesting.PatchActionImpl{test.NewKafkaSecretFinalizerPatchActionImpl()},
			WantEvents: []string{
				test.NewKafkaSecretFinalizerUpdateEvent(),
				test.NewKafkaSecretSuccessfulReconciliationEvent(),
			},
		},
		{
			Name: "Complete Reconciliation With KafkaChannel",
			Key:  test.KafkaSecretKey,
			Objects: []runtime.Object{
				test.NewKafkaSecret(),
				test.NewKafkaChannel(),
			},
			WantCreates: []runtime.Object{
				test.NewKafkaChannelChannelService(),
				test.NewKafkaChannelChannelDeployment(),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: test.NewKafkaChannel(
						test.WithChannelServiceReady,
						test.WithChannelDeploymentReady,
					),
				},
			},
			WantPatches: []clientgotesting.PatchActionImpl{test.NewKafkaSecretFinalizerPatchActionImpl()},
			WantEvents: []string{
				test.NewKafkaSecretFinalizerUpdateEvent(),
				test.NewKafkaSecretSuccessfulReconciliationEvent(),
			},
		},

		//
		// KafkaChannel Secret Deletion (Finalizer)
		//

		{
			Name: "Finalize Deleted Secret",
			Key:  test.KafkaSecretKey,
			Objects: []runtime.Object{
				test.NewKafkaSecret(test.WithKafkaSecretDeleted),
				test.NewKafkaChannel(
					test.WithChannelServiceReady,
					test.WithChannelDeploymentReady,
				),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: test.NewKafkaChannel(
						test.WithChannelServiceFinalized,
						test.WithChannelDeploymentFinalized,
					),
				},
			},
			WantEvents: []string{
				test.NewKafkaSecretSuccessfulFinalizedEvent(),
			},
		},

		//
		// KafkaChannel Channel Service
		//

		{
			Name: "Reconcile Missing Channel Service Success",
			Key:  test.KafkaSecretKey,
			Objects: []runtime.Object{
				test.NewKafkaSecret(test.WithKafkaSecretFinalizer),
				test.NewKafkaChannel(
					test.WithChannelServiceReady,
					test.WithChannelDeploymentReady,
				),
				test.NewKafkaChannelChannelDeployment(),
			},
			WantCreates: []runtime.Object{test.NewKafkaChannelChannelService()},
			WantEvents: []string{
				test.NewKafkaSecretSuccessfulReconciliationEvent(),
			},
		},
		{
			Name: "Reconcile Missing Channel Service Error(Create)",
			Key:  test.KafkaSecretKey,
			Objects: []runtime.Object{
				test.NewKafkaSecret(test.WithKafkaSecretFinalizer),
				test.NewKafkaChannel(
					test.WithChannelServiceReady,
					test.WithChannelDeploymentReady,
				),
				test.NewKafkaChannelService(),
				test.NewKafkaChannelChannelDeployment(),
				test.NewKafkaChannelDispatcherService(),
				test.NewKafkaChannelDispatcherDeployment(),
			},
			WithReactors: []clientgotesting.ReactionFunc{InduceFailure("create", "services")},
			WantErr:      true,
			WantCreates:  []runtime.Object{test.NewKafkaChannelChannelService()},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: test.NewKafkaChannel(
						test.WithChannelServiceFailed,
						test.WithChannelDeploymentReady,
					),
				},
			},
			WantEvents: []string{
				Eventf(corev1.EventTypeWarning, event.ChannelServiceReconciliationFailed.String(), "Failed To Reconcile Channel Service: inducing failure for create services"),
				test.NewKafkaSecretFailedReconciliationEvent(),
			},
		},

		//
		// KafkaChannel Channel Deployment
		//

		{
			Name: "Reconcile Missing Channel Deployment Success",
			Key:  test.KafkaSecretKey,
			Objects: []runtime.Object{
				test.NewKafkaSecret(test.WithKafkaSecretFinalizer),
				test.NewKafkaChannel(
					test.WithChannelServiceReady,
					test.WithChannelDeploymentReady,
				),
				test.NewKafkaChannelService(),
				test.NewKafkaChannelChannelService(),
			},
			WantCreates: []runtime.Object{test.NewKafkaChannelChannelDeployment()},
			WantEvents:  []string{test.NewKafkaSecretSuccessfulReconciliationEvent()},
		},
		{
			Name: "Reconcile Missing Channel Deployment Error(Create)",
			Key:  test.KafkaSecretKey,
			Objects: []runtime.Object{
				test.NewKafkaSecret(test.WithKafkaSecretFinalizer),
				test.NewKafkaChannel(
					test.WithChannelServiceReady,
					test.WithChannelDeploymentReady,
				),
				test.NewKafkaChannelService(),
				test.NewKafkaChannelChannelService(),
			},
			WithReactors: []clientgotesting.ReactionFunc{InduceFailure("create", "deployments")},
			WantErr:      true,
			WantCreates:  []runtime.Object{test.NewKafkaChannelChannelDeployment()},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: test.NewKafkaChannel(
						test.WithChannelServiceReady,
						test.WithChannelDeploymentFailed,
					),
				},
			},
			WantEvents: []string{
				Eventf(corev1.EventTypeWarning, event.ChannelDeploymentReconciliationFailed.String(), "Failed To Reconcile Channel Deployment: inducing failure for create deployments"),
				test.NewKafkaSecretFailedReconciliationEvent(),
			},
		},
	}

	// Run The TableTest Using The KafkaChannel Reconciler Provided By The Factory
	logger := logtesting.TestLogger(t)
	tableTest.Test(t, test.MakeFactory(func(ctx context.Context, listers *test.Listers, cmw configmap.Watcher) controller.Reconciler {
		r := &Reconciler{
			logger:             logging.FromContext(ctx).Desugar(),
			kubeClientset:      kubeclient.Get(ctx),
			environment:        test.NewEnvironment(),
			config:             test.NewConfig(),
			kafkaChannelClient: fakekafkaclient.Get(ctx),
			kafkachannelLister: listers.GetKafkaChannelLister(),
			deploymentLister:   listers.GetDeploymentLister(),
			serviceLister:      listers.GetServiceLister(),
		}
		return kafkasecretinjection.NewReconciler(ctx, r.logger.Sugar(), r.kubeClientset.CoreV1(), listers.GetSecretLister(), controller.GetEventRecorder(ctx), r)
	}, logger.Desugar()))
}
