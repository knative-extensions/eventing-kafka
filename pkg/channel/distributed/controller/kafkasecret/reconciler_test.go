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
	controllertesting "knative.dev/eventing-kafka/pkg/channel/distributed/controller/testing"
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
			Key:  controllertesting.KafkaSecretKey,
			Objects: []runtime.Object{
				controllertesting.NewKafkaSecret(),
			},
			WantCreates: []runtime.Object{
				controllertesting.NewKafkaChannelChannelService(),
				controllertesting.NewKafkaChannelChannelDeployment(),
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
				controllertesting.NewKafkaChannelChannelService(),
				controllertesting.NewKafkaChannelChannelDeployment(),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: controllertesting.NewKafkaChannel(
						controllertesting.WithChannelServiceReady,
						controllertesting.WithChannelDeploymentReady,
					),
				},
			},
			WantPatches: []clientgotesting.PatchActionImpl{controllertesting.NewKafkaSecretFinalizerPatchActionImpl()},
			WantEvents: []string{
				controllertesting.NewKafkaSecretFinalizerUpdateEvent(),
				controllertesting.NewKafkaSecretSuccessfulReconciliationEvent(),
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
					controllertesting.WithChannelServiceReady,
					controllertesting.WithChannelDeploymentReady,
				),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: controllertesting.NewKafkaChannel(
						controllertesting.WithChannelServiceFinalized,
						controllertesting.WithChannelDeploymentFinalized,
					),
				},
			},
			WantEvents: []string{
				controllertesting.NewKafkaSecretSuccessfulFinalizedEvent(),
			},
		},

		//
		// KafkaChannel Channel Service
		//

		{
			Name: "Reconcile Missing Channel Service Success",
			Key:  controllertesting.KafkaSecretKey,
			Objects: []runtime.Object{
				controllertesting.NewKafkaSecret(controllertesting.WithKafkaSecretFinalizer),
				controllertesting.NewKafkaChannel(
					controllertesting.WithChannelServiceReady,
					controllertesting.WithChannelDeploymentReady,
				),
				controllertesting.NewKafkaChannelChannelDeployment(),
			},
			WantCreates: []runtime.Object{controllertesting.NewKafkaChannelChannelService()},
			WantEvents: []string{
				controllertesting.NewKafkaSecretSuccessfulReconciliationEvent(),
			},
		},
		{
			Name: "Reconcile Missing Channel Service Error(Create)",
			Key:  controllertesting.KafkaSecretKey,
			Objects: []runtime.Object{
				controllertesting.NewKafkaSecret(controllertesting.WithKafkaSecretFinalizer),
				controllertesting.NewKafkaChannel(
					controllertesting.WithChannelServiceReady,
					controllertesting.WithChannelDeploymentReady,
				),
				controllertesting.NewKafkaChannelService(),
				controllertesting.NewKafkaChannelChannelDeployment(),
				controllertesting.NewKafkaChannelDispatcherService(),
				controllertesting.NewKafkaChannelDispatcherDeployment(),
			},
			WithReactors: []clientgotesting.ReactionFunc{InduceFailure("create", "services")},
			WantErr:      true,
			WantCreates:  []runtime.Object{controllertesting.NewKafkaChannelChannelService()},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: controllertesting.NewKafkaChannel(
						controllertesting.WithChannelServiceFailed,
						controllertesting.WithChannelDeploymentReady,
					),
				},
			},
			WantEvents: []string{
				Eventf(corev1.EventTypeWarning, event.ChannelServiceReconciliationFailed.String(), "Failed To Reconcile Channel Service: inducing failure for create services"),
				controllertesting.NewKafkaSecretFailedReconciliationEvent(),
			},
		},

		//
		// KafkaChannel Channel Deployment
		//

		{
			Name: "Reconcile Missing Channel Deployment Success",
			Key:  controllertesting.KafkaSecretKey,
			Objects: []runtime.Object{
				controllertesting.NewKafkaSecret(controllertesting.WithKafkaSecretFinalizer),
				controllertesting.NewKafkaChannel(
					controllertesting.WithChannelServiceReady,
					controllertesting.WithChannelDeploymentReady,
				),
				controllertesting.NewKafkaChannelService(),
				controllertesting.NewKafkaChannelChannelService(),
			},
			WantCreates: []runtime.Object{controllertesting.NewKafkaChannelChannelDeployment()},
			WantEvents:  []string{controllertesting.NewKafkaSecretSuccessfulReconciliationEvent()},
		},
		{
			Name: "Reconcile Missing Channel Deployment Error(Create)",
			Key:  controllertesting.KafkaSecretKey,
			Objects: []runtime.Object{
				controllertesting.NewKafkaSecret(controllertesting.WithKafkaSecretFinalizer),
				controllertesting.NewKafkaChannel(
					controllertesting.WithChannelServiceReady,
					controllertesting.WithChannelDeploymentReady,
				),
				controllertesting.NewKafkaChannelService(),
				controllertesting.NewKafkaChannelChannelService(),
			},
			WithReactors: []clientgotesting.ReactionFunc{InduceFailure("create", "deployments")},
			WantErr:      true,
			WantCreates:  []runtime.Object{controllertesting.NewKafkaChannelChannelDeployment()},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: controllertesting.NewKafkaChannel(
						controllertesting.WithChannelServiceReady,
						controllertesting.WithChannelDeploymentFailed,
					),
				},
			},
			WantEvents: []string{
				Eventf(corev1.EventTypeWarning, event.ChannelDeploymentReconciliationFailed.String(), "Failed To Reconcile Channel Deployment: inducing failure for create deployments"),
				controllertesting.NewKafkaSecretFailedReconciliationEvent(),
			},
		},
	}

	// Run The TableTest Using The KafkaChannel Reconciler Provided By The Factory
	logger := logtesting.TestLogger(t)
	tableTest.Test(t, controllertesting.MakeFactory(func(ctx context.Context, listers *controllertesting.Listers, cmw configmap.Watcher) controller.Reconciler {
		r := &Reconciler{
			logger:             logging.FromContext(ctx).Desugar(),
			kubeClientset:      kubeclient.Get(ctx),
			environment:        controllertesting.NewEnvironment(),
			config:             controllertesting.NewConfig(),
			kafkaChannelClient: fakekafkaclient.Get(ctx),
			kafkachannelLister: listers.GetKafkaChannelLister(),
			deploymentLister:   listers.GetDeploymentLister(),
			serviceLister:      listers.GetServiceLister(),
		}
		return kafkasecretinjection.NewReconciler(ctx, r.logger.Sugar(), r.kubeClientset.CoreV1(), listers.GetSecretLister(), controller.GetEventRecorder(ctx), r)
	}, logger.Desugar()))
}
