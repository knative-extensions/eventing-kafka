package kafkachannel

import (
	"context"
	kafkaadmin "knative.dev/eventing-kafka/pkg/common/kafka/admin"
	"knative.dev/eventing-kafka/pkg/controller/constants"
	"knative.dev/eventing-kafka/pkg/controller/env"
	"knative.dev/eventing-kafka/pkg/controller/kafkasecretinformer"
	"go.uber.org/zap"
	"k8s.io/client-go/tools/cache"
	kafkachannelv1alpha1 "knative.dev/eventing-contrib/kafka/channel/pkg/apis/messaging/v1alpha1"
	kafkaclientsetinjection "knative.dev/eventing-contrib/kafka/channel/pkg/client/injection/client"
	"knative.dev/eventing-contrib/kafka/channel/pkg/client/injection/informers/messaging/v1alpha1/kafkachannel"
	kafkachannelreconciler "knative.dev/eventing-contrib/kafka/channel/pkg/client/injection/reconciler/messaging/v1alpha1/kafkachannel"
	"knative.dev/eventing/pkg/logging"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/client/injection/kube/informers/apps/v1/deployment"
	"knative.dev/pkg/client/injection/kube/informers/core/v1/service"
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"
	"os"
)

// Package Level Kafka AdminClient Reference (For Shutdown() Usage)
var adminClient kafkaadmin.AdminClientInterface

// Create A New KafkaChannel Controller
func NewController(ctx context.Context, _ configmap.Watcher) *controller.Impl {

	// Get A Logger
	logger := logging.FromContext(ctx)

	// Get The Needed Informers
	kafkachannelInformer := kafkachannel.Get(ctx)
	deploymentInformer := deployment.Get(ctx)
	serviceInformer := service.Get(ctx)
	kafkaSecretInformer := kafkasecretinformer.Get(ctx)

	// Load The Environment Variables
	environment, err := env.GetEnvironment(logger)
	if err != nil {
		logger.Panic("Failed To Load Environment Variables - Terminating!", zap.Error(err))
		os.Exit(1)
	}

	// Determine The Kafka AdminClient Type (Assume Kafka Unless Azure EventHubs Are Specified)
	kafkaAdminClientType := kafkaadmin.Kafka
	if environment.KafkaProvider == env.KafkaProviderValueAzure {
		kafkaAdminClientType = kafkaadmin.EventHub
	}

	// Get The Kafka AdminClient
	adminClient, err := kafkaadmin.CreateAdminClient(ctx, kafkaAdminClientType)
	if adminClient == nil || err != nil {
		logger.Fatal("Failed To Create Kafka AdminClient", zap.Error(err))
	}

	// Create A KafkaChannel Reconciler
	r := &Reconciler{
		logger:               logging.FromContext(ctx),
		kubeClientset:        kubeclient.Get(ctx),
		environment:          environment,
		kafkaClientSet:       kafkaclientsetinjection.Get(ctx),
		kafkachannelLister:   kafkachannelInformer.Lister(),
		kafkachannelInformer: kafkachannelInformer.Informer(),
		deploymentLister:     deploymentInformer.Lister(),
		serviceLister:        serviceInformer.Lister(),
		adminClient:          adminClient,
	}

	// Create A New KafkaChannel Controller Impl With The Reconciler
	controllerImpl := kafkachannelreconciler.NewImpl(ctx, r)

	//
	// Configure The Informers' EventHandlers
	//
	// Note - The use of EnqueueLabelOfNamespaceScopedResource() is to facilitate cross-namespace OwnerReference
	//        management and relies upon the reconciler creating the Services/Deployments with appropriate labels.
	//        Kubernetes OwnerReferences are not intended to be cross-namespace and thus don't include the namespace
	//        information.
	//
	r.logger.Info("Setting Up EventHandlers")
	kafkachannelInformer.Informer().AddEventHandler(
		controller.HandleAll(controllerImpl.Enqueue),
	)
	serviceInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: controller.FilterGroupVersionKind(kafkachannelv1alpha1.SchemeGroupVersion.WithKind(constants.KafkaChannelKind)),
		Handler:    controller.HandleAll(controllerImpl.EnqueueLabelOfNamespaceScopedResource(constants.KafkaChannelNamespaceLabel, constants.KafkaChannelNameLabel)),
	})
	deploymentInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: controller.FilterGroupVersionKind(kafkachannelv1alpha1.SchemeGroupVersion.WithKind(constants.KafkaChannelKind)),
		Handler:    controller.HandleAll(controllerImpl.EnqueueLabelOfNamespaceScopedResource(constants.KafkaChannelNamespaceLabel, constants.KafkaChannelNameLabel)),
	})
	kafkaSecretInformer.Informer().AddEventHandler(
		controller.HandleAll(r.resetKafkaAdminClient(ctx, kafkaAdminClientType)),
	)

	// Return The KafkaChannel Controller Impl
	return controllerImpl
}

// Graceful Shutdown Hook
func Shutdown() {
	if adminClient != nil {
		adminClient.Close()
	}
}

// Recreate The Kafka AdminClient On The Reconciler (Useful To Reload Cache Which Is Not Yet Exposed)
func (r *Reconciler) resetKafkaAdminClient(ctx context.Context, kafkaAdminClientType kafkaadmin.AdminClientType) func(obj interface{}) {
	return func(obj interface{}) {
		adminClient, err := kafkaadmin.CreateAdminClient(ctx, kafkaAdminClientType)
		if adminClient == nil || err != nil {
			r.logger.Error("Failed To Re-Create Kafka AdminClient", zap.Error(err))
		} else {
			r.logger.Info("Successfully Re-Created Kafka AdminClient")
			r.adminClient = adminClient
		}
	}
}
