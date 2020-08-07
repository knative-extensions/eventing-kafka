package kafkachannel

import (
	"context"
	"go.uber.org/zap"
	"k8s.io/client-go/tools/cache"
	kafkachannelv1beta1 "knative.dev/eventing-contrib/kafka/channel/pkg/apis/messaging/v1beta1"
	kafkaclientsetinjection "knative.dev/eventing-contrib/kafka/channel/pkg/client/injection/client"
	"knative.dev/eventing-contrib/kafka/channel/pkg/client/injection/informers/messaging/v1beta1/kafkachannel"
	kafkachannelreconciler "knative.dev/eventing-contrib/kafka/channel/pkg/client/injection/reconciler/messaging/v1beta1/kafkachannel"
	kafkaadmin "knative.dev/eventing-kafka/pkg/common/kafka/admin"
	"knative.dev/eventing-kafka/pkg/controller/constants"
	"knative.dev/eventing-kafka/pkg/controller/env"
	"knative.dev/eventing/pkg/logging"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/client/injection/kube/informers/apps/v1/deployment"
	"knative.dev/pkg/client/injection/kube/informers/core/v1/service"
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"
)

// Track The Reconciler For Shutdown() Usage
var rec *Reconciler

// Create A New KafkaChannel Controller
func NewController(ctx context.Context, _ configmap.Watcher) *controller.Impl {

	// Get A Logger
	logger := logging.FromContext(ctx)

	// Get The Needed Informers
	kafkachannelInformer := kafkachannel.Get(ctx)
	deploymentInformer := deployment.Get(ctx)
	serviceInformer := service.Get(ctx)

	// Load The Environment Variables
	environment, err := env.GetEnvironment(logger)
	if err != nil {
		logger.Fatal("Failed To Load Environment Variables - Terminating!", zap.Error(err))
	}

	// Determine The Kafka AdminClient Type (Assume Kafka Unless Azure EventHubs Are Specified)
	kafkaAdminClientType := kafkaadmin.Kafka
	if environment.KafkaProvider == env.KafkaProviderValueAzure {
		kafkaAdminClientType = kafkaadmin.EventHub
	}

	// Create A KafkaChannel Reconciler & Track As Package Variable
	rec = &Reconciler{
		logger:               logging.FromContext(ctx),
		kubeClientset:        kubeclient.Get(ctx),
		environment:          environment,
		kafkaClientSet:       kafkaclientsetinjection.Get(ctx),
		kafkachannelLister:   kafkachannelInformer.Lister(),
		kafkachannelInformer: kafkachannelInformer.Informer(),
		deploymentLister:     deploymentInformer.Lister(),
		serviceLister:        serviceInformer.Lister(),
		adminClientType:      kafkaAdminClientType,
		adminClient:          nil,
	}

	// Create A New KafkaChannel Controller Impl With The Reconciler
	controllerImpl := kafkachannelreconciler.NewImpl(ctx, rec)

	//
	// Configure The Informers' EventHandlers
	//
	// Note - The use of EnqueueLabelOfNamespaceScopedResource() is to facilitate cross-namespace OwnerReference
	//        management and relies upon the reconciler creating the Services/Deployments with appropriate labels.
	//        Kubernetes OwnerReferences are not intended to be cross-namespace and thus don't include the namespace
	//        information.
	//
	rec.logger.Info("Setting Up EventHandlers")
	kafkachannelInformer.Informer().AddEventHandler(
		controller.HandleAll(controllerImpl.Enqueue),
	)
	serviceInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: controller.FilterControllerGVK(kafkachannelv1beta1.SchemeGroupVersion.WithKind(constants.KafkaChannelKind)),
		Handler:    controller.HandleAll(controllerImpl.EnqueueLabelOfNamespaceScopedResource(constants.KafkaChannelNamespaceLabel, constants.KafkaChannelNameLabel)),
	})
	deploymentInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: controller.FilterControllerGVK(kafkachannelv1beta1.SchemeGroupVersion.WithKind(constants.KafkaChannelKind)),
		Handler:    controller.HandleAll(controllerImpl.EnqueueLabelOfNamespaceScopedResource(constants.KafkaChannelNamespaceLabel, constants.KafkaChannelNameLabel)),
	})

	// Return The KafkaChannel Controller Impl
	return controllerImpl
}

// Graceful Shutdown Hook
func Shutdown() {
	rec.ClearKafkaAdminClient()
}
