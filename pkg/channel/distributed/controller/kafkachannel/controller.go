package kafkachannel

import (
	"context"
	"sync"

	"go.uber.org/zap"
	"k8s.io/client-go/tools/cache"
	kafkachannelv1beta1 "knative.dev/eventing-kafka/pkg/apis/messaging/v1beta1"
	commonconfig "knative.dev/eventing-kafka/pkg/channel/distributed/common/config"
	kafkaadmin "knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/admin"
	"knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/sarama"
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/constants"
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/env"
	"knative.dev/eventing-kafka/pkg/channel/distributed/dispatcher/config"
	kafkaclientsetinjection "knative.dev/eventing-kafka/pkg/client/injection/client"
	"knative.dev/eventing-kafka/pkg/client/injection/informers/messaging/v1beta1/kafkachannel"
	kafkachannelreconciler "knative.dev/eventing-kafka/pkg/client/injection/reconciler/messaging/v1beta1/kafkachannel"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/client/injection/kube/informers/apps/v1/deployment"
	"knative.dev/pkg/client/injection/kube/informers/core/v1/service"
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"
)

// Track The Reconciler For Shutdown() Usage
var rec *Reconciler

// Create A New KafkaChannel Controller
func NewController(ctx context.Context, _ configmap.Watcher) *controller.Impl {

	// Get A Logger
	logger := logging.FromContext(ctx).Desugar()

	// Get The Needed Informers
	kafkachannelInformer := kafkachannel.Get(ctx)
	deploymentInformer := deployment.Get(ctx)
	serviceInformer := service.Get(ctx)

	// Load The Environment Variables
	environment, err := env.GetEnvironment(logger)
	if err != nil {
		logger.Fatal("Failed To Load Environment Variables - Terminating!", zap.Error(err))
	}

	// Load the Sarama and other eventing-kafka settings from our configmap
	saramaConfig, configuration, err := sarama.LoadSettings(ctx)
	if err != nil {
		logger.Fatal("Failed To Load Eventing-Kafka Settings", zap.Error(err))
	}

	// Verify that our loaded configuration is valid
	if err = config.VerifyConfiguration(configuration); err != nil {
		logger.Fatal("Invalid / Missing Settings - Terminating", zap.Error(err))
	}

	// Determine The Kafka AdminClient Type (Assume Kafka Unless Otherwise Specified)
	var kafkaAdminClientType kafkaadmin.AdminClientType
	switch configuration.Kafka.AdminType {
	case constants.KafkaAdminTypeValueKafka:
		kafkaAdminClientType = kafkaadmin.Kafka
	case constants.KafkaAdminTypeValueAzure:
		kafkaAdminClientType = kafkaadmin.EventHub
	case constants.KafkaAdminTypeValueCustom:
		kafkaAdminClientType = kafkaadmin.Custom
	default:
		logger.Warn("Encountered Unexpected Kafka AdminType - Defaulting To 'kafka'", zap.String("AdminType", configuration.Kafka.AdminType))
		kafkaAdminClientType = kafkaadmin.Kafka
	}

	// Create A KafkaChannel Reconciler & Track As Package Variable
	rec = &Reconciler{
		logger:               logger,
		kubeClientset:        kubeclient.Get(ctx),
		environment:          environment,
		config:               configuration,
		saramaConfig:         saramaConfig,
		kafkaClientSet:       kafkaclientsetinjection.Get(ctx),
		kafkachannelLister:   kafkachannelInformer.Lister(),
		kafkachannelInformer: kafkachannelInformer.Informer(),
		deploymentLister:     deploymentInformer.Lister(),
		serviceLister:        serviceInformer.Lister(),
		adminClientType:      kafkaAdminClientType,
		adminClient:          nil,
		adminMutex:           &sync.Mutex{},
		configObserver:       rec.configMapObserver, // Maintains a reference so that the ConfigWatcher can call it
	}

	// Watch The Settings ConfigMap For Changes
	err = commonconfig.InitializeConfigWatcher(ctx, logger.Sugar(), rec.configMapObserver)
	if err != nil {
		logger.Fatal("Failed To Initialize ConfigMap Watcher", zap.Error(err))
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
