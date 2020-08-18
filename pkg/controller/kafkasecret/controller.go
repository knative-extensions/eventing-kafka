package kafkasecret

import (
	"context"

	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
	injectionclient "knative.dev/eventing-contrib/kafka/channel/pkg/client/injection/client"
	"knative.dev/eventing-contrib/kafka/channel/pkg/client/injection/informers/messaging/v1beta1/kafkachannel"
	commonconfig "knative.dev/eventing-kafka/pkg/common/config"
	"knative.dev/eventing-kafka/pkg/controller/constants"
	"knative.dev/eventing-kafka/pkg/controller/env"
	"knative.dev/eventing-kafka/pkg/controller/kafkasecretinformer"
	"knative.dev/eventing-kafka/pkg/controller/kafkasecretinjection"
	"knative.dev/eventing/pkg/logging"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/client/injection/kube/informers/apps/v1/deployment"
	"knative.dev/pkg/client/injection/kube/informers/core/v1/service"
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"
)

// Create A New KafkaSecret Controller
func NewController(ctx context.Context, _ configmap.Watcher) *controller.Impl {

	// Get A Logger
	logger := logging.FromContext(ctx)

	// Get The Needed Informers
	kafkaSecretInformer := kafkasecretinformer.Get(ctx)
	kafkachannelInformer := kafkachannel.Get(ctx)
	deploymentInformer := deployment.Get(ctx)
	serviceInformer := service.Get(ctx)

	// Load The Environment Variables
	environment, err := env.GetEnvironment(logger)
	if err != nil {
		logger.Fatal("Failed To Load Environment Variables - Terminating!", zap.Error(err))
	}

	// Load the Sarama and other eventing-kafka settings from our configmap
	// (though we don't need the Sarama settings here; the AdminClient loads them from the configmap each time it needs them)
	_, ekConfig, err := commonconfig.LoadEventingKafkaSettings(ctx)
	if err != nil {
		logger.Fatal("Failed To Load Eventing-Kafka Settings", zap.Error(err))
	}

	// Overwrite configmap settings with anything provided by the environment
	if err = env.VerifyOverrides(ekConfig, environment); err != nil {
		logger.Fatal("Invalid / Missing Settings - Terminating", zap.Error(err))
	}

	// Create The KafkaSecret Reconciler
	r := &Reconciler{
		logger:             logging.FromContext(ctx),
		kubeClientset:      kubeclient.Get(ctx),
		config:             ekConfig,
		kafkaChannelClient: injectionclient.Get(ctx),
		kafkachannelLister: kafkachannelInformer.Lister(),
		deploymentLister:   deploymentInformer.Lister(),
		serviceLister:      serviceInformer.Lister(),
	}

	// Create A New KafkaSecret Controller Impl With The Reconciler
	controllerImpl := kafkasecretinjection.NewImpl(ctx, r)

	// Configure The Informers' EventHandlers
	r.logger.Info("Setting Up EventHandlers")
	kafkaSecretInformer.Informer().AddEventHandler(
		controller.HandleAll(controllerImpl.Enqueue),
	)
	serviceInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: controller.FilterControllerGVK(corev1.SchemeGroupVersion.WithKind(constants.SecretKind)),
		Handler:    controller.HandleAll(controllerImpl.EnqueueControllerOf),
	})
	deploymentInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: controller.FilterControllerGVK(corev1.SchemeGroupVersion.WithKind(constants.SecretKind)),
		Handler:    controller.HandleAll(controllerImpl.EnqueueControllerOf),
	})
	kafkachannelInformer.Informer().AddEventHandler(
		controller.HandleAll(enqueueSecretOfKafkaChannel(controllerImpl)),
	)

	// Return The KafkaSecret Controller Impl
	return controllerImpl
}

// Graceful Shutdown Hook
func Shutdown() {
	// Nothing To Cleanup
}

// Enqueue The Kafka Secret Associated With The Specified KafkaChannel
func enqueueSecretOfKafkaChannel(controller *controller.Impl) func(obj interface{}) {
	return func(obj interface{}) {
		if object, ok := obj.(metav1.Object); ok {
			labels := object.GetLabels()
			if len(labels) > 0 {
				secretName := labels[constants.KafkaSecretLabel]
				if len(secretName) > 0 {
					controller.EnqueueKey(types.NamespacedName{
						Namespace: constants.KnativeEventingNamespace,
						Name:      secretName,
					})
				}
			}
		}
	}
}
