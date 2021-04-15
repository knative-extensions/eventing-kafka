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

	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
	"knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/sarama"
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/config"
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/constants"
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/env"
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/kafkasecretinformer"
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/kafkasecretinjection"
	injectionclient "knative.dev/eventing-kafka/pkg/client/injection/client"
	"knative.dev/eventing-kafka/pkg/client/injection/informers/messaging/v1beta1/kafkachannel"
	commonconfig "knative.dev/eventing-kafka/pkg/common/config"
	"knative.dev/eventing-kafka/pkg/common/configmaploader"
	commonconstants "knative.dev/eventing-kafka/pkg/common/constants"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/client/injection/kube/informers/apps/v1/deployment"
	"knative.dev/pkg/client/injection/kube/informers/core/v1/service"
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"
)

// Create A New KafkaSecret Controller
func NewController(ctx context.Context, cmw configmap.Watcher) *controller.Impl {

	// Get A Logger
	logger := logging.FromContext(ctx).Desugar()

	// Get The Needed Informers
	kafkaSecretInformer := kafkasecretinformer.Get(ctx)
	kafkachannelInformer := kafkachannel.Get(ctx)
	deploymentInformer := deployment.Get(ctx)
	serviceInformer := service.Get(ctx)

	// Load The Environment Variables
	environment, err := env.FromContext(ctx)
	if err != nil {
		logger.Fatal("Failed To Get Environment From Context - Terminating!", zap.Error(err))
	}

	// Get the configmap loader
	configmapLoader, err := configmaploader.FromContext(ctx)
	if err != nil {
		logger.Fatal("Failed To Get ConfigmapLoader From Context - Terminating!", zap.Error(err))
	}

	configMap, err := configmapLoader(commonconstants.SettingsConfigMapMountPath)
	if err != nil {
		logger.Fatal("error loading configuration", zap.Error(err))
	}

	// Load the Sarama and other eventing-kafka settings from our configmap
	// (though we don't need the Sarama settings here; the AdminClient loads them from the configmap each time it needs them)
	_, configuration, err := sarama.LoadSettings(ctx, "", configMap, nil)
	if err != nil {
		logger.Fatal("Failed To Load Eventing-Kafka Settings", zap.Error(err))
	}

	// Verify that our loaded configuration is valid
	if err = config.VerifyConfiguration(configuration); err != nil {
		logger.Fatal("Invalid / Missing Settings - Terminating", zap.Error(err))
	}

	// Create The KafkaSecret Reconciler
	r := &Reconciler{
		kubeClientset:      kubeclient.Get(ctx),
		config:             configuration,
		environment:        environment,
		kafkaChannelClient: injectionclient.Get(ctx),
		kafkachannelLister: kafkachannelInformer.Lister(),
		deploymentLister:   deploymentInformer.Lister(),
		serviceLister:      serviceInformer.Lister(),
		kafkaConfigMapHash: commonconfig.ConfigmapDataCheckSum(configMap),
	}

	// Create A New KafkaSecret Controller Impl With The Reconciler
	controllerImpl := kafkasecretinjection.NewImpl(ctx, r)

	// Call GlobalResync on kafkachannels.
	grCh := func(obj interface{}) {
		logger.Info("Changes detected, doing global resync")
		controllerImpl.GlobalResync(kafkachannelInformer.Informer())
	}

	handleKafkaConfigMapChange := func(ctx context.Context, configMap *corev1.ConfigMap) {
		logger.Info("Configmap is updated or, it is being read for the first time")
		r.updateKafkaConfig(ctx, configMap)
		grCh(configMap)
	}

	// Watch The Settings ConfigMap For Changes
	err = commonconfig.InitializeKafkaConfigMapWatcher(ctx, cmw, logger.Sugar(), handleKafkaConfigMapChange, environment.SystemNamespace)
	if err != nil {
		logger.Fatal("Failed To Initialize ConfigMap Watcher", zap.Error(err))
	}

	// Configure The Informers' EventHandlers
	logger.Info("Setting Up EventHandlers")
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
		controller.HandleAll(enqueueSecretOfKafkaChannel(controllerImpl, environment.SystemNamespace)),
	)

	// Return The KafkaSecret Controller Impl
	return controllerImpl
}

// Graceful Shutdown Hook
func Shutdown() {
	// Nothing To Cleanup
}

// Enqueue The Kafka Secret Associated With The Specified KafkaChannel
func enqueueSecretOfKafkaChannel(controller *controller.Impl, namespace string) func(obj interface{}) {
	return func(obj interface{}) {
		if object, ok := obj.(metav1.Object); ok {
			labels := object.GetLabels()
			if len(labels) > 0 {
				secretName := labels[constants.KafkaSecretLabel]
				if len(secretName) > 0 {
					controller.EnqueueKey(types.NamespacedName{
						Namespace: namespace,
						Name:      secretName,
					})
				}
			}
		}
	}
}
