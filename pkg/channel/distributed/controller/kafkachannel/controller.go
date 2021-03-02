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
	"sync"

	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
	kafkachannelv1beta1 "knative.dev/eventing-kafka/pkg/apis/messaging/v1beta1"
	"knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/admin/types"
	clientconstants "knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/constants"
	"knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/sarama"
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/constants"
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/env"
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/util"
	kafkaclientsetinjection "knative.dev/eventing-kafka/pkg/client/injection/client"
	"knative.dev/eventing-kafka/pkg/client/injection/informers/messaging/v1beta1/kafkachannel"
	kafkachannelreconciler "knative.dev/eventing-kafka/pkg/client/injection/reconciler/messaging/v1beta1/kafkachannel"
	commonclient "knative.dev/eventing-kafka/pkg/common/client"
	commonconfig "knative.dev/eventing-kafka/pkg/common/config"
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
	environment, err := env.FromContext(ctx)
	if err != nil {
		logger.Fatal("Failed To Get Environment From Context - Terminating!", zap.Error(err))
	}

	// Get The K8S Client
	kubeClientset := kubeclient.Get(ctx)

	// Get & Validate The Kafka Auth Secret
	kafkaSecret, err := util.GetKafkaSecret(ctx, kubeClientset, environment.SystemNamespace)
	if err != nil {
		logger.Fatal("Failed To Load Kafka Auth Secret", zap.Error(err))
	} else if kafkaSecret == nil {
		logger.Fatal("No Kafka Auth Secret Found")
	} else {
		logger.Info("Found Valid Kafka Auth Secret")
	}

	// Extract The Relevant Data From The Kafka Secret
	kafkaSecretName := kafkaSecret.Name
	kafkaUsername := string(kafkaSecret.Data[clientconstants.KafkaSecretKeyUsername])
	kafkaPassword := string(kafkaSecret.Data[clientconstants.KafkaSecretKeyPassword])
	kafkaSaslType := string(kafkaSecret.Data[clientconstants.KafkaSecretKeySaslType])

	// Create A Kafka Auth Config From Current Credentials (Secret Data Takes Precedence Over ConfigMap)
	var kafkaAuthCfg *commonclient.KafkaAuthConfig
	if kafkaUsername != "" {
		kafkaAuthCfg = &commonclient.KafkaAuthConfig{
			SASL: &commonclient.KafkaSaslConfig{
				User:     kafkaUsername,
				Password: kafkaPassword,
				SaslType: kafkaSaslType,
			},
		}
	}

	// Load the Sarama and other eventing-kafka settings from our configmap
	saramaConfig, configuration, err := sarama.LoadSettings(ctx, "", kafkaAuthCfg)
	if err != nil {
		logger.Fatal("Failed To Load Eventing-Kafka Settings", zap.Error(err))
	}

	// Enable Sarama Logging If Specified In ConfigMap
	sarama.EnableSaramaLogging(configuration.Kafka.EnableSaramaLogging)

	// Determine The Kafka AdminClient Type (Assume Kafka Unless Otherwise Specified)
	var kafkaAdminClientType types.AdminClientType
	switch configuration.Kafka.AdminType {
	case constants.KafkaAdminTypeValueKafka:
		kafkaAdminClientType = types.Kafka
	case constants.KafkaAdminTypeValueAzure:
		kafkaAdminClientType = types.EventHub
	case constants.KafkaAdminTypeValueCustom:
		kafkaAdminClientType = types.Custom
	default:
		logger.Warn("Encountered Unexpected Kafka AdminType - Defaulting To 'kafka'", zap.String("AdminType", configuration.Kafka.AdminType))
		kafkaAdminClientType = types.Kafka
	}

	// Create A KafkaChannel Reconciler & Track As Package Variable
	rec = &Reconciler{
		kubeClientset:        kubeClientset,
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
		kafkaSecret:          kafkaSecretName,
		kafkaBrokers:         configuration.Kafka.Brokers,
		kafkaUsername:        kafkaUsername,
		kafkaPassword:        kafkaPassword,
		kafkaSaslType:        kafkaSaslType,
	}

	// Watch The Settings ConfigMap For Changes
	err = commonconfig.InitializeConfigWatcher(ctx, logger.Sugar(), rec.configMapObserver, environment.SystemNamespace)
	if err != nil {
		logger.Fatal("Failed To Initialize ConfigMap Watcher", zap.Error(err))
	}

	// Create A New KafkaChannel Controller Impl With The Reconciler
	controllerImpl := kafkachannelreconciler.NewImpl(ctx, rec)

	//
	// Configure The Informers' EventHandlers
	//
	// Note - The use of FilterKafkaChannelOwnerByReferenceOrLabel() and EnqueueLabelOfNamespaceScopedResource()
	//        is to facilitate cross-namespace owner relationships and relies upon the reconciler creating
	//        the Services/Deployments with appropriate labels. Kubernetes does NOT support cross-namespace
	//        OwnerReferences, and so we use "marker" labels to identify them instead.
	//
	logger.Info("Setting Up EventHandlers")
	kafkachannelInformer.Informer().AddEventHandler(
		controller.HandleAll(controllerImpl.Enqueue),
	)
	serviceInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: FilterKafkaChannelOwnerByReferenceOrLabel(),
		Handler:    controller.HandleAll(controllerImpl.EnqueueLabelOfNamespaceScopedResource(constants.KafkaChannelNamespaceLabel, constants.KafkaChannelNameLabel)),
	})
	deploymentInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: FilterKafkaChannelOwnerByReferenceOrLabel(),
		Handler:    controller.HandleAll(controllerImpl.EnqueueLabelOfNamespaceScopedResource(constants.KafkaChannelNamespaceLabel, constants.KafkaChannelNameLabel)),
	})

	// Return The KafkaChannel Controller Impl
	return controllerImpl
}

//
// FilterWithKafkaChannelLabels - Custom Filter For Common K8S Components "Owned" By KafkaChannels
//
// This function is similar to, and based on, the various knative.dev/pkg/controller/FilterXYZ
// functions.  It is used to filter common Kubernetes objects (Service, Deployment, etc) owned by
// KafkaChannels using either a K8S OwnerReference (preferred), or Name/Namespace marker labels.
// This secondary support for such marker labels is necessary to work around the need for
// Cross-Namespace OwnerReferences which are not supported by K8S.
//
func FilterKafkaChannelOwnerByReferenceOrLabel() func(obj interface{}) bool {
	return func(obj interface{}) bool {

		// Validate The Object
		if object, ok := obj.(metav1.Object); ok {

			// Use The Controller's OwnerReference If Present
			owner := metav1.GetControllerOf(object)
			if owner != nil {
				gvk := kafkachannelv1beta1.SchemeGroupVersion.WithKind(constants.KafkaChannelKind)
				return owner.APIVersion == gvk.GroupVersion().String() && owner.Kind == gvk.Kind
			}

			// Otherwise Failover To KafkaChannel Name/Namespace Labels
			labels := object.GetLabels()
			return len(labels[constants.KafkaChannelNameLabel]) > 0 && len(labels[constants.KafkaChannelNamespaceLabel]) > 0
		}

		// Exclude Invalid Object
		return false
	}
}

// Graceful Shutdown Hook
func Shutdown() {
	rec.ClearKafkaAdminClient(context.Background())
}
