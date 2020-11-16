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
	commonconfig "knative.dev/eventing-kafka/pkg/channel/distributed/common/config"
	"knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/sarama"
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/constants"
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/env"
	kafkaclientsetinjection "knative.dev/eventing-kafka/pkg/client/injection/client"
	"knative.dev/eventing-kafka/pkg/client/injection/informers/messaging/v1beta1/kafkachannel"
	kafkachannelreconciler "knative.dev/eventing-kafka/pkg/client/injection/reconciler/messaging/v1beta1/kafkachannel"
	kafkaadmin "knative.dev/eventing-kafka/pkg/common/kafka/admin"
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

	// TODO - new - get from secret (incorporate into LoadSettings() above, etc...)
	username := "" // Empty for quick strimzi test
	password := "" // Empty for quick strimzi test
	sarama.UpdateSaramaConfig(saramaConfig, constants.ControllerComponentName, username, password)

	// Enable Sarama Logging If Specified In ConfigMap
	sarama.EnableSaramaLogging(configuration.Kafka.EnableSaramaLogging)

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
	// Note - The use of FilterKafkaChannelOwnerByReferenceOrLabel() and EnqueueLabelOfNamespaceScopedResource()
	//        is to facilitate cross-namespace owner relationships and relies upon the reconciler creating
	//        the Services/Deployments with appropriate labels. Kubernetes does NOT support cross-namespace
	//        OwnerReferences, and so we use "marker" labels to identify them instead.
	//
	rec.logger.Info("Setting Up EventHandlers")
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

/* TODO - sample code for loading username/password from secret - find a home for it ; ) ...
//// Get The K8S Client From The Context
//k8sClient := kubeclient.Get(ctx)
//
//// Get A List Of The Kafka Secrets
//kafkaSecrets, err := adminutil.GetKafkaSecrets(ctx, k8sClient, namespace)
//if err != nil {
//	logger.Error("Failed To Get Kafka Authentication Secrets", zap.Error(err))
//	return nil, err
//}
//
//// Currently Only Support One Kafka Secret - Invalid AdminClient For All Other Cases!
//var kafkaSecret corev1.Secret
//if len(kafkaSecrets.Items) != 1 {
//	logger.Warn(fmt.Sprintf("Expected 1 Kafka Secret But Found %d - Kafka AdminClient Will Not Be Functional!", len(kafkaSecrets.Items)))
//	return &KafkaAdminClient{logger: logger, namespace: namespace, clientId: clientId}, nil
//} else {
//	logger.Info("Found 1 Kafka Secret", zap.String("Secret", kafkaSecrets.Items[0].Name))
//	kafkaSecret = kafkaSecrets.Items[0]
//}
//
//// Validate Secret Data
//if !adminutil.ValidateKafkaSecret(logger, &kafkaSecret) {
//	err = errors.New("invalid Kafka Secret found")
//	return nil, err
//}
//
//// Extract The Relevant Data From The Kafka Secret
//brokers := strings.Split(string(kafkaSecret.Data[constants.KafkaSecretKeyBrokers]), ",")
//username := string(kafkaSecret.Data[constants.KafkaSecretKeyUsername])
//password := string(kafkaSecret.Data[constants.KafkaSecretKeyPassword])
//
//// Update The Sarama ClusterAdmin Configuration With Our Values
//kafkasarama.UpdateSaramaConfig(saramaConfig, clientId, username, password)
*/

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
	rec.ClearKafkaAdminClient()
}
