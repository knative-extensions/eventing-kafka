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
	"fmt"
	"strings"
	"sync"

	"knative.dev/eventing-kafka/pkg/channel/distributed/common/config"

	apierrs "k8s.io/apimachinery/pkg/api/errors"

	"github.com/Shopify/sarama"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	appsv1listers "k8s.io/client-go/listers/apps/v1"
	corev1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	kafkav1beta1 "knative.dev/eventing-kafka/pkg/apis/messaging/v1beta1"
	"knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/admin"
	"knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/admin/types"
	kafkasarama "knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/sarama"
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/constants"
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/env"
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/event"
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/util"
	kafkaclientset "knative.dev/eventing-kafka/pkg/client/clientset/versioned"
	"knative.dev/eventing-kafka/pkg/client/injection/reconciler/messaging/v1beta1/kafkachannel"
	kafkalisters "knative.dev/eventing-kafka/pkg/client/listers/messaging/v1beta1"
	"knative.dev/eventing-kafka/pkg/common/client"
	commonconfig "knative.dev/eventing-kafka/pkg/common/config"
	commonconstants "knative.dev/eventing-kafka/pkg/common/constants"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/reconciler"
)

// Reconciler Implements controller.Reconciler for KafkaChannel Resources
type Reconciler struct {
	kubeClientset        kubernetes.Interface
	kafkaClientSet       kafkaclientset.Interface
	adminClientType      types.AdminClientType
	adminClient          types.AdminClientInterface
	environment          *env.Environment
	config               *commonconfig.EventingKafkaConfig
	saramaConfig         *sarama.Config
	kafkachannelLister   kafkalisters.KafkaChannelLister
	kafkachannelInformer cache.SharedIndexInformer
	deploymentLister     appsv1listers.DeploymentLister
	serviceLister        corev1listers.ServiceLister
	adminMutex           *sync.Mutex
	authConfig           *client.KafkaAuthConfig
	kafkaConfigMapHash   string
}

var (
	_ kafkachannel.Interface = (*Reconciler)(nil) // Verify Reconciler Implements Interface
	_ kafkachannel.Finalizer = (*Reconciler)(nil) // Verify Reconciler Implements Finalizer
)

//
// SetKafkaAdminClient Clears / Re-Sets The Kafka AdminClient On The Reconciler
//
// Ideally we would re-use the Kafka AdminClient but due to Issues with the Sarama ClusterAdmin we're
// forced to recreate a new connection every time.  We were seeing "broken-pipe" failures (non-recoverable)
// with the ClusterAdmin after periods of inactivity.
//   https://github.com/Shopify/sarama/issues/1162
//   https://github.com/Shopify/sarama/issues/866
//
// EventHub AdminClients could be reused, and this is somewhat inefficient for them, but they are very simple
// lightweight REST clients so recreating them isn't a big deal and it simplifies the code significantly to
// not have to support both use cases.
//
func (r *Reconciler) SetKafkaAdminClient(ctx context.Context) {
	r.ClearKafkaAdminClient(ctx)
	var err error
	brokers := strings.Split(r.config.Kafka.Brokers, ",")
	r.adminClient, err = admin.CreateAdminClient(ctx, brokers, r.saramaConfig, r.adminClientType)
	if err != nil {
		logger := logging.FromContext(ctx)
		logger.Error("Failed To Create Kafka AdminClient", zap.Error(err))
	}
}

// ClearKafkaAdminClient Clears (Closes) The Reconciler's Kafka AdminClient
func (r *Reconciler) ClearKafkaAdminClient(ctx context.Context) {
	if r.adminClient != nil {
		err := r.adminClient.Close()
		if err != nil {
			logger := logging.FromContext(ctx)
			logger.Error("Failed To Close Kafka AdminClient", zap.Error(err))
		}
		r.adminClient = nil
	}
}

// ReconcileKind Implements The Reconciler Interface & Is Responsible For Performing The Reconciliation (Creation)
func (r *Reconciler) ReconcileKind(ctx context.Context, channel *kafkav1beta1.KafkaChannel) reconciler.Event {

	// Get The Logger Via The Context
	logger := logging.FromContext(ctx).Desugar()
	logger.Debug("<==========  START KAFKA-CHANNEL RECONCILIATION  ==========>")

	// Verify channel is valid.
	channel.SetDefaults(ctx)
	if err := channel.Validate(ctx); err != nil {
		logger.Error("Invalid kafka channel", zap.String("channel", channel.Name), zap.Error(err))
		return err
	}

	// Add The K8S ClientSet To The Reconcile Context
	ctx = context.WithValue(ctx, kubeclient.Key{}, r.kubeClientset)
	// Add A Channel-Specific Logger To The Context
	ctx = logging.WithLogger(ctx, util.ChannelLogger(logger, channel).Sugar())

	// Don't let another goroutine clear out the admin client while we're using it in this one
	r.adminMutex.Lock()
	defer r.adminMutex.Unlock()

	// Create A New Kafka AdminClient For Each Reconciliation Attempt
	r.SetKafkaAdminClient(ctx)
	defer r.ClearKafkaAdminClient(ctx)

	// Reset The Channel's Status Conditions To Unknown (Addressable, Topic, Service, Deployment, etc...)
	channel.Status.InitializeConditions()

	// Perform The KafkaChannel Reconciliation & Handle Error Response
	logger.Info("Channel Owned By Controller - Reconciling", zap.Any("Channel.Spec", channel.Spec))
	err := r.reconcile(ctx, channel)
	if err != nil {
		logger.Error("Failed To Reconcile KafkaChannel", zap.Any("Channel", channel), zap.Error(err))
		return err
	}

	// Return Success
	logger.Info("Successfully Reconciled KafkaChannel", zap.Any("Channel", channel))
	channel.Status.ObservedGeneration = channel.Generation
	return reconciler.NewEvent(corev1.EventTypeNormal, event.KafkaChannelReconciled.String(), "KafkaChannel Reconciled Successfully: \"%s/%s\"", channel.Namespace, channel.Name)
}

// FinalizeKind Implements The Finalizer Interface & Is Responsible For Performing The Finalization (Topic Deletion)
func (r *Reconciler) FinalizeKind(ctx context.Context, channel *kafkav1beta1.KafkaChannel) reconciler.Event {

	// Get The Logger Via The Context
	logger := logging.FromContext(ctx).Desugar()
	logger.Debug("<==========  START KAFKA-CHANNEL FINALIZATION  ==========>")

	// Add The K8S ClientSet To The Reconcile Context
	ctx = context.WithValue(ctx, kubeclient.Key{}, r.kubeClientset)
	// Add A Channel-Specific Logger To The Context
	ctx = logging.WithLogger(ctx, util.ChannelLogger(logger, channel).Sugar())

	// Don't let another goroutine clear out the admin client while we're using it in this one
	r.adminMutex.Lock()
	defer r.adminMutex.Unlock()

	// Create A New Kafka AdminClient For Each Reconciliation Attempt
	r.SetKafkaAdminClient(ctx)
	defer r.ClearKafkaAdminClient(ctx)

	// Finalize The Dispatcher (Manual Finalization Due To Cross-Namespace Ownership)
	err := r.finalizeDispatcher(ctx, channel)
	if err != nil {
		logger.Info("Failed To Finalize KafkaChannel", zap.Error(err))
		return fmt.Errorf(constants.FinalizationFailedError)
	}

	// Finalize The Kafka Topic
	err = r.finalizeKafkaTopic(ctx, channel)
	if err != nil {
		logger.Error("Failed To Finalize KafkaChannel", zap.Error(err))
		return fmt.Errorf(constants.FinalizationFailedError)
	}

	// Return Success
	logger.Info("Successfully Finalized KafkaChannel")
	return reconciler.NewEvent(corev1.EventTypeNormal, event.KafkaChannelFinalized.String(), "KafkaChannel Finalized Successfully: \"%s/%s\"", channel.Namespace, channel.Name)
}

// Perform The Actual Channel Reconciliation
func (r *Reconciler) reconcile(ctx context.Context, channel *kafkav1beta1.KafkaChannel) error {

	// NOTE - The sequential order of reconciliation must be "Topic" then "Channel / Dispatcher" in order for the
	//        EventHub Cache to know the dynamically determined EventHub Namespace / Kafka Secret selected for the topic.

	// Reconcile The KafkaChannel's Kafka Topic
	err := r.reconcileKafkaTopic(ctx, channel)
	if err != nil {
		return fmt.Errorf(constants.ReconciliationFailedError)
	}

	//
	// This implementation is based on the "consolidated" KafkaChannel, and thus we're using
	// their Status tracking even though it does not align with the distributed channel's
	// architecture.  We get our Kafka configuration from the "Kafka Secrets" and not a
	// ConfigMap.  Therefore, we will instead check the Kafka Secret associated with the
	// KafkaChannel here.
	//
	if len(r.config.Kafka.AuthSecretName) > 0 {
		channel.Status.MarkConfigTrue()
	} else {
		channel.Status.MarkConfigFailed(event.KafkaSecretReconciled.String(), "No Kafka Secret For KafkaChannel")
		return fmt.Errorf(constants.ReconciliationFailedError)
	}

	// Reconcile the Receiver Deployment/Service
	secret, err := r.kubeClientset.CoreV1().Secrets(r.config.Kafka.AuthSecretNamespace).Get(ctx, r.config.Kafka.AuthSecretName, metav1.GetOptions{})
	if err != nil && apierrs.IsNotFound(err) {
		// The Receiver reconciler needs the namespace and name for various purposes, so construct a dummy Secret
		err = r.reconcileReceiver(ctx, channel, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: r.config.Kafka.AuthSecretName, Namespace: r.config.Kafka.AuthSecretNamespace}}, false)
		if err != nil {
			logging.FromContext(ctx).Error("Reconcile Receiver With Empty Secret Failed", zap.Error(err))
		}

		// Not having an auth secret for the Receiver is a problem
		channel.Status.MarkConfigFailed(event.KafkaSecretReconciled.String(), "No Kafka Secret For KafkaChannel")
		return fmt.Errorf(constants.ReconciliationFailedError)
	} else if err != nil {
		logging.FromContext(ctx).Error("Error reading receiver secret", zap.Error(err))
		return fmt.Errorf(constants.ReconciliationFailedError)
	}

	receiverError := r.reconcileReceiver(ctx, channel, secret, true)

	// Reconcile The KafkaChannel's Channel & Dispatcher Deployment/Service
	channelError := r.reconcileChannel(ctx, channel)

	dispatcherError := r.reconcileDispatcher(ctx, channel)
	if channelError != nil || dispatcherError != nil || receiverError != nil {
		return fmt.Errorf(constants.ReconciliationFailedError)
	}

	// Reconcile The KafkaChannel Itself (MetaData, etc...)
	err = r.reconcileKafkaChannel(ctx, channel)
	if err != nil {
		return fmt.Errorf(constants.ReconciliationFailedError)
	}

	// Return Success
	return nil
}

// updateKafkaConfig is the callback function that handles changes to our ConfigMap
func (r *Reconciler) updateKafkaConfig(ctx context.Context, configMap *corev1.ConfigMap) error {
	logger := logging.FromContext(ctx)

	if r == nil {
		return fmt.Errorf("reconciler is nil (possible startup race condition)")
	}

	if configMap == nil {
		return fmt.Errorf("nil configMap passed to updateKafkaConfig")
	}

	logger.Info("Reloading Kafka configuration")

	// Validate The ConfigMap Data
	if configMap.Data == nil {
		return fmt.Errorf("configMap.Data is nil")
	}

	// Enable Sarama Logging If Specified In ConfigMap
	ekConfig, err := kafkasarama.LoadEventingKafkaSettings(configMap.Data)
	if err != nil {
		return err
	} else if ekConfig == nil {
		return fmt.Errorf("eventing-kafka config is nil")
	}
	kafkasarama.EnableSaramaLogging(ekConfig.Kafka.EnableSaramaLogging)
	logger.Debug("Updated Sarama logging", zap.Bool("Kafka.EnableSaramaLogging", ekConfig.Kafka.EnableSaramaLogging))

	// Re-Read The Auth Config From The Secret As It May Have Changed
	kafkaAuthCfg, err := config.GetAuthConfigFromKubernetes(ctx, ekConfig.Kafka.AuthSecretName, ekConfig.Kafka.AuthSecretNamespace)
	if err != nil {
		logger.Error("Could Not Read New Auth Config From Secret - Keeping Existing",
			zap.String("authSecretName", ekConfig.Kafka.AuthSecretName),
			zap.String("authSecretNamespace", ekConfig.Kafka.AuthSecretNamespace), zap.Error(err))
	} else {
		if kafkaAuthCfg != nil && kafkaAuthCfg.SASL != nil && kafkaAuthCfg.SASL.User == "" {
			kafkaAuthCfg = nil // The Sarama builder expects a nil KafkaAuthConfig if no authentication is desired
		}
		r.authConfig = kafkaAuthCfg
	}

	// Get The Sarama Config Yaml From The ConfigMap
	saramaSettingsYamlString := configMap.Data[commonconstants.SaramaSettingsConfigKey]

	// Build A New Sarama Config With Auth From Secret And YAML Config From ConfigMap
	saramaConfig, err := client.NewConfigBuilder().
		WithDefaults().
		WithAuth(r.authConfig).
		FromYaml(saramaSettingsYamlString).
		Build(ctx)
	if err != nil {
		return err
	}

	logger.Info("ConfigMap Changed; Updating Sarama And Eventing-Kafka Configuration")
	r.saramaConfig = saramaConfig
	r.config = ekConfig

	r.kafkaConfigMapHash = commonconfig.ConfigmapDataCheckSum(configMap.Data)
	return nil
}
