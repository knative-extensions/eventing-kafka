package kafkachannel

import (
	"context"
	"fmt"
	kafkaadmin "github.com/kyma-incubator/knative-kafka/pkg/common/kafka/admin"
	"github.com/kyma-incubator/knative-kafka/pkg/controller/constants"
	"github.com/kyma-incubator/knative-kafka/pkg/controller/env"
	"github.com/kyma-incubator/knative-kafka/pkg/controller/event"
	"github.com/kyma-incubator/knative-kafka/pkg/controller/util"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	appsv1listers "k8s.io/client-go/listers/apps/v1"
	corev1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	kafkav1alpha1 "knative.dev/eventing-contrib/kafka/channel/pkg/apis/messaging/v1alpha1"
	kafkaclientset "knative.dev/eventing-contrib/kafka/channel/pkg/client/clientset/versioned"
	"knative.dev/eventing-contrib/kafka/channel/pkg/client/injection/reconciler/messaging/v1alpha1/kafkachannel"
	kafkalisters "knative.dev/eventing-contrib/kafka/channel/pkg/client/listers/messaging/v1alpha1"
	"knative.dev/pkg/reconciler"
)

// Reconciler Implements controller.Reconciler for KafkaChannel Resources
type Reconciler struct {
	logger               *zap.Logger
	kubeClientset        kubernetes.Interface
	kafkaClientSet       kafkaclientset.Interface
	adminClient          kafkaadmin.AdminClientInterface
	environment          *env.Environment
	kafkachannelLister   kafkalisters.KafkaChannelLister
	kafkachannelInformer cache.SharedIndexInformer
	deploymentLister     appsv1listers.DeploymentLister
	serviceLister        corev1listers.ServiceLister
}

var (
	_ kafkachannel.Interface = (*Reconciler)(nil) // Verify Reconciler Implements Interface
	_ kafkachannel.Finalizer = (*Reconciler)(nil) // Verify Reconciler Implements Finalizer
)

// ReconcileKind Implements The Reconciler Interface & Is Responsible For Performing The Reconciliation (Creation)
func (r *Reconciler) ReconcileKind(ctx context.Context, channel *kafkav1alpha1.KafkaChannel) reconciler.Event {

	r.logger.Debug("<==========  START KAFKA-CHANNEL RECONCILIATION  ==========>")

	// Reset The Channel's Status Conditions To Unknown (Addressable, Topic, Service, Deployment, etc...)
	channel.Status.InitializeConditions()

	// Perform The KafkaChannel Reconciliation & Handle Error Response
	r.logger.Info("Channel Owned By Controller - Reconciling", zap.Any("Channel.Spec", channel.Spec))
	err := r.reconcile(ctx, channel)
	if err != nil {
		r.logger.Error("Failed To Reconcile KafkaChannel", zap.Any("Channel", channel), zap.Error(err))
		return err
	}

	// Return Success
	r.logger.Info("Successfully Reconciled KafkaChannel", zap.Any("Channel", channel))
	channel.Status.ObservedGeneration = channel.Generation
	return reconciler.NewEvent(corev1.EventTypeNormal, event.KafkaChannelReconciled.String(), "KafkaChannel Reconciled Successfully: \"%s/%s\"", channel.Namespace, channel.Name)
}

// ReconcileKind Implements The Finalizer Interface & Is Responsible For Performing The Finalization (Topic Deletion)
func (r *Reconciler) FinalizeKind(ctx context.Context, channel *kafkav1alpha1.KafkaChannel) reconciler.Event {

	r.logger.Debug("<==========  START KAFKA-CHANNEL FINALIZATION  ==========>")

	// Get The Kafka Topic Name For Specified Channel
	topicName := util.TopicName(channel)

	// Delete The Kafka Topic & Handle Error Response
	err := r.deleteTopic(ctx, topicName)
	if err != nil {
		r.logger.Error("Failed To Finalize KafkaChannel", zap.Any("Channel", channel), zap.Error(err))
		return err
	}

	// Return Success
	r.logger.Info("Successfully Finalized KafkaChannel", zap.Any("Channel", channel))
	return reconciler.NewEvent(corev1.EventTypeNormal, event.KafkaChannelFinalized.String(), "KafkaChannel Finalized Successfully: \"%s/%s\"", channel.Namespace, channel.Name)
}

// Perform The Actual Channel Reconciliation
func (r *Reconciler) reconcile(ctx context.Context, channel *kafkav1alpha1.KafkaChannel) error {

	// NOTE - The sequential order of reconciliation must be "Topic" then "Channel / Dispatcher" in order for the
	//        EventHub Cache to know the dynamically determined EventHub Namespace / Kafka Secret selected for the topic.

	// Reconcile The KafkaChannel's Kafka Topic
	err := r.reconcileTopic(ctx, channel)
	if err != nil {
		return fmt.Errorf(constants.ReconciliationFailedError)
	}

	//
	// This implementation is based on the eventing-contrib KafkaChannel, and thus we're using
	// their Status tracking even though it does not align with our architecture.  We get our
	// Kafka configuration from the "Kafka Secrets" and not a ConfigMap.  Therefore, we will
	// instead check the Kafka Secret associated with the KafkaChannel here.
	//
	if len(r.adminClient.GetKafkaSecretName(util.TopicName(channel))) > 0 {
		channel.Status.MarkConfigTrue()
	} else {
		channel.Status.MarkConfigFailed(event.KafkaSecretReconciled.String(), "No Kafka Secret For KafkaChannel")
		return fmt.Errorf(constants.ReconciliationFailedError)
	}

	// Reconcile The KafkaChannel's Channel & Dispatcher Deployment/Service
	channelError := r.reconcileChannel(ctx, channel)
	dispatcherError := r.reconcileDispatcher(ctx, channel)
	if channelError != nil || dispatcherError != nil {
		return fmt.Errorf(constants.ReconciliationFailedError)
	}

	// Add Labels To KafkaChannel
	err = r.reconcileKafkaChannel(ctx, channel)
	if err != nil {
		return fmt.Errorf(constants.ReconciliationFailedError)
	}

	// Return Success
	return nil
}
