package kafkasecret

import (
	"context"
	"fmt"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	appsv1listers "k8s.io/client-go/listers/apps/v1"
	corev1listers "k8s.io/client-go/listers/core/v1"
	"knative.dev/eventing-contrib/kafka/channel/pkg/client/clientset/versioned"
	kafkalisters "knative.dev/eventing-contrib/kafka/channel/pkg/client/listers/messaging/v1alpha1"
	"knative.dev/eventing-kafka/pkg/controller/constants"
	"knative.dev/eventing-kafka/pkg/controller/env"
	"knative.dev/eventing-kafka/pkg/controller/event"
	"knative.dev/eventing-kafka/pkg/controller/kafkasecretinjection"
	"knative.dev/pkg/reconciler"
)

// Reconciler Implements controller.Reconciler for K8S Secrets Containing Kafka Auth (Labelled)
type Reconciler struct {
	logger             *zap.Logger
	kubeClientset      kubernetes.Interface
	environment        *env.Environment
	kafkaChannelClient versioned.Interface
	kafkachannelLister kafkalisters.KafkaChannelLister
	deploymentLister   appsv1listers.DeploymentLister
	serviceLister      corev1listers.ServiceLister
}

var (
	_ kafkasecretinjection.Interface = (*Reconciler)(nil) // Verify Reconciler Implements Interface
	_ kafkasecretinjection.Finalizer = (*Reconciler)(nil) // Verify Reconciler Implements Finalizer
)

// ReconcileKind Implements The Reconciler Interface & Is Responsible For Performing The Reconciliation (Creation)
func (r *Reconciler) ReconcileKind(ctx context.Context, secret *corev1.Secret) reconciler.Event {

	// Setup Logger & Debug Log Separator
	r.logger.Debug("<==========  START KAFKA-SECRET RECONCILIATION  ==========>")
	logger := r.logger.With(zap.String("Secret", secret.Name))

	// Perform The Secret Reconciliation & Handle Error Response
	logger.Info("Secret Owned By Controller - Reconciling", zap.String("Secret", secret.Name))
	err := r.reconcile(ctx, secret)
	if err != nil {
		logger.Error("Failed To Reconcile Kafka Secret", zap.String("Secret", secret.Name), zap.Error(err))
		return err
	}

	// Return Success
	logger.Info("Successfully Reconciled Kafka Secret", zap.Any("Channel", secret.Name))
	return reconciler.NewEvent(corev1.EventTypeNormal, event.KafkaSecretReconciled.String(), "Kafka Secret Reconciled Successfully: \"%s/%s\"", secret.Namespace, secret.Name)
}

// ReconcileKind Implements The Finalizer Interface & Is Responsible For Performing The Finalization (KafkaChannel Status)
func (r *Reconciler) FinalizeKind(_ context.Context, secret *corev1.Secret) reconciler.Event {

	// Setup Logger & Debug Log Separator
	r.logger.Debug("<==========  START KAFKA-SECRET FINALIZATION  ==========>")
	logger := r.logger.With(zap.String("Secret", secret.Name))

	// Reconcile The Affected KafkaChannel Status To Indicate The Channel Service/Deployment Are Not Longer Available
	err := r.reconcileKafkaChannelStatus(secret,
		false, "ChannelServiceUnavailable", "Kafka Auth Secret Finalized",
		false, "ChannelDeploymentUnavailable", "Kafka Auth Secret Finalized")
	if err != nil {
		logger.Error("Failed To Finalize Kafka Secret - KafkaChannel Status Update Failed", zap.Error(err))
		return err
	}

	// Return Success
	logger.Info("Successfully Finalized Kafka Secret", zap.Any("Secret", secret.Name))
	return reconciler.NewEvent(corev1.EventTypeNormal, event.KafkaSecretFinalized.String(), "Kafka Secret Finalized Successfully: \"%s/%s\"", secret.Namespace, secret.Name)
}

// Perform The Actual Secret Reconciliation
func (r *Reconciler) reconcile(ctx context.Context, secret *corev1.Secret) error {

	// Perform The Kafka Secret Reconciliation
	err := r.reconcileChannel(ctx, secret)
	if err != nil {
		return fmt.Errorf(constants.ReconciliationFailedError)
	}

	// Return Success
	return nil
}
