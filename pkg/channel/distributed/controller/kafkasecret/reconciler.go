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
	"fmt"

	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	appsv1listers "k8s.io/client-go/listers/apps/v1"
	corev1listers "k8s.io/client-go/listers/core/v1"
	distributedcommonconfig "knative.dev/eventing-kafka/pkg/channel/distributed/common/config"
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/constants"
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/env"
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/event"
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/kafkasecretinjection"
	"knative.dev/eventing-kafka/pkg/client/clientset/versioned"
	kafkalisters "knative.dev/eventing-kafka/pkg/client/listers/messaging/v1beta1"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/reconciler"
)

// Reconciler Implements controller.Reconciler for K8S Secrets Containing Kafka Auth (Labelled)
type Reconciler struct {
	kubeClientset      kubernetes.Interface
	config             *distributedcommonconfig.EventingKafkaConfig
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

	// Get The Logger From The Context
	logger := logging.FromContext(ctx)

	// Setup Logger & Debug Log Separator
	logger.Debug("<==========  START KAFKA-SECRET RECONCILIATION  ==========>")
	logger = logger.With(zap.String("Secret", secret.Name))

	// Perform The Secret Reconciliation & Handle Error Response
	logger.Info("Secret Owned By Controller - Reconciling", zap.String("Secret", secret.Name))
	err := r.reconcile(ctx, secret)
	if err != nil {
		logger.Error("Failed To Reconcile Kafka Secret", zap.String("Secret", secret.Name), zap.Error(err))
		return err
	}

	// Return Success
	logger.Info("Successfully Reconciled Kafka Secret", zap.Any("Secret", secret.Name))
	return reconciler.NewEvent(corev1.EventTypeNormal, event.KafkaSecretReconciled.String(), "Kafka Secret Reconciled Successfully: \"%s/%s\"", secret.Namespace, secret.Name)
}

// ReconcileKind Implements The Finalizer Interface & Is Responsible For Performing The Finalization (KafkaChannel Status)
func (r *Reconciler) FinalizeKind(ctx context.Context, secret *corev1.Secret) reconciler.Event {
	logger := logging.FromContext(ctx)

	// Setup Logger & Debug Log Separator
	logger.Debug("<==========  START KAFKA-SECRET FINALIZATION  ==========>")
	logger = logger.With(zap.String("Secret", secret.Name))

	// Reconcile The Affected KafkaChannel Status To Indicate The Receiver Service/Deployment Is No Longer Available
	err := r.reconcileKafkaChannelStatus(ctx,
		secret,
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
