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

	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kafkav1beta1 "knative.dev/eventing-kafka/pkg/apis/messaging/v1beta1"
	kafkautil "knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/util"
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/constants"
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/event"
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/util"
	"knative.dev/pkg/apis"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/network"
)

// Reconcile The "Channel" Inbound For The Specified Channel
func (r *Reconciler) reconcileChannel(ctx context.Context, channel *kafkav1beta1.KafkaChannel) error {

	// Get The Channel-Specific Logger Provided Via The Context
	logger := logging.FromContext(ctx).Desugar()

	// Reconcile The KafkaChannel's Service
	err := r.reconcileKafkaChannelService(ctx, logger, channel)
	if err != nil {
		controller.GetEventRecorder(ctx).Eventf(channel, corev1.EventTypeWarning, event.KafkaChannelServiceReconciliationFailed.String(), "Failed To Reconcile KafkaChannel Service: %v", err)
		logger.Error("Failed To Reconcile KafkaChannel Service", zap.Error(err))
		return fmt.Errorf("failed to reconcile channel resources")
	} else {
		logger.Info("Successfully Reconciled KafkaChannel Service")
		return nil // Success
	}
}

//
// KafkaChannel Kafka Channel Service
//
// One K8S Service per KafkaChannel, in the same namespace as the KafkaChannel, with an
// ExternalName reference to the single K8S Service in the knative-eventing namespace
// for the Channel Deployment/Pods.
//

// Reconcile The KafkaChannel Service
func (r *Reconciler) reconcileKafkaChannelService(ctx context.Context, logger *zap.Logger, channel *kafkav1beta1.KafkaChannel) error {

	// Attempt To Get The Service Associated With The Specified Channel
	service, err := r.getKafkaChannelService(channel)
	if service == nil || err != nil {

		// If The Service Was Not Found - Then Create A New One For The Channel
		if errors.IsNotFound(err) {
			logger.Info("KafkaChannel Service Not Found - Creating New One")
			service = r.newKafkaChannelService(channel)
			service, err = r.kubeClientset.CoreV1().Services(service.Namespace).Create(ctx, service, metav1.CreateOptions{})
			if err != nil {
				logger.Error("Failed To Create KafkaChannel Service", zap.Error(err))
				channel.Status.MarkChannelServiceFailed(event.KafkaChannelServiceReconciliationFailed.String(), "Failed To Create KafkaChannel Service: %v", err)
				return err
			} else {
				logger.Info("Successfully Created KafkaChannel Service")
				// Continue To Update Channel Status
			}
		} else {
			logger.Error("Failed To Get KafkaChannel Service", zap.Error(err))
			channel.Status.MarkChannelServiceFailed(event.KafkaChannelServiceReconciliationFailed.String(), "Failed To Get KafkaChannel Service: %v", err)
			return err
		}
	} else {

		// Verify KafkaChannel Service Is Not Terminating
		if service.DeletionTimestamp.IsZero() {
			logger.Info("Successfully Verified KafkaChannel Service")
			// Continue To Update Channel Status
		} else {
			logger.Warn("Encountered KafkaChannel Service With DeletionTimestamp - Forcing Reconciliation", zap.String("Namespace", service.Namespace), zap.String("Name", service.Name))
			return fmt.Errorf("encountered KafkaChannel Service with DeletionTimestamp %s/%s - potential race condition", service.Namespace, service.Name)
		}
	}

	// Update Channel Status
	channel.Status.MarkChannelServiceTrue()
	channel.Status.SetAddress(&apis.URL{
		Scheme: "http",
		Host:   network.GetServiceHostname(service.Name, service.Namespace),
	})

	// Return Success
	return nil
}

// Get The KafkaChannel Service Associated With The Specified Channel
func (r *Reconciler) getKafkaChannelService(channel *kafkav1beta1.KafkaChannel) (*corev1.Service, error) {

	// Get The KafkaChannel Service Name
	serviceName := kafkautil.AppendKafkaChannelServiceNameSuffix(channel.Name)

	// Get The Service By Namespace / Name
	service, err := r.serviceLister.Services(channel.Namespace).Get(serviceName)

	// Return The Results
	return service, err
}

// Create KafkaChannel Service Model For The Specified Channel
func (r *Reconciler) newKafkaChannelService(channel *kafkav1beta1.KafkaChannel) *corev1.Service {

	// Get The KafkaChannel Service Name
	serviceName := kafkautil.AppendKafkaChannelServiceNameSuffix(channel.Name)

	// Get The Receiver Service Name For The Kafka Secret (One Receiver Service Per Kafka Secret)
	deploymentName := util.ReceiverDnsSafeName(r.kafkaSecret)
	serviceAddress := network.GetServiceHostname(deploymentName, r.environment.SystemNamespace)

	// Create & Return The Service Model
	return &corev1.Service{
		TypeMeta: metav1.TypeMeta{
			APIVersion: corev1.SchemeGroupVersion.String(),
			Kind:       constants.ServiceKind,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      serviceName,       // Must Match KafkaChannel For HOST Parsing In Channel Implementation!
			Namespace: channel.Namespace, // Must Match KafkaChannel For HOST Parsing In Channel Implementation!
			Labels: map[string]string{
				constants.KafkaChannelReceiverLabel:  "true",                               // Identifies the Service as being a KafkaChannel "Channel"
				constants.KafkaChannelNameLabel:      channel.Name,                         // Identifies the Service's Owning KafkaChannel's Name
				constants.KafkaChannelNamespaceLabel: channel.Namespace,                    // Identifies the Service's Owning KafkaChannel's Namespace
				constants.K8sAppChannelSelectorLabel: constants.K8sAppChannelSelectorValue, // Prometheus ServiceMonitor
			},
			OwnerReferences: []metav1.OwnerReference{
				util.NewChannelOwnerReference(channel),
			},
		},
		Spec: corev1.ServiceSpec{
			Type:         corev1.ServiceTypeExternalName,
			ExternalName: serviceAddress,
		},
	}
}
