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
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"knative.dev/eventing/pkg/apis/messaging"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/reconciler"

	kafkav1beta1 "knative.dev/eventing-kafka/pkg/apis/messaging/v1beta1"
	commonk8s "knative.dev/eventing-kafka/pkg/channel/distributed/common/k8s"
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/constants"
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/util"
)

// Reconcile The KafkaChannel Itself - After Channel Reconciliation (Add MetaData)
func (r *Reconciler) reconcileKafkaChannel(ctx context.Context, channel *kafkav1beta1.KafkaChannel) error {

	// Get The Channel-Specific Logger Provided Via The Context
	logger := logging.FromContext(ctx).Desugar()

	// Reconcile The KafkaChannel's MetaData
	err := r.reconcileMetaData(ctx, channel)
	if err != nil {
		logger.Error("Failed To Reconcile KafkaChannel MetaData", zap.Error(err))
		return fmt.Errorf("failed to reconcile kafkachannel metadata")
	} else {
		logger.Info("Successfully Reconciled KafkaChannel MetaData")
		return nil // Success
	}
}

// Reconcile The KafkaChannels MetaData (Annotations, Labels, etc...)
func (r *Reconciler) reconcileMetaData(ctx context.Context, channel *kafkav1beta1.KafkaChannel) error {

	// Get The Channel-Specific Logger Provided Via The Context
	logger := logging.FromContext(ctx).Desugar()

	// Update The MetaData (Annotations, Labels, etc...)
	annotationsModified := r.reconcileAnnotations(channel)
	labelsModified := r.reconcileLabels(channel)

	// If The KafkaChannel's MetaData Was Modified
	if annotationsModified || labelsModified {

		// Then Persist Changes To Kubernetes
		_, err := r.kafkaClientSet.MessagingV1beta1().KafkaChannels(channel.Namespace).Update(ctx, channel, metav1.UpdateOptions{})
		if err != nil {
			logger.Error("Failed To Update KafkaChannel MetaData", zap.Error(err))
			return err
		} else {
			logger.Info("Successfully Updated KafkaChannel MetaData")
			return nil
		}
	} else {

		// Otherwise Nothing To Do
		logger.Info("Successfully Verified KafkaChannel MetaData")
		return nil
	}
}

// Update KafkaChannel Annotations
func (r *Reconciler) reconcileAnnotations(channel *kafkav1beta1.KafkaChannel) bool {

	// Get The KafkaChannel's Current Annotations
	annotations := channel.Annotations

	// Initialize The Annotations Map If Empty
	if annotations == nil {
		annotations = make(map[string]string)
	}

	// Track Modified Status
	modified := false

	// Add SubscribableDuckVersion Annotation If Missing Or Incorrect
	if annotations[messaging.SubscribableDuckVersionAnnotation] != constants.SubscribableDuckVersionAnnotationV1 {
		annotations[messaging.SubscribableDuckVersionAnnotation] = constants.SubscribableDuckVersionAnnotationV1
		modified = true
	}

	// Update The Channel's Annotations
	if modified {
		channel.ObjectMeta.Annotations = annotations
	}

	// Return The Modified Status
	return modified
}

// Update KafkaChannel Labels
func (r *Reconciler) reconcileLabels(channel *kafkav1beta1.KafkaChannel) bool {

	// Get The KafkaChannel's Current Labels
	labels := channel.Labels

	// Initialize The Labels Map If Empty
	if labels == nil {
		labels = make(map[string]string)
	}

	// Track Modified Status
	modified := false

	// Add Kafka Topic Label If Missing
	safeTopicName := commonk8s.TruncateLabelValue(util.TopicName(channel))
	if labels[constants.KafkaTopicLabel] != safeTopicName {
		labels[constants.KafkaTopicLabel] = safeTopicName
		modified = true
	}

	// Update The Channel's Labels
	if modified {
		channel.ObjectMeta.Labels = labels
	}

	// Return The Modified Status
	return modified
}

//
// KafkaChannel Status Reconciliation
//

// updateKafkaChannelReceiverStatus updates a KafkaChannel's Status to reflect the Receiver Service/Deployment state.
func (r *Reconciler) updateKafkaChannelReceiverStatus(ctx context.Context, originalChannel *kafkav1beta1.KafkaChannel,
	serviceValid bool, serviceReason string, serviceMessage string,
	deploymentValid bool, deploymentReason string, deploymentMessage string) error {

	// Get A KafkaChannel Logger
	logger := util.ChannelLogger(logging.FromContext(ctx).Desugar(), originalChannel)

	// Update The KafkaChannel (Retry On Conflict - KafkaChannel Controller Will Also Be Updating KafkaChannel Status)
	return reconciler.RetryUpdateConflicts(func(attempts int) error {

		var err error

		// After First Attempt - Reload The Original KafkaChannel From K8S
		if attempts > 0 {
			originalChannel, err = r.kafkachannelLister.KafkaChannels(originalChannel.Namespace).Get(originalChannel.Name)
			if err != nil {
				logger.Error("Failed To Reload KafkaChannel For Status Update", zap.Error(err))
				return err
			}
		}

		// Clone The KafkaChannel So As Not To Perturb Informers Copy
		updatedChannel := originalChannel.DeepCopy()

		// Update Service Status Based On Specified State
		if serviceValid {
			updatedChannel.Status.MarkReceiverServiceTrue()
		} else {
			updatedChannel.Status.MarkReceiverServiceFailed(serviceReason, serviceMessage)
		}

		// Update Deployment Status Based On Specified State
		if deploymentValid {
			updatedChannel.Status.MarkReceiverDeploymentTrue()
		} else {
			updatedChannel.Status.MarkReceiverDeploymentFailed(deploymentReason, deploymentMessage)
		}

		// If The KafkaChannel Status Changed
		if !equality.Semantic.DeepEqual(originalChannel.Status, updatedChannel.Status) {

			// Then Attempt To Update The KafkaChannel Status
			_, err = r.kafkaClientSet.MessagingV1beta1().KafkaChannels(updatedChannel.Namespace).UpdateStatus(ctx, updatedChannel, metav1.UpdateOptions{})
			if err != nil {
				logger.Error("Failed To Update KafkaChannel Status", zap.Error(err))
				return err
			} else {
				logger.Info("Successfully Updated KafkaChannel Status")
				return nil
			}

		} else {

			// Otherwise No Change To Status - Return Success
			logger.Info("Successfully Verified KafkaChannel Status")
			return nil
		}
	})
}
