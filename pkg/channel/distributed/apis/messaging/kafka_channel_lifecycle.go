/*
Copyright 2021 The Knative Authors

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

package messaging

import (
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"knative.dev/pkg/apis"

	messaging "knative.dev/eventing-kafka/pkg/apis/messaging/v1beta1"
)

/*
 * Each KafkaChannel implementation will require its own unique set of
 * Status Conditions in order to accurately reflect the state of its
 * runtime artifacts.  Therefore, these conditions are defined here
 * instead of the standard /apis/.../kafka_channel_lifecycle.go location.
 */

const (

	// KafkaChannelConditionReceiverServiceReady has status True when the Receiver's K8S Service is ready.
	// This basically just means it exists because there's no meaningful status in Service.
	KafkaChannelConditionReceiverServiceReady apis.ConditionType = "ReceiverServiceReady"

	// KafkaChannelConditionReceiverDeploymentReady has status True when the Receiver's K8S Deployment is ready.
	KafkaChannelConditionReceiverDeploymentReady apis.ConditionType = "ReceiverDeploymentReady"

	// KafkaChannelConditionDispatcherServiceReady has status True when the Dispatcher's K8S Service is ready.
	// This basically just means it exists because there's no meaningful status in Service.
	// Note - Dispatcher Service is only exposed for Metrics collection and is not an active part of the data-plane.
	KafkaChannelConditionDispatcherServiceReady apis.ConditionType = "DispatcherServiceReady"

	// KafkaChannelConditionDispatcherDeploymentReady has status True when the Receiver's K8S Deployment is ready.
	KafkaChannelConditionDispatcherDeploymentReady apis.ConditionType = "DispatcherDeploymentReady"
)

// RegisterDistributedKafkaChannelConditionSet initializes the ConditionSet to those pertaining to the distributed KafkaChannel.
func RegisterDistributedKafkaChannelConditionSet() {
	messaging.RegisterAlternateKafkaChannelConditionSet(
		apis.NewLivingConditionSet(
			messaging.KafkaChannelConditionAddressable,
			messaging.KafkaChannelConditionConfigReady,
			messaging.KafkaChannelConditionTopicReady,
			messaging.KafkaChannelConditionChannelServiceReady,
			KafkaChannelConditionReceiverServiceReady,
			KafkaChannelConditionReceiverDeploymentReady,
			KafkaChannelConditionDispatcherServiceReady,
			KafkaChannelConditionDispatcherDeploymentReady,
		),
	)
}

func MarkReceiverServiceFailed(kcs *messaging.KafkaChannelStatus, reason, messageFormat string, messageA ...interface{}) {
	kcs.GetConditionSet().Manage(kcs).MarkFalse(KafkaChannelConditionReceiverServiceReady, reason, messageFormat, messageA...)
}

func MarkReceiverServiceTrue(kcs *messaging.KafkaChannelStatus) {
	kcs.GetConditionSet().Manage(kcs).MarkTrue(KafkaChannelConditionReceiverServiceReady)
}

func MarkReceiverDeploymentFailed(kcs *messaging.KafkaChannelStatus, reason, messageFormat string, messageA ...interface{}) {
	kcs.GetConditionSet().Manage(kcs).MarkFalse(KafkaChannelConditionReceiverDeploymentReady, reason, messageFormat, messageA...)
}

func MarkReceiverDeploymentTrue(kcs *messaging.KafkaChannelStatus) {
	kcs.GetConditionSet().Manage(kcs).MarkTrue(KafkaChannelConditionReceiverDeploymentReady)
}

func MarkDispatcherServiceFailed(kcs *messaging.KafkaChannelStatus, reason, messageFormat string, messageA ...interface{}) {
	kcs.GetConditionSet().Manage(kcs).MarkFalse(KafkaChannelConditionDispatcherServiceReady, reason, messageFormat, messageA...)
}

func MarkDispatcherServiceTrue(kcs *messaging.KafkaChannelStatus) {
	kcs.GetConditionSet().Manage(kcs).MarkTrue(KafkaChannelConditionDispatcherServiceReady)
}

func MarkDispatcherServiceUnknown(kcs *messaging.KafkaChannelStatus, reason, messageFormat string, messageA ...interface{}) {
	kcs.GetConditionSet().Manage(kcs).MarkUnknown(KafkaChannelConditionDispatcherServiceReady, reason, messageFormat, messageA...)
}

func MarkDispatcherDeploymentFailed(kcs *messaging.KafkaChannelStatus, reason, messageFormat string, messageA ...interface{}) {
	kcs.GetConditionSet().Manage(kcs).MarkFalse(KafkaChannelConditionDispatcherDeploymentReady, reason, messageFormat, messageA...)
}

func MarkDispatcherDeploymentUnknown(kcs *messaging.KafkaChannelStatus, reason, messageFormat string, messageA ...interface{}) {
	kcs.GetConditionSet().Manage(kcs).MarkUnknown(KafkaChannelConditionDispatcherDeploymentReady, reason, messageFormat, messageA...)
}

func PropagateDispatcherDeploymentStatus(kcs *messaging.KafkaChannelStatus, ds *appsv1.DeploymentStatus) {
	for _, cond := range ds.Conditions {
		if cond.Type == appsv1.DeploymentAvailable {
			if cond.Status == corev1.ConditionTrue {
				kcs.GetConditionSet().Manage(kcs).MarkTrue(KafkaChannelConditionDispatcherDeploymentReady)
			} else if cond.Status == corev1.ConditionFalse {
				MarkDispatcherDeploymentFailed(kcs, "DispatcherDeploymentFalse", "The status of Dispatcher Deployment is False: %s : %s", cond.Reason, cond.Message)
			} else if cond.Status == corev1.ConditionUnknown {
				MarkDispatcherDeploymentUnknown(kcs, "DispatcherDeploymentUnknown", "The status of Dispatcher Deployment is Unknown: %s : %s", cond.Reason, cond.Message)
			}
		}
	}
}
