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

package event

// CoreV1 EventType "Enum" Type
type CoreV1EventType int

// CoreV1 EventType "Enum" Values
const (
	// KafkaChannel Reconciler/Finalizer
	KafkaChannelReconciled CoreV1EventType = iota
	KafkaChannelFinalized

	// ClusterChannelProvisioner Reconciliation
	ClusterChannelProvisionerReconciliationFailed
	ClusterChannelProvisionerUpdateStatusFailed

	// KafkaChannel Service (In User Namespace)
	KafkaChannelServiceReconciliationFailed

	// Channel Updates (Finalizers, Status)
	ChannelUpdateFailed
	ChannelStatusReconciliationFailed

	// Receiver (Kafka Producer) Reconciliation
	ReceiverServiceReconciliationFailed
	ReceiverDeploymentReconciliationFailed

	// Kafka Topic Reconciliation
	KafkaTopicReconciliationFailed

	// Dispatcher (Kafka Consumer) Reconciliation
	DispatcherServiceReconciliationFailed
	DispatcherDeploymentReconciliationFailed
	DispatcherServiceFinalizationFailed
	DispatcherDeploymentFinalizationFailed
	DispatcherDeploymentUpdated
	DispatcherDeploymentUpdateFailed

	// Kafka Secret Reconciliation
	KafkaSecretReconciled
	KafkaSecretFinalized
)

// CoreV1 EventType String Value
func (et CoreV1EventType) String() string {

	// Default The EventType String Value
	eventTypeString := "Unknown Event Type"

	// Map EventTypes To Their String Values
	switch et {
	case KafkaChannelReconciled:
		eventTypeString = "KafkaChannelReconciled"
	case KafkaChannelFinalized:
		eventTypeString = "KafkaChannelFinalized"
	case ClusterChannelProvisionerReconciliationFailed:
		eventTypeString = "ClusterChannelProvisionerReconciliationFailed"
	case ClusterChannelProvisionerUpdateStatusFailed:
		eventTypeString = "ClusterChannelProvisionerUpdateStatusFailed"
	case KafkaChannelServiceReconciliationFailed:
		eventTypeString = "KafkaChannelServiceReconciliationFailed"
	case ChannelUpdateFailed:
		eventTypeString = "ChannelUpdateFailed"
	case ReceiverServiceReconciliationFailed:
		eventTypeString = "ReceiverServiceReconciliationFailed"
	case ReceiverDeploymentReconciliationFailed:
		eventTypeString = "ReceiverDeploymentReconciliationFailed"
	case ChannelStatusReconciliationFailed:
		eventTypeString = "ChannelStatusReconciliationFailed"
	case KafkaTopicReconciliationFailed:
		eventTypeString = "KafkaTopicReconciliationFailed"
	case DispatcherServiceReconciliationFailed:
		eventTypeString = "DispatcherServiceReconciliationFailed"
	case DispatcherDeploymentReconciliationFailed:
		eventTypeString = "DispatcherDeploymentReconciliationFailed"
	case DispatcherServiceFinalizationFailed:
		eventTypeString = "DispatcherServiceFinalizationFailed"
	case DispatcherDeploymentFinalizationFailed:
		eventTypeString = "DispatcherDeploymentFinalizationFailed"
	case DispatcherDeploymentUpdated:
		eventTypeString = "DispatcherDeploymentUpdated"
	case DispatcherDeploymentUpdateFailed:
		eventTypeString = "DispatcherDeploymentUpdateFailed"
	case KafkaSecretReconciled:
		eventTypeString = "KafkaSecretReconciled"
	case KafkaSecretFinalized:
		eventTypeString = "KafkaSecretFinalized"
	}

	// Return The EventType String Value
	return eventTypeString
}
