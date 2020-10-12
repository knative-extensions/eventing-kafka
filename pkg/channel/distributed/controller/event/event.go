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
	case KafkaSecretReconciled:
		eventTypeString = "KafkaSecretReconciled"
	case KafkaSecretFinalized:
		eventTypeString = "KafkaSecretFinalized"
	}

	// Return The EventType String Value
	return eventTypeString
}
