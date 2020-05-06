package event

import (
	"testing"
)

// Test The CoreV1 EventType "Enum" String Values
func TestEventTypes(t *testing.T) {
	performEventTypeStringTest(t, KafkaChannelReconciled, "KafkaChannelReconciled")
	performEventTypeStringTest(t, KafkaChannelFinalized, "KafkaChannelFinalized")
	performEventTypeStringTest(t, ClusterChannelProvisionerReconciliationFailed, "ClusterChannelProvisionerReconciliationFailed")
	performEventTypeStringTest(t, ClusterChannelProvisionerUpdateStatusFailed, "ClusterChannelProvisionerUpdateStatusFailed")
	performEventTypeStringTest(t, KafkaChannelServiceReconciliationFailed, "KafkaChannelServiceReconciliationFailed")
	performEventTypeStringTest(t, ChannelUpdateFailed, "ChannelUpdateFailed")
	performEventTypeStringTest(t, ChannelServiceReconciliationFailed, "ChannelServiceReconciliationFailed")
	performEventTypeStringTest(t, ChannelServiceReconciliationFailed, "ChannelServiceReconciliationFailed")
	performEventTypeStringTest(t, ChannelDeploymentReconciliationFailed, "ChannelDeploymentReconciliationFailed")
	performEventTypeStringTest(t, KafkaTopicReconciliationFailed, "KafkaTopicReconciliationFailed")
	performEventTypeStringTest(t, DispatcherServiceReconciliationFailed, "DispatcherServiceReconciliationFailed")
	performEventTypeStringTest(t, DispatcherDeploymentReconciliationFailed, "DispatcherDeploymentReconciliationFailed")
	performEventTypeStringTest(t, KafkaSecretReconciled, "KafkaSecretReconciled")
	performEventTypeStringTest(t, KafkaSecretFinalized, "KafkaSecretFinalized")
}

// Perform A Single Instance Of The CoreV1 EventType String Test
func performEventTypeStringTest(t *testing.T, eventType CoreV1EventType, expectedString string) {
	actualString := eventType.String()
	if actualString != expectedString {
		t.Errorf("Expected '%s' but got '%s'", expectedString, actualString)
	}
}
