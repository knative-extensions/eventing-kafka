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
	performEventTypeStringTest(t, ReceiverServiceReconciliationFailed, "ReceiverServiceReconciliationFailed")
	performEventTypeStringTest(t, ReceiverServiceReconciliationFailed, "ReceiverServiceReconciliationFailed")
	performEventTypeStringTest(t, ReceiverDeploymentReconciliationFailed, "ReceiverDeploymentReconciliationFailed")
	performEventTypeStringTest(t, ReceiverServicePatched, "ReceiverServicePatched")
	performEventTypeStringTest(t, ReceiverDeploymentUpdated, "ReceiverDeploymentUpdated")
	performEventTypeStringTest(t, ReceiverServicePatchFailed, "ReceiverServicePatchFailed")
	performEventTypeStringTest(t, ReceiverDeploymentUpdateFailed, "ReceiverDeploymentUpdateFailed")
	performEventTypeStringTest(t, ChannelStatusReconciliationFailed, "ChannelStatusReconciliationFailed")
	performEventTypeStringTest(t, KafkaTopicReconciliationFailed, "KafkaTopicReconciliationFailed")
	performEventTypeStringTest(t, DispatcherServiceReconciliationFailed, "DispatcherServiceReconciliationFailed")
	performEventTypeStringTest(t, DispatcherDeploymentReconciliationFailed, "DispatcherDeploymentReconciliationFailed")
	performEventTypeStringTest(t, DispatcherServiceFinalizationFailed, "DispatcherServiceFinalizationFailed")
	performEventTypeStringTest(t, DispatcherDeploymentFinalizationFailed, "DispatcherDeploymentFinalizationFailed")
	performEventTypeStringTest(t, DispatcherDeploymentUpdated, "DispatcherDeploymentUpdated")
	performEventTypeStringTest(t, DispatcherDeploymentUpdateFailed, "DispatcherDeploymentUpdateFailed")
	performEventTypeStringTest(t, DispatcherServicePatched, "DispatcherServicePatched")
	performEventTypeStringTest(t, DispatcherServicePatchFailed, "DispatcherServicePatchFailed")
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
