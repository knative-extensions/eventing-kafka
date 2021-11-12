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

package controller

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/multierr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	ctrl "knative.dev/control-protocol/pkg"
	ctrlmessage "knative.dev/control-protocol/pkg/message"
	"knative.dev/pkg/logging"
	logtesting "knative.dev/pkg/logging/testing"

	controllertesting "knative.dev/eventing-kafka/pkg/common/commands/resetoffset/controller/testing"
	refmapperstesting "knative.dev/eventing-kafka/pkg/common/commands/resetoffset/refmappers/testing"
	"knative.dev/eventing-kafka/pkg/common/controlprotocol"
	"knative.dev/eventing-kafka/pkg/common/controlprotocol/commands"
	controlprotocoltesting "knative.dev/eventing-kafka/pkg/common/controlprotocol/testing"
)

func TestReconciler_ReconcileDataPlaneServices(t *testing.T) {

	// Test Data
	podIp1 := "1.2.3.4"
	podIp2 := "2.3.4.5"
	podIpPort1 := fmt.Sprintf("%s:%d", podIp1, controlprotocol.ServerPort)
	podIpPort2 := fmt.Sprintf("%s:%d", podIp2, controlprotocol.ServerPort)
	podIpPorts := []string{podIpPort1, podIpPort2}
	pods := []*corev1.Pod{
		{Status: corev1.PodStatus{PodIP: podIp1}},
		{Status: corev1.PodStatus{PodIP: podIp2}},
	}
	mockDataPlaneService1 := &controlprotocoltesting.MockService{}
	mockDataPlaneService2 := &controlprotocoltesting.MockService{}
	testErr := fmt.Errorf("test-error")

	// Create A Context With Test Logger
	logger := logtesting.TestLogger(t)
	ctx := logging.WithLogger(context.Background(), logger)

	// Define The Test Cases
	tests := []struct {
		name              string
		podListerErr      error
		connectionPoolErr error
		expectedServices  map[string]ctrl.Service
		expectedErr       error
	}{
		{
			name:              "Success",
			podListerErr:      nil,
			connectionPoolErr: nil,
			expectedServices:  map[string]ctrl.Service{podIp1: mockDataPlaneService1, podIp2: mockDataPlaneService2},
			expectedErr:       nil,
		},
		{
			name:              "PodLister Error",
			podListerErr:      testErr,
			connectionPoolErr: nil,
			expectedServices:  nil,
			expectedErr:       testErr,
		},
		{
			name:              "ConnectionPool Error",
			podListerErr:      nil,
			connectionPoolErr: testErr,
			expectedServices:  nil,
			expectedErr:       testErr,
		},
	}

	// Execute The Test Cases
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {

			// Create A ResetOffset To Test
			resetOffset := controllertesting.NewResetOffset()

			// Create A RefInfo To Test
			refInfo := refmapperstesting.NewRefInfo()

			// Create A Mock PodLister
			mockPodNamespaceLister := &controllertesting.MockPodNamespaceLister{}
			mockPodNamespaceLister.On("List", labels.Set(refInfo.DataPlaneLabels).AsSelector()).Return(pods, test.podListerErr)
			mockPodLister := &controllertesting.MockPodLister{}
			mockPodLister.On("Pods", refInfo.DataPlaneNamespace).Return(mockPodNamespaceLister)

			// Create A Mock Control-Protocol ConnectionPool
			mockConnectionPool := &controlprotocoltesting.MockConnectionPool{}
			if test.podListerErr == nil {
				mockConnectionPool.On("ReconcileConnections",
					ctx,
					refInfo.ConnectionPoolKey,
					podIpPorts,
					mock.AnythingOfType("func(string, control.Service)"),
					mock.AnythingOfType("func(string)")).
					Return(test.expectedServices, test.connectionPoolErr)
			}

			// Create A Mock Control-Protocol AsyncCommandNotificationStore
			mockAsyncCommandNotificationStore := &controlprotocoltesting.MockAsyncCommandNotificationStore{}

			// Create A Reconciler To Test
			reconciler := &Reconciler{
				podLister:                     mockPodLister,
				asyncCommandNotificationStore: mockAsyncCommandNotificationStore,
				connectionPool:                mockConnectionPool,
			}

			// Perform The Test
			actualServices, actualErr := reconciler.reconcileDataPlaneServices(ctx, resetOffset, refInfo)

			// Verify The Results
			assert.Equal(t, test.expectedErr, actualErr)
			assert.Equal(t, test.expectedServices, actualServices)
			mockPodLister.AssertExpectations(t)
			mockAsyncCommandNotificationStore.AssertExpectations(t)
			mockConnectionPool.AssertExpectations(t)
		})
	}
}

func TestReconciler_StartConsumerGroups(t *testing.T) {
	performStartStopConsumerGroupAsyncCommandsTest(t, commands.StartConsumerGroupOpCode)
}

func TestReconciler_StopConsumerGroups(t *testing.T) {
	performStartStopConsumerGroupAsyncCommandsTest(t, commands.StopConsumerGroupOpCode)
}

func performStartStopConsumerGroupAsyncCommandsTest(t *testing.T, opCode ctrl.OpCode) {

	// Test Data
	reconcilerUID := types.UID(uuid.NewString())
	podIp1 := "1.2.3.4"
	podIp2 := "2.3.4.5"
	testErr := fmt.Errorf("test-error")

	// Create A Context With Test Logger
	logger := logtesting.TestLogger(t)
	ctx := logging.WithLogger(context.Background(), logger)

	// Define The Test Cases
	tests := []struct {
		name    string
		sendErr error
		result  *ctrlmessage.AsyncCommandResult
	}{
		{
			name:    "Success",
			sendErr: nil,
			result:  &ctrlmessage.AsyncCommandResult{},
		},
		{
			name:    "SendAndWaitForAck Error",
			sendErr: testErr,
			result:  nil,
		},
	}

	// Execute The Test Cases
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {

			// Create A ResetOffset To Test
			resetOffset := controllertesting.NewResetOffset()
			resetOffsetNamespacedName := types.NamespacedName{
				Namespace: resetOffset.GetNamespace(),
				Name:      resetOffset.GetName(),
			}

			// Create A RefInfo To Test
			refInfo := refmapperstesting.NewRefInfo()

			// Determine The Expected CommandIDs
			commandId1, err := GenerateCommandId(resetOffset, podIp1, opCode)
			assert.Nil(t, err)
			commandId2, err := GenerateCommandId(resetOffset, podIp2, opCode)
			assert.Nil(t, err)

			// Determine The Expected CommandLock Based On OpCode
			lockToken := GenerateLockToken(reconcilerUID, resetOffset.UID)
			var commandLock *commands.CommandLock
			switch opCode {
			case commands.StopConsumerGroupOpCode:
				commandLock = commands.NewCommandLock(lockToken, asyncCommandLockTimeout, true, false)
			case commands.StartConsumerGroupOpCode:
				commandLock = commands.NewCommandLock(lockToken, 0, false, true)
			default:
				assert.Fail(t, "Unexpected OpCode - Unable To Determine Expected CommandLock")
			}

			// Create Test ConsumerGroupAsyncCommands For Each Pod
			consumerGroupAsyncCommand1 := &commands.ConsumerGroupAsyncCommand{Version: 1, CommandId: commandId1, TopicName: refInfo.TopicName, GroupId: refInfo.GroupId, Lock: commandLock}
			consumerGroupAsyncCommand2 := &commands.ConsumerGroupAsyncCommand{Version: 1, CommandId: commandId2, TopicName: refInfo.TopicName, GroupId: refInfo.GroupId, Lock: commandLock}

			// Create A Mock Control-Protocol AsyncCommandNotificationStore & Assign To Reconciler
			mockAsyncCommandNotificationStore := &controlprotocoltesting.MockAsyncCommandNotificationStore{}
			if test.result != nil {
				mockAsyncCommandNotificationStore.On("GetCommandResult", resetOffsetNamespacedName, podIp1, consumerGroupAsyncCommand1).Return(test.result)
				mockAsyncCommandNotificationStore.On("GetCommandResult", resetOffsetNamespacedName, podIp2, consumerGroupAsyncCommand2).Return(test.result)
			}

			// Create The Mock Services To Test Against
			mockDataPlaneService1 := &controlprotocoltesting.MockService{}
			mockDataPlaneService2 := &controlprotocoltesting.MockService{}
			mockDataPlaneService1.On("SendAndWaitForAck", opCode, consumerGroupAsyncCommand1).Return(test.sendErr)
			mockDataPlaneService2.On("SendAndWaitForAck", opCode, consumerGroupAsyncCommand2).Return(test.sendErr)
			services := map[string]ctrl.Service{podIp1: mockDataPlaneService1, podIp2: mockDataPlaneService2}

			// Create A Reconciler To Test
			reconciler := &Reconciler{
				uid:                           reconcilerUID,
				asyncCommandNotificationStore: mockAsyncCommandNotificationStore,
			}

			// Perform The Test Based On OpCode
			var actualErr error
			switch opCode {
			case commands.StartConsumerGroupOpCode:
				actualErr = reconciler.startConsumerGroups(ctx, resetOffset, services, refInfo)
			case commands.StopConsumerGroupOpCode:
				actualErr = reconciler.stopConsumerGroups(ctx, resetOffset, services, refInfo)
			default:
				assert.Fail(t, "Unexpected OpCode - Unable To Determine Test Operation")
			}

			// Setup Expected Errors Based On Test
			var expectedErrs []error
			if test.sendErr != nil {
				expectedErrs = []error{
					fmt.Errorf("failed to send ConsumerGroup AsyncCommand '%d': %v", commandId1, testErr),
					fmt.Errorf("failed to send ConsumerGroup AsyncCommand '%d': %v", commandId2, testErr),
				}
			}

			// Verify The Results
			if expectedErrs == nil {
				assert.Nil(t, actualErr)
			} else {
				for _, expectedErr := range expectedErrs {
					assert.Contains(t, multierr.Errors(actualErr), expectedErr)
				}
			}
			mockAsyncCommandNotificationStore.AssertExpectations(t)
			mockDataPlaneService1.AssertExpectations(t)
			mockDataPlaneService2.AssertExpectations(t)
		})
	}
}
