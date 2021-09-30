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

package consumer

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	ctrl "knative.dev/control-protocol/pkg"
	ctrlservice "knative.dev/control-protocol/pkg/service"
	logtesting "knative.dev/pkg/logging/testing"

	"knative.dev/eventing-kafka/pkg/common/controlprotocol/commands"
	controltesting "knative.dev/eventing-kafka/pkg/common/controlprotocol/testing"
	kafkatesting "knative.dev/eventing-kafka/pkg/common/kafka/testing"
)

func TestNewConsumerGroupManager(t *testing.T) {
	server := getMockServerHandler()
	manager := NewConsumerGroupManager(logtesting.TestLogger(t).Desugar(), server, []string{}, &sarama.Config{}, &NoopConsumerOffsetInitializer{})
	assert.NotNil(t, manager)
	assert.NotNil(t, server.Router[commands.StopConsumerGroupOpCode])
	assert.NotNil(t, server.Router[commands.StartConsumerGroupOpCode])
	server.AssertExpectations(t)
}

func TestReconfigure(t *testing.T) {
	defer restoreNewConsumerGroup(newConsumerGroup)

	for _, testCase := range []struct {
		name       string
		groupId    string
		factoryErr bool
		closeErr   error
		expectErr  string
	}{
		{
			name: "No managed groups",
		},
		{
			name:    "One managed group",
			groupId: "test-id1",
		},
		{
			name:      "Error stopping groups",
			groupId:   "test-id1",
			closeErr:  fmt.Errorf("error closing consumer group"),
			expectErr: "error closing consumer group",
		},
		{
			name:       "Error starting groups",
			groupId:    "test-id1",
			factoryErr: true,
			expectErr:  "factory error",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			manager, group, _, server := getManagerWithMockGroup(t, testCase.groupId, testCase.factoryErr)
			if group != nil {
				group.On("Close").Return(testCase.closeErr)
			}
			err := manager.Reconfigure([]string{"new-broker"}, &sarama.Config{})
			if testCase.expectErr == "" {
				assert.Nil(t, err)
			} else {
				assert.NotNil(t, err)
				assert.Equal(t, testCase.expectErr, err.Error())
			}
			server.AssertExpectations(t)
		})
	}
}

func TestStartConsumerGroup(t *testing.T) {
	defer restoreNewConsumerGroup(newConsumerGroup)

	for _, testCase := range []struct {
		name       string
		factoryErr bool
	}{
		{
			name: "No error",
		},
		{
			name:       "Factory error",
			factoryErr: true,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := context.TODO()
			manager := NewConsumerGroupManager(logtesting.TestLogger(t).Desugar(), getMockServerHandler(), []string{}, &sarama.Config{}, &NoopConsumerOffsetInitializer{})
			mockGroup := kafkatesting.NewMockConsumerGroup()
			newConsumerGroup = func(addrs []string, groupID string, config *sarama.Config) (sarama.ConsumerGroup, error) {
				if testCase.factoryErr {
					return mockGroup, fmt.Errorf("factory error")
				}
				mockGroup.On("Errors").Return(mockGroup.ErrorChan)
				return mockGroup, nil
			}
			err := manager.StartConsumerGroup(ctx, "testid", []string{}, nil, nil)
			assert.Equal(t, testCase.factoryErr, err != nil)
			time.Sleep(5 * time.Millisecond) // Give the transferErrors routine a chance to call Errors()
			mockGroup.AssertExpectations(t)
		})
	}
}

func TestCloseConsumerGroup(t *testing.T) {
	defer restoreNewConsumerGroup(newConsumerGroup) // must use if calling getManagerWithMockGroup in the test

	for _, testCase := range []struct {
		name      string
		groupId   string
		expectErr bool
		closeErr  error
		cancel    bool
	}{
		{
			name:      "Nonexistent GroupID",
			expectErr: true,
		},
		{
			name:    "Existing GroupID",
			groupId: "test-group-id",
		},
		{
			name:      "Existing GroupID, Error Closing Group",
			groupId:   "test-group-id",
			closeErr:  fmt.Errorf("close error"),
			expectErr: true,
		},
		{
			name:    "With Cancel Functions",
			groupId: "test-group-id",
			cancel:  true,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			manager, group, managedGrp, server := getManagerWithMockGroup(t, testCase.groupId, false)
			var cancelConsumeCalled, cancelErrorsCalled bool
			if group != nil {
				group.On("Close").Return(testCase.closeErr)
				if testCase.cancel {
					managedGrp.cancelConsume = func() { cancelConsumeCalled = true }
					managedGrp.cancelErrors = func() { cancelErrorsCalled = true }
				}
			}
			err := manager.CloseConsumerGroup(testCase.groupId)
			assert.Equal(t, testCase.expectErr, err != nil)
			assert.Equal(t, testCase.cancel, cancelConsumeCalled)
			assert.Equal(t, testCase.cancel, cancelErrorsCalled)
			server.AssertExpectations(t)
		})
	}
}

func TestConsume(t *testing.T) {
	defer restoreNewConsumerGroup(newConsumerGroup) // must use if calling getManagerWithMockGroup in the test

	for _, testCase := range []struct {
		name      string
		groupId   string
		stopped   bool
		cancel    bool
		expectErr string
	}{
		{
			name:      "Nonexistent GroupID",
			expectErr: "consume called on nonexistent groupId ''",
		},
		{
			name:      "Existing GroupID, Started",
			groupId:   "test-group-id",
			expectErr: "kafka: tried to use a consumer group that was closed",
		},
		{
			name:      "Existing GroupID, Stopped",
			groupId:   "test-group-id",
			stopped:   true,
			expectErr: "kafka: tried to use a consumer group that was closed",
		},
		{
			name:      "Existing GroupID, Canceled",
			groupId:   "test-group-id",
			stopped:   true,
			cancel:    true,
			expectErr: "context was canceled waiting for group 'test-group-id' to start",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			manager, mockGroup, mgdGroup, server := getManagerWithMockGroup(t, testCase.groupId, false)
			if mockGroup != nil {
				mockGroup.On("Consume", ctx, []string{"topic"}, nil).Return(sarama.ErrClosedConsumerGroup)
				mockGroup.On("Close").Return(nil)
			}

			if testCase.stopped {
				mgdGroup.createRestartChannel()
			}
			waitGroup := sync.WaitGroup{}
			waitGroup.Add(1)
			go func() {
				err := manager.(*kafkaConsumerGroupManagerImpl).consume(ctx, testCase.groupId, []string{"topic"}, nil)
				if testCase.expectErr != "" {
					assert.NotNil(t, err)
					assert.Equal(t, testCase.expectErr, err.Error())
				} else {
					assert.Nil(t, err)
				}
				waitGroup.Done()
			}()
			time.Sleep(5 * time.Millisecond) // Give Consume() a chance to call waitForStart()
			if mgdGroup != nil {
				if testCase.cancel {
					cancel()
				} else {
					mgdGroup.closeRestartChannel()
				}
				assert.Nil(t, mgdGroup.group.Close()) // Stops the MockConsumerGroup's Consume() call
			}
			waitGroup.Wait() // Allows the goroutine with the consume call to finish
			server.AssertExpectations(t)
		})
	}
}

func TestErrors(t *testing.T) {
	defer restoreNewConsumerGroup(newConsumerGroup) // must use if calling getManagerWithMockGroup in the test

	for _, testCase := range []struct {
		name      string
		groupId   string
		expectErr bool
	}{
		{
			name:    "Existing GroupID",
			groupId: "test-group-id",
		},
		{
			name:      "Nonexistent GroupID",
			expectErr: true,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			manager, _, _, server := getManagerWithMockGroup(t, testCase.groupId, false)
			valid := manager.IsManaged(testCase.groupId)
			mgrErrors := manager.Errors(testCase.groupId)
			assert.Equal(t, testCase.expectErr, mgrErrors == nil)
			assert.Equal(t, !testCase.expectErr, valid)
			server.AssertExpectations(t)
		})
	}
}

func TestNotifications(t *testing.T) {
	defer restoreNewConsumerGroup(newConsumerGroup) // must use if calling getManagerWithMockGroup in the test
	for _, testCase := range []struct {
		name        string
		groupId     string
		version     int16
		opcode      ctrl.OpCode
		initialStop bool
		expectStop  bool
		factoryErr  bool
		expectErr   bool
		closeErr    error
		expectClose bool
		lockFail    bool
		lock        *commands.CommandLock
	}{
		{
			name:    "Invalid Command Version",
			opcode:  commands.StopConsumerGroupOpCode,
			version: 0,
		},
		{
			name:    "Stop Group OpCode, Nonexistent Group",
			opcode:  commands.StopConsumerGroupOpCode,
			version: 1,
		},
		{
			name:    "Start Group OpCode, Nonexistent Group",
			opcode:  commands.StartConsumerGroupOpCode,
			version: 1,
		},
		{
			name:        "Stop Group OpCode",
			opcode:      commands.StopConsumerGroupOpCode,
			groupId:     "test-group-id",
			version:     1,
			expectStop:  true,
			expectClose: true,
		},
		{
			name:        "Stop Group OpCode, Group Close Error",
			opcode:      commands.StopConsumerGroupOpCode,
			groupId:     "test-group-id",
			version:     1,
			closeErr:    fmt.Errorf("close error"),
			expectClose: true,
		},
		{
			name:    "Start Group OpCode, Group Already Started",
			opcode:  commands.StartConsumerGroupOpCode,
			groupId: "test-group-id",
			version: 1,
		},
		{
			name:        "Start Group OpCode, Group Not Started, Factory Error",
			opcode:      commands.StartConsumerGroupOpCode,
			groupId:     "test-group-id",
			version:     1,
			factoryErr:  true,
			initialStop: true,
			expectStop:  true,
		},
		{
			name:        "Start Group OpCode, Group Not Started",
			opcode:      commands.StartConsumerGroupOpCode,
			groupId:     "test-group-id",
			version:     1,
			initialStop: true,
		},
		{
			name:        "Start Group OpCode, Lock Failure",
			opcode:      commands.StartConsumerGroupOpCode,
			groupId:     "test-group-id",
			version:     1,
			lock:        &commands.CommandLock{LockBefore: true, Token: "token"},
			initialStop: true,
			lockFail:    true,
			expectStop:  true,
		},
		{
			name:     "Stop Group OpCode, Lock Failure",
			opcode:   commands.StopConsumerGroupOpCode,
			groupId:  "test-group-id",
			version:  1,
			lock:     &commands.CommandLock{LockBefore: true, Token: "token"},
			lockFail: true,
		},
		{
			name:        "Start Group OpCode, Unlock Failure",
			opcode:      commands.StartConsumerGroupOpCode,
			groupId:     "test-group-id",
			version:     1,
			lock:        &commands.CommandLock{UnlockAfter: true, Token: "token"},
			initialStop: true,
			lockFail:    true,
		},
		{
			name:        "Stop Group OpCode, Unlock Failure",
			opcode:      commands.StopConsumerGroupOpCode,
			groupId:     "test-group-id",
			version:     1,
			lock:        &commands.CommandLock{UnlockAfter: true, Token: "token"},
			expectClose: true,
			lockFail:    true,
			expectStop:  true,
		},
		{
			name:        "Start Group OpCode, Lock Success",
			opcode:      commands.StartConsumerGroupOpCode,
			groupId:     "test-group-id",
			lock:        &commands.CommandLock{LockBefore: true, Token: "token"},
			initialStop: true,
			version:     1,
		},
		{
			name:        "Stop Group OpCode, Lock Success",
			opcode:      commands.StopConsumerGroupOpCode,
			groupId:     "test-group-id",
			lock:        &commands.CommandLock{LockBefore: true, Token: "token"},
			expectClose: true,
			expectStop:  true,
			version:     1,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			manager, group, managedGrp, serverHandler := getManagerWithMockGroup(t, testCase.groupId, testCase.factoryErr)
			impl := manager.(*kafkaConsumerGroupManagerImpl)
			if testCase.initialStop {
				impl.groups[testCase.groupId].createRestartChannel()
			}
			if group != nil && testCase.expectClose {
				group.On("Close").Return(testCase.closeErr)
			}

			handler, ok := serverHandler.Router[testCase.opcode]
			assert.True(t, ok)

			if testCase.lockFail {
				managedGrp.lockedBy.Store("different-lock-token")
			} else if testCase.lock != nil {
				managedGrp.lockedBy.Store(testCase.lock.Token)
			}

			testCommand := commands.ConsumerGroupAsyncCommand{
				Version:   testCase.version,
				CommandId: 1,
				TopicName: "test-topic-name",
				GroupId:   testCase.groupId,
				Lock:      testCase.lock,
			}
			payload, err := testCommand.MarshalBinary()
			assert.Nil(t, err)

			msg := ctrl.NewMessage([16]byte{1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4}, uint8(testCase.opcode), payload)
			handler.HandleServiceMessage(context.Background(), ctrl.NewServiceMessage(&msg, func(err error) {
				assert.Equal(t, testCase.expectErr, err != nil)
			}))

			if group != nil {
				assert.Equal(t, testCase.expectStop, impl.groups[testCase.groupId].isStopped())
				group.AssertExpectations(t)
			}
			serverHandler.AssertExpectations(t)
		})
	}
}

func TestProcessLock(t *testing.T) {
	defer restoreNewConsumerGroup(newConsumerGroup) // must use if calling getManagerWithMockGroup in the test
	const existingGroup = "existing-group"
	const newToken = "new-token"
	const shortTimeout = 60 * time.Millisecond

	for _, testCase := range []struct {
		name            string
		groupId         string
		lock            *commands.CommandLock
		existingToken   string
		expectLock      string
		expectUnlock    string
		expectErrBefore error
		expectErrAfter  error
		expectTimerStop bool
	}{
		{
			name:    "Nil LockCommand",
			groupId: existingGroup,
		},
		{
			name:    "Nonexistent Group",
			groupId: "nonexistent-group",
			lock:    &commands.CommandLock{LockBefore: true, UnlockAfter: true},
		},
		{
			name:            "Different Lock Token",
			groupId:         existingGroup,
			existingToken:   "existing-token",
			expectLock:      "existing-token",
			expectUnlock:    "existing-token",
			lock:            &commands.CommandLock{LockBefore: true, UnlockAfter: true, Token: newToken},
			expectErrBefore: GroupLockedError,
			expectErrAfter:  GroupLockedError,
		},
		{
			name:          "Same Lock Token",
			groupId:       existingGroup,
			existingToken: newToken,
			expectLock:    newToken,
			lock:          &commands.CommandLock{LockBefore: true, UnlockAfter: true, Token: newToken},
		},
		{
			name:       "Zero Timeout",
			groupId:    existingGroup,
			lock:       &commands.CommandLock{LockBefore: true, UnlockAfter: true, Token: newToken},
			expectLock: newToken,
		},
		{
			name:            "Explicit Timeout",
			groupId:         existingGroup,
			lock:            &commands.CommandLock{LockBefore: true, UnlockAfter: true, Token: newToken, Timeout: shortTimeout},
			expectLock:      newToken,
			expectTimerStop: true,
		},
		{
			name:         "LockBefore Only",
			groupId:      existingGroup,
			lock:         &commands.CommandLock{LockBefore: true, Token: newToken},
			expectLock:   newToken,
			expectUnlock: newToken,
		},
		{
			name:    "UnlockAfter Only",
			groupId: existingGroup,
			lock:    &commands.CommandLock{UnlockAfter: true, Token: newToken},
		},
		{
			name:    "No Lock Or Unlock",
			groupId: existingGroup,
			lock:    &commands.CommandLock{Token: newToken},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			time.Sleep(time.Millisecond)
			manager, _, managedGrp, server := getManagerWithMockGroup(t, existingGroup, false)
			if testCase.existingToken != "" {
				managedGrp.lockedBy.Store(testCase.existingToken)
			}
			impl := manager.(*kafkaConsumerGroupManagerImpl)
			errBefore := impl.processLock(testCase.lock, testCase.groupId, true)
			assert.Equal(t, testCase.expectLock, managedGrp.lockedBy.Load())
			if testCase.expectTimerStop {
				time.Sleep(2 * shortTimeout)
				assert.Equal(t, "", managedGrp.lockedBy.Load())
			}
			errAfter := impl.processLock(testCase.lock, testCase.groupId, false)
			assert.Equal(t, testCase.expectUnlock, managedGrp.lockedBy.Load())
			assert.Equal(t, testCase.expectErrBefore, errBefore)
			assert.Equal(t, testCase.expectErrAfter, errAfter)

			server.AssertExpectations(t)
		})
	}
}

func TestProcessAsyncGroupNotification_BadMessage(t *testing.T) {
	cmdFunctionCalled := false
	cmdFunction := func(lock *commands.CommandLock, groupId string) error {
		cmdFunctionCalled = true
		return nil
	}
	processAsyncGroupNotification(ctrlservice.AsyncCommandMessage{}, cmdFunction)
	// Verify that an improper AsyncCommandMessage does not result in the cmdFunction being called
	assert.False(t, cmdFunctionCalled)
}

// getManagerWithMockGroup creates a KafkaConsumerGroupManager and optionally seeds it with a mock consumer group
func getManagerWithMockGroup(t *testing.T, groupId string, factoryErr bool) (KafkaConsumerGroupManager,
	*kafkatesting.MockConsumerGroup,
	*managedGroup,
	*controltesting.MockServerHandler) {

	serverHandler := getMockServerHandler()
	newConsumerGroup = func(addrs []string, groupID string, config *sarama.Config) (sarama.ConsumerGroup, error) {
		if factoryErr {
			return nil, fmt.Errorf("factory error")
		}
		return kafkatesting.NewMockConsumerGroup(), nil
	}
	manager := NewConsumerGroupManager(logtesting.TestLogger(t).Desugar(), serverHandler, []string{}, &sarama.Config{}, &NoopConsumerOffsetInitializer{})
	if groupId != "" {
		mockGrp, managedGrp := createTestGroup(t)
		manager.(*kafkaConsumerGroupManagerImpl).groups[groupId] = managedGrp
		return manager, mockGrp, managedGrp, serverHandler
	}
	return manager, nil, nil, serverHandler
}

func getMockServerHandler() *controltesting.MockServerHandler {
	server := controltesting.GetMockServerHandler()
	server.On("AddAsyncHandler", commands.StopConsumerGroupOpCode, commands.StopConsumerGroupResultOpCode, mock.Anything, mock.Anything).Return()
	server.On("AddAsyncHandler", commands.StartConsumerGroupOpCode, commands.StartConsumerGroupResultOpCode, mock.Anything, mock.Anything).Return()
	server.Service.On("SendAndWaitForAck", commands.StopConsumerGroupOpCode, mock.Anything).Return(nil)
	server.Service.On("SendAndWaitForAck", commands.StartConsumerGroupOpCode, mock.Anything).Return(nil)
	server.Service.On("SendAndWaitForAck", commands.StopConsumerGroupResultOpCode, mock.Anything).Return(nil)
	server.Service.On("SendAndWaitForAck", commands.StartConsumerGroupResultOpCode, mock.Anything).Return(nil)
	return server
}

func createTestGroup(t *testing.T) (*kafkatesting.MockConsumerGroup, *managedGroup) {
	mockGroup := kafkatesting.NewMockConsumerGroup()
	return mockGroup, createManagedGroup(logtesting.TestLogger(t).Desugar(), mockGroup, func() {})
}

// restoreNewConsumerGroup allows a single defer call to be used for saving and restoring the newConsumerGroup wrapper
func restoreNewConsumerGroup(fn func(addrs []string, groupID string, config *sarama.Config) (sarama.ConsumerGroup, error)) {
	newConsumerGroup = fn
}
