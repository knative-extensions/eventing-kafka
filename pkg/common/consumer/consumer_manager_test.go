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
	"sync/atomic"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"k8s.io/apimachinery/pkg/types"
	ctrl "knative.dev/control-protocol/pkg"
	ctrlservice "knative.dev/control-protocol/pkg/service"
	logtesting "knative.dev/pkg/logging/testing"

	"knative.dev/eventing-kafka/pkg/common/controlprotocol/commands"
	controltesting "knative.dev/eventing-kafka/pkg/common/controlprotocol/testing"
	kafkatesting "knative.dev/eventing-kafka/pkg/common/kafka/testing"
)

func TestNewConsumerGroupManager(t *testing.T) {
	server := getMockServerHandler()
	manager := NewConsumerGroupManager(logtesting.TestLogger(t).Desugar(), server, []string{}, &sarama.Config{}, &NoopConsumerGroupOffsetsChecker{}, func(ref types.NamespacedName) {})
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
			expectErr: "Reconfigure Failed: MultiErr='error closing consumer group', GroupIds='[test-id1]'",
		},
		{
			name:       "Error starting groups",
			groupId:    "test-id1",
			factoryErr: true,
			expectErr:  "Reconfigure Failed: MultiErr='factory error', GroupIds='[test-id1]'",
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
				assert.Len(t, err.GroupIds, 1)
				assert.Equal(t, testCase.groupId, err.GroupIds[0])
			}
			server.AssertExpectations(t)
		})
	}
}

func TestReconfigureError(t *testing.T) {
	for _, testCase := range []struct {
		name   string
		err    ReconfigureError
		result string
	}{
		{
			name: "Empty",
			err: ReconfigureError{
				MultiError: nil,
				GroupIds:   nil,
			},
			result: "Reconfigure Failed: MultiErr='', GroupIds='[]'",
		},
		{
			name: "MultiErr Only",
			err: ReconfigureError{
				MultiError: fmt.Errorf("test-error"),
				GroupIds:   nil,
			},
			result: "Reconfigure Failed: MultiErr='test-error', GroupIds='[]'",
		},
		{
			name: "GroupIds Only",
			err: ReconfigureError{
				MultiError: nil,
				GroupIds:   []string{"one", "two", "three"},
			},
			result: "Reconfigure Failed: MultiErr='', GroupIds='[one two three]'",
		},
		{
			name: "Complete",
			err: ReconfigureError{
				MultiError: fmt.Errorf("test-error"),
				GroupIds:   []string{"one", "two", "three"},
			},
			result: "Reconfigure Failed: MultiErr='test-error', GroupIds='[one two three]'",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			assert.Equal(t, testCase.result, testCase.err.Error())
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
			manager := NewConsumerGroupManager(logtesting.TestLogger(t).Desugar(), getMockServerHandler(), []string{}, &sarama.Config{}, &NoopConsumerGroupOffsetsChecker{}, func(ref types.NamespacedName) {})
			mockGroup := kafkatesting.NewMockConsumerGroup()
			newConsumerGroup = func(addrs []string, groupID string, config *sarama.Config) (sarama.ConsumerGroup, error) {
				if testCase.factoryErr {
					return mockGroup, fmt.Errorf("factory error")
				}
				mockGroup.On("Errors").Return(mockGroup.ErrorChan)
				return mockGroup, nil
			}
			err := manager.StartConsumerGroup(ctx, "testid", []string{}, nil, types.NamespacedName{})
			assert.Equal(t, testCase.factoryErr, err != nil)
			time.Sleep(shortTimeout) // Give the transferErrors routine a chance to call Errors()
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
	} {
		t.Run(testCase.name, func(t *testing.T) {
			manager, group, _, server := getManagerWithMockGroup(t, testCase.groupId, false)
			if group != nil {
				group.On("Close").Return(testCase.closeErr)
				group.On("Errors").Return(make(chan error))
			}
			err := manager.CloseConsumerGroup(testCase.groupId)
			assert.Equal(t, testCase.expectErr, err != nil)
			server.AssertExpectations(t)
		})
	}
}

func TestConsume(t *testing.T) {
	for _, testCase := range []struct {
		name      string
		groupId   string
		expectErr string
	}{
		{
			name:      "Nonexistent GroupID",
			expectErr: "consume called on nonexistent groupId ''",
		},
		{
			name:    "Existing GroupID",
			groupId: "test-group-id",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			manager := &kafkaConsumerGroupManagerImpl{logger: logtesting.TestLogger(t).Desugar(), groups: make(groupMap)}
			if testCase.groupId != "" {
				mockGroup := &mockManagedGroup{}
				mockGroup.On("consume", context.Background(), []string{"topic"}, nil).Return(nil)
				manager.groups[testCase.groupId] = mockGroup
			}
			err := manager.consume(context.Background(), testCase.groupId, []string{"topic"}, nil)
			if testCase.expectErr != "" {
				assert.NotNil(t, err)
				assert.Equal(t, testCase.expectErr, err.Error())
			} else {
				assert.Nil(t, err)
			}
		})
	}
}

func TestLockUnlockWrappers(t *testing.T) {

	for _, testCase := range []struct {
		name      string
		groupId   string
		expectErr bool
	}{
		{
			name: "Nonexistent GroupID",
		},
		{
			name:      "Existing GroupID",
			groupId:   "test-group-id",
			expectErr: true,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			manager := &kafkaConsumerGroupManagerImpl{logger: logtesting.TestLogger(t).Desugar(), groups: make(groupMap)}
			if testCase.groupId != "" {
				mockGroup := &mockManagedGroup{}
				mockGroup.On("processLock", mock.Anything, mock.Anything).Return(fmt.Errorf("test error"))
				manager.groups[testCase.groupId] = mockGroup
			}
			err := manager.lockBefore(nil, testCase.groupId)
			assert.Equal(t, testCase.expectErr, err != nil)
			err = manager.unlockAfter(nil, testCase.groupId)
			assert.Equal(t, testCase.expectErr, err != nil)
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
			stopped := manager.IsStopped(testCase.groupId)
			mgrErrors := manager.Errors(testCase.groupId)
			assert.Equal(t, testCase.expectErr, mgrErrors == nil)
			assert.Equal(t, !testCase.expectErr, valid)
			assert.False(t, stopped) // Not actually using a stopped group in this test
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
		unlockFail  bool
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
			expectStop:  true, // Still stopped, because the group is locked by a different token
			lockFail:    true,
		},
		{
			name:     "Stop Group OpCode, Unlock Failure",
			opcode:   commands.StopConsumerGroupOpCode,
			groupId:  "test-group-id",
			version:  1,
			lock:     &commands.CommandLock{UnlockAfter: true, Token: "token"},
			lockFail: true,
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
		{
			name:        "Start Group OpCode, Lock Success, Unlock Failure",
			opcode:      commands.StartConsumerGroupOpCode,
			groupId:     "test-group-id",
			version:     1,
			lock:        &commands.CommandLock{LockBefore: true, Token: "token"},
			initialStop: true,
			unlockFail:  true,
		},
		{
			name:       "Stop Group OpCode, Lock Success, Unlock Failure",
			opcode:     commands.StopConsumerGroupOpCode,
			groupId:    "test-group-id",
			version:    1,
			lock:       &commands.CommandLock{LockBefore: true, Token: "token"},
			expectStop: true,
			unlockFail: true,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {

			manager, group, managedGrp, serverHandler := getManagerWithMockGroup(t, testCase.groupId, testCase.factoryErr)
			impl := manager.(*kafkaConsumerGroupManagerImpl)
			if testCase.initialStop {
				impl.groups[testCase.groupId].(*managedGroupImpl).createRestartChannel()
			}
			if group != nil && testCase.expectClose {
				group.On("Close").Return(testCase.closeErr)
			}

			handler, ok := serverHandler.Router[testCase.opcode]
			assert.True(t, ok)

			if testCase.lockFail {
				managedGrp.(*managedGroupImpl).lockedBy.Store("different-lock-token")
			} else if testCase.lock != nil {
				managedGrp.(*managedGroupImpl).lockedBy.Store(testCase.lock.Token)
			}

			mockingManagedGroup := false
			if testCase.unlockFail {
				// Having lock succeed but unlock fail is difficult to accomplish practically, so use a mock instead
				mockingManagedGroup = true
				mockGroup := &mockManagedGroup{}
				mockGroup.On("processLock", mock.Anything, true).Return(nil)
				mockGroup.On("stop").Return(nil)
				mockGroup.On("start", mock.Anything).Return(nil)
				mockGroup.On("processLock", mock.Anything, false).Return(fmt.Errorf("unlock error"))
				impl.groups[testCase.groupId] = mockGroup
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
				if !mockingManagedGroup {
					assert.Equal(t, testCase.expectStop, impl.groups[testCase.groupId].(*managedGroupImpl).isStopped())
				}
				group.AssertExpectations(t)
			}
			serverHandler.AssertExpectations(t)
		})
	}
}

func TestManagerEvents(t *testing.T) {
	defer restoreNewConsumerGroup(newConsumerGroup) // must use if calling getManagerWithMockGroup in the test
	for _, testCase := range []struct {
		name        string
		channels    int
		listen      bool
		expectEvent int32
		expectClose int32
	}{
		{
			name: "Notify Zero Channels",
		},
		{
			name:        "Notify One Channel",
			channels:    1,
			listen:      true,
			expectEvent: 1,
			expectClose: 1,
		},
		{
			name:        "Notify Two Channels",
			channels:    2,
			listen:      true,
			expectEvent: 2,
			expectClose: 2,
		},
		{
			name:        "Notify Ten Channels",
			channels:    10,
			listen:      true,
			expectEvent: 10,
			expectClose: 10,
		},
		{
			name:        "Notify Non-Listening Channel",
			channels:    1,
			expectClose: 1,
		},
		{
			name:        "Notify Five Non-Listening Channels",
			channels:    5,
			expectClose: 5,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			manager, _, _, _ := getManagerWithMockGroup(t, "", false)
			impl := manager.(*kafkaConsumerGroupManagerImpl)

			var notifyChannels []<-chan ManagerEvent
			for i := 0; i < testCase.channels; i++ {
				notifyChannels = append(notifyChannels, manager.GetNotificationChannel())
			}

			waitGroup := sync.WaitGroup{}
			count := int32(0)

			if testCase.listen {
				waitGroup.Add(len(notifyChannels))
				for _, notifyChan := range notifyChannels {
					go func(channel <-chan ManagerEvent) {
						<-channel
						atomic.AddInt32(&count, 1)
						waitGroup.Done()
					}(notifyChan)
				}
			}

			time.Sleep(shortTimeout)
			impl.notify(ManagerEvent{Event: GroupCreated, GroupId: "created-group-id"})
			waitGroup.Wait()
			assert.Equal(t, testCase.expectEvent, count)

			waitGroup = sync.WaitGroup{}
			count = int32(0)
			waitGroup.Add(len(notifyChannels))
			for _, notifyChan := range notifyChannels {
				go func(channel <-chan ManagerEvent) {
					<-channel
					atomic.AddInt32(&count, 1)
					waitGroup.Done()
				}(notifyChan)
			}

			time.Sleep(shortTimeout)
			manager.ClearNotifications()
			waitGroup.Wait()
			assert.Equal(t, testCase.expectClose, count)
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
	managedGroup,
	*controltesting.MockServerHandler) {

	serverHandler := getMockServerHandler()
	newConsumerGroup = func(addrs []string, groupID string, config *sarama.Config) (sarama.ConsumerGroup, error) {
		if factoryErr {
			return nil, fmt.Errorf("factory error")
		}
		mockGroup := kafkatesting.NewMockConsumerGroup()
		mockGroup.On("Errors").Return(make(chan error))
		return mockGroup, nil
	}
	manager := NewConsumerGroupManager(logtesting.TestLogger(t).Desugar(), serverHandler, []string{}, &sarama.Config{}, &NoopConsumerGroupOffsetsChecker{}, func(ref types.NamespacedName) {})
	if groupId != "" {
		mockGroup, managedGrp := createMockAndManagedGroups(t)
		manager.(*kafkaConsumerGroupManagerImpl).groups[groupId] = managedGrp
		return manager, mockGroup, managedGrp, serverHandler
	}
	return manager, nil, nil, serverHandler
}

func createMockAndManagedGroups(t *testing.T) (*kafkatesting.MockConsumerGroup, *managedGroupImpl) {
	mockGroup := kafkatesting.NewMockConsumerGroup()
	mockGroup.On("Errors").Return(make(chan error))
	managedGrp := createManagedGroup(context.Background(), logtesting.TestLogger(t).Desugar(), mockGroup, func() {}, func() {})
	// let the transferErrors function start (otherwise AssertExpectations will randomly fail because Errors() isn't called)
	time.Sleep(shortTimeout)
	return mockGroup, managedGrp.(*managedGroupImpl)
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

// restoreNewConsumerGroup allows a single defer call to be used for saving and restoring the newConsumerGroup wrapper
func restoreNewConsumerGroup(fn func(addrs []string, groupID string, config *sarama.Config) (sarama.ConsumerGroup, error)) {
	newConsumerGroup = fn
}
