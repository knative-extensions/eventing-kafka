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
	logtesting "knative.dev/pkg/logging/testing"

	"knative.dev/eventing-kafka/pkg/common/controlprotocol/commands"
	controltesting "knative.dev/eventing-kafka/pkg/common/controlprotocol/testing"
	kafkatesting "knative.dev/eventing-kafka/pkg/common/kafka/testing"
)

func TestNewConsumerGroupManager(t *testing.T) {
	server := getMockServerHandler()
	manager := NewConsumerGroupManager(logtesting.TestLogger(t).Desugar(), server, []string{}, &sarama.Config{})
	assert.NotNil(t, manager)
	assert.NotNil(t, server.Router[commands.StopConsumerGroupOpCode])
	assert.NotNil(t, server.Router[commands.StartConsumerGroupOpCode])
}

func TestManagedGroup(t *testing.T) {
	for _, testCase := range []struct {
		name     string
		restart  bool
		cancel   bool
		expected bool
	}{
		{
			name:     "Wait for Started Group",
			expected: true,
		},
		{
			name:     "Wait for Restarted Group",
			restart:  true,
			expected: true,
		},
		{
			name:     "Cancel Wait",
			cancel:   true,
			expected: false,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			// Test stop/start of a managedGroup
			group := managedGroup{ restartChanMutex: sync.Mutex{} }
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			waitGroup := sync.WaitGroup{}
			assert.Nil(t, group.restartWaitChannel)

			if testCase.restart || testCase.cancel {
				group.createRestartChannel()
				assert.NotNil(t, group.restartWaitChannel)
			}

			waitGroup.Add(1)
			go func() {
				assert.Equal(t, testCase.expected, group.waitForStart(ctx))
				waitGroup.Done()
			}()
			time.Sleep(5 * time.Millisecond) // Let the waitForStart function begin

			if testCase.restart {
				group.closeRestartChannel()
			} else if testCase.cancel {
				cancel()
			}
			waitGroup.Wait() // Let the waitForStart function finish
		})
	}

}

func TestManagedGroup_transferErrors(t *testing.T) {
	for _, testCase := range []struct {
		name       string
		stopGroup  bool
		startGroup bool
		cancel     bool
	}{
		{
			name: "Close Channel Without Stop",
		},
		{
			name:      "Close Channel After Stop",
			stopGroup: true,
		},
		{
			name:      "Cancel Context",
			stopGroup: true,
			cancel:    true,
		},
		{
			name:       "Restart After Stop",
			stopGroup:  true,
			startGroup: true,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			mockGrp, managedGrp := createTestGroup()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			managedGrp.transferErrors(ctx)

			mockGrp.ErrorChan <- fmt.Errorf("test-error")
			err := <-managedGrp.errors
			assert.NotNil(t, err)
			assert.Equal(t, "test-error", err.Error())
			if testCase.stopGroup {
				managedGrp.createRestartChannel()
			}
			if testCase.cancel {
				cancel()
			}
			close(mockGrp.ErrorChan)
			time.Sleep(5 * time.Millisecond) // Let the error handling loop move forward
			if testCase.startGroup {
				// Simulate the effects of startConsumerGroup (new ConsumerGroup, same managedConsumerGroup)
				mockGrp = kafkatesting.NewStubbedMockConsumerGroup()
				managedGrp.group = mockGrp
				managedGrp.closeRestartChannel()

				time.Sleep(5 * time.Millisecond) // Let the waitForStart function finish
				// Verify that errors work again after restart
				mockGrp.ErrorChan <- fmt.Errorf("test-error-2")
				err = <-managedGrp.errors
				assert.NotNil(t, err)
				assert.Equal(t, "test-error-2", err.Error())
				close(mockGrp.ErrorChan)
			}
		})
	}
}

func TestReconfigure(t *testing.T) {
	defer restoreNewConsumerGroup(newConsumerGroup)

	for _, testCase := range []struct {
		name       string
		groupId    string
		factoryErr bool
		closeErr   bool
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
			closeErr:  true,
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
			manager, group, _, _ := getManagerWithMockGroup(t, testCase.groupId, testCase.factoryErr)
			if group != nil {
				group.CloseErr = testCase.closeErr
			}
			err := manager.Reconfigure([]string{"new-broker"}, &sarama.Config{})
			if testCase.expectErr == "" {
				assert.Nil(t, err)
			} else {
				assert.NotNil(t, err)
				assert.Equal(t, testCase.expectErr, err.Error())
			}
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
			manager := NewConsumerGroupManager(logtesting.TestLogger(t).Desugar(), getMockServerHandler(), []string{}, &sarama.Config{})
			newConsumerGroup = func(addrs []string, groupID string, config *sarama.Config) (sarama.ConsumerGroup, error) {
				if testCase.factoryErr {
					return kafkatesting.NewStubbedMockConsumerGroup(), fmt.Errorf("factory error")
				}
				return kafkatesting.NewStubbedMockConsumerGroup(), nil
			}
			err := manager.StartConsumerGroup("testid", []string{}, nil, nil)
			assert.Equal(t, testCase.factoryErr, err != nil)
		})
	}
}

func TestCloseConsumerGroup(t *testing.T) {
	defer restoreNewConsumerGroup(newConsumerGroup) // must use if calling getManagerWithMockGroup in the test

	for _, testCase := range []struct {
		name      string
		groupId   string
		expectErr bool
		closeErr  bool
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
			closeErr:  true,
			expectErr: true,
		},
		{
			name:    "With Cancel Functions",
			groupId: "test-group-id",
			cancel:  true,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			manager, group, managedGrp, _ := getManagerWithMockGroup(t, testCase.groupId, false)
			var cancelConsumeCalled, cancelErrorsCalled bool
			if group != nil {
				group.CloseErr = testCase.closeErr
				if testCase.cancel {
					managedGrp.cancelConsume = func() { cancelConsumeCalled = true }
					managedGrp.cancelErrors = func() { cancelErrorsCalled = true }
				}
			}
			err := manager.CloseConsumerGroup(testCase.groupId)
			assert.Equal(t, testCase.expectErr, err != nil)
			assert.Equal(t, testCase.cancel, cancelConsumeCalled)
			assert.Equal(t, testCase.cancel, cancelErrorsCalled)
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
			manager, _, mgdGroup, _ := getManagerWithMockGroup(t, testCase.groupId, false)

			if testCase.stopped {
				mgdGroup.createRestartChannel()
			}
			waitGroup := sync.WaitGroup{}
			waitGroup.Add(1)
			go func() {
				err := manager.(*kafkaConsumerGroupManagerImpl).consume(ctx, testCase.groupId, nil, nil)
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
			manager, _, _, _ := getManagerWithMockGroup(t, testCase.groupId, false)
			valid := manager.IsManaged(testCase.groupId)
			mgrErrors := manager.Errors(testCase.groupId)
			assert.Equal(t, testCase.expectErr, mgrErrors == nil)
			assert.Equal(t, !testCase.expectErr, valid)
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
		closeErr    bool
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
			name:       "Stop Group OpCode",
			opcode:     commands.StopConsumerGroupOpCode,
			groupId:    "test-group-id",
			version:    1,
			expectStop: true,
		},
		{
			name:     "Stop Group OpCode, Group Close Error",
			opcode:   commands.StopConsumerGroupOpCode,
			groupId:  "test-group-id",
			version:  1,
			closeErr: true,
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
	} {
		t.Run(testCase.name, func(t *testing.T) {
			manager, group, _, serverHandler := getManagerWithMockGroup(t, testCase.groupId, testCase.factoryErr)
			impl := manager.(*kafkaConsumerGroupManagerImpl)
			if testCase.initialStop {
				impl.groups[testCase.groupId].createRestartChannel()
			}
			if group != nil {
				group.CloseErr = testCase.closeErr
			}

			handler, ok := serverHandler.Router[testCase.opcode]
			assert.True(t, ok)

			testCommand := commands.ConsumerGroupAsyncCommand{
				Version:   testCase.version,
				CommandId: 1,
				TopicName: "test-topic-name",
				GroupId:   testCase.groupId,
			}
			payload, err := testCommand.MarshalBinary()
			assert.Nil(t, err)

			msg := ctrl.NewMessage([16]byte{1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4}, uint8(testCase.opcode), payload)
			handler.HandleServiceMessage(context.Background(), ctrl.NewServiceMessage(&msg, func(err error) {
				assert.Equal(t, testCase.expectErr, err != nil)
			}))

			if group != nil {
				assert.Equal(t, testCase.expectStop, impl.groups[testCase.groupId].restartWaitChannel != nil)
			}
		})
	}
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
		return kafkatesting.NewStubbedMockConsumerGroup(), nil
	}
	manager := NewConsumerGroupManager(logtesting.TestLogger(t).Desugar(), serverHandler, []string{}, &sarama.Config{})
	if groupId != "" {
		mockGrp, managedGrp := createTestGroup()
		manager.(*kafkaConsumerGroupManagerImpl).groups[groupId] = managedGrp
		return manager, mockGrp, managedGrp, serverHandler
	}
	return manager, nil, nil, serverHandler
}

func getMockServerHandler() *controltesting.MockServerHandler {
	serverHandler := controltesting.GetMockServerHandler()
	serverHandler.On("AddAsyncHandler", commands.StopConsumerGroupOpCode, commands.StopConsumerGroupResultOpCode, mock.Anything, mock.Anything).Return()
	serverHandler.On("AddAsyncHandler", commands.StartConsumerGroupOpCode, commands.StartConsumerGroupResultOpCode, mock.Anything, mock.Anything).Return()
	serverHandler.Service.On("SendAndWaitForAck", commands.StopConsumerGroupOpCode, mock.Anything).Return(nil)
	serverHandler.Service.On("SendAndWaitForAck", commands.StartConsumerGroupOpCode, mock.Anything).Return(nil)
	serverHandler.Service.On("SendAndWaitForAck", commands.StopConsumerGroupResultOpCode, mock.Anything).Return(nil)
	serverHandler.Service.On("SendAndWaitForAck", commands.StartConsumerGroupResultOpCode, mock.Anything).Return(nil)
	return serverHandler
}

func createTestGroup() (*kafkatesting.MockConsumerGroup, *managedGroup) {
	mockGroup := kafkatesting.NewStubbedMockConsumerGroup()
	return mockGroup, &managedGroup{
		group:            mockGroup,
		errors:           make(chan error),
		restartChanMutex: sync.Mutex{},
	}
}

// restoreNewConsumerGroup allows a single defer call to be used for saving and restoring the newConsumerGroup wrapper
func restoreNewConsumerGroup(fn func(addrs []string, groupID string, config *sarama.Config) (sarama.ConsumerGroup, error)) {
	newConsumerGroup = fn
}
