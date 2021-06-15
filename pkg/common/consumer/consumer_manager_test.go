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
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	ctrl "knative.dev/control-protocol/pkg"

	"github.com/Shopify/sarama"
	"go.uber.org/zap"
	controltesting "knative.dev/eventing-kafka/pkg/common/controlprotocol/testing"
	commontesting "knative.dev/eventing-kafka/pkg/common/testing"

	"github.com/stretchr/testify/assert"
	"knative.dev/eventing-kafka/pkg/common/controlprotocol/commands"
)

// MockKafkaConsumerGroupFactory is similar to the struct in pkg/common/consumer/testing/mocks.go but is replicated
// here to avoid an import cycle (the mocks in mocks.go are for other packages to use, not this one)
type mockKafkaConsumerGroupFactory struct {
	// CreateErr will return an error when creating a consumer
	CreateErr bool
}

func (c mockKafkaConsumerGroupFactory) StartConsumerGroup(_ string, _ []string, _ *zap.SugaredLogger, _ KafkaConsumerHandler, _ ...SaramaConsumerHandlerOption) (sarama.ConsumerGroup, error) {
	if c.CreateErr {
		return nil, errors.New("error creating consumer")
	}
	return commontesting.NewMockConsumerGroup(), nil
}

func TestNewConsumerGroupManager(t *testing.T) {
	server := controltesting.GetMockServerHandler()
	manager := NewConsumerGroupManager(server, []string{}, &sarama.Config{})
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
			group := managedGroup{}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			waitGroup := sync.WaitGroup{}
			assert.False(t, group.isStopped())

			if testCase.restart || testCase.cancel {
				group.Stop()
				assert.True(t, group.isStopped())
			}

			waitGroup.Add(1)
			go func() {
				assert.Equal(t, testCase.expected, group.waitForStart(ctx))
				waitGroup.Done()
			}()
			time.Sleep(5 * time.Millisecond) // Let the waitForStart function begin

			if testCase.restart {
				group.Start()
			} else if testCase.cancel {
				cancel()
			}
			waitGroup.Wait() // Let the waitForStart function finish
		})
	}

}

func createTestGroup() (*commontesting.MockConsumerGroup, *managedGroup) {
	mockGroup := commontesting.NewMockConsumerGroup()
	return mockGroup, &managedGroup{
		factory: &mockKafkaConsumerGroupFactory{},
		topics:  nil,
		logger:  nil,
		handler: KafkaConsumerHandler(nil),
		options: nil,
		groupId: "testid",
		group:   &customConsumerGroup{func() {}, mockGroup.Consume, make(chan error), mockGroup, make(chan bool)},
		errors:  make(chan error),
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
				managedGrp.Stop()
			}
			if testCase.cancel {
				cancel()
			}
			close(mockGrp.ErrorChan)
			close(managedGrp.group.handlerErrorChannel)
			time.Sleep(5 * time.Millisecond) // Let the error handling loop move forward
			if testCase.startGroup {
				// Simulate the effects of startConsumerGroup (new ConsumerGroup, same managedConsumerGroup)
				mockGrp = commontesting.NewMockConsumerGroup()
				managedGrp.group = &customConsumerGroup{func() {}, mockGrp.Consume, make(chan error), mockGrp, make(chan bool)}
				managedGrp.Start()

				time.Sleep(5 * time.Millisecond) // Let the waitForStart function finish
				// Verify that errors work again after restart
				mockGrp.ErrorChan <- fmt.Errorf("test-error-2")
				err = <-managedGrp.errors
				assert.NotNil(t, err)
				assert.Equal(t, "test-error-2", err.Error())
				close(mockGrp.ErrorChan)
				close(managedGrp.group.handlerErrorChannel)
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
			manager := NewConsumerGroupManager(controltesting.GetMockServerHandler(), []string{}, &sarama.Config{})
			newConsumerGroup = func(addrs []string, groupID string, config *sarama.Config) (sarama.ConsumerGroup, error) {
				if testCase.factoryErr {
					return &commontesting.MockConsumerGroup{}, fmt.Errorf("factory error")
				}
				return &commontesting.MockConsumerGroup{}, nil
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
	} {
		t.Run(testCase.name, func(t *testing.T) {
			manager, group, mgGroup, _ := getManagerWithMockGroup(testCase.groupId, false)
			if group != nil {
				group.CloseErr = testCase.closeErr
				close(mgGroup.group.releasedCh)
			}
			err := manager.CloseConsumerGroup(testCase.groupId)
			assert.Equal(t, testCase.expectErr, err != nil)
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
			manager, _, mgdGroup, _ := getManagerWithMockGroup(testCase.groupId, false)

			if testCase.stopped {
				mgdGroup.Stop()
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
					mgdGroup.Start()
				}
				close(mgdGroup.group.releasedCh)      // Allows customConsumerGroup's Close() to finish
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
			manager, _, _, _ := getManagerWithMockGroup(testCase.groupId, false)
			valid := manager.IsValid(testCase.groupId)
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
			manager, group, mgdGroup, serverHandler := getManagerWithMockGroup(testCase.groupId, testCase.factoryErr)
			impl := manager.(*kafkaConsumerGroupManagerImpl)
			if testCase.initialStop {
				impl.groups[testCase.groupId].Stop()
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
			if mgdGroup != nil {
				close(mgdGroup.group.releasedCh) // Allows customConsumerGroup's Close() to finish
			}
			handler.HandleServiceMessage(context.Background(), ctrl.NewServiceMessage(&msg, func(err error) {
				assert.Equal(t, testCase.expectErr, err != nil)
			}))

			if group != nil {
				assert.Equal(t, testCase.expectStop, impl.groups[testCase.groupId].isStopped())
			}
		})
	}
}

// getManagerWithMockGroup creates a KafkaConsumerGroupManager and optionally seeds it with a mock consumer group
func getManagerWithMockGroup(groupId string, factoryErr bool) (KafkaConsumerGroupManager,
	*commontesting.MockConsumerGroup,
	*managedGroup,
	*controltesting.MockServerHandler) {
	serverHandler := controltesting.GetMockServerHandler()
	newConsumerGroup = func(addrs []string, groupID string, config *sarama.Config) (sarama.ConsumerGroup, error) {
		if factoryErr {
			return nil, fmt.Errorf("factory error")
		}
		return commontesting.NewMockConsumerGroup(), nil
	}
	manager := NewConsumerGroupManager(serverHandler, []string{}, &sarama.Config{})
	if groupId != "" {
		mockGrp, managedGrp := createTestGroup()
		manager.(*kafkaConsumerGroupManagerImpl).groups[groupId] = managedGrp
		return manager, mockGrp, managedGrp, serverHandler
	}
	return manager, nil, nil, serverHandler
}

// restoreNewConsumerGroup allows a one-line defer call to be used for saving and restoring the newConsumerGroup wrapper
func restoreNewConsumerGroup(fn func(addrs []string, groupID string, config *sarama.Config) (sarama.ConsumerGroup, error)) {
	newConsumerGroup = fn
}
