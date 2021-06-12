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

	"github.com/Shopify/sarama"
	"go.uber.org/zap"
	controltesting "knative.dev/eventing-kafka/pkg/common/controlprotocol/testing"
	commontesting "knative.dev/eventing-kafka/pkg/common/testing"

	"github.com/stretchr/testify/assert"
	ctrl "knative.dev/control-protocol/pkg"

	"knative.dev/eventing-kafka/pkg/common/controlprotocol/commands"
)

// MockKafkaConsumerGroupFactory is similar to the struct in pkg/common/consumer/testing/mocks.go but is replicated
// here to avoid an import cycle (the mocks in mocks.go are for other packages to use, not this one)
type mockKafkaConsumerGroupFactory struct {
	// CreateErr will return an error when creating a consumer
	CreateErr bool
}

func (c mockKafkaConsumerGroupFactory) StartConsumerGroup(manager KafkaConsumerGroupManager, groupID string, topics []string, logger *zap.SugaredLogger, handler KafkaConsumerHandler, options ...SaramaConsumerHandlerOption) (sarama.ConsumerGroup, error) {
	if c.CreateErr {
		return nil, errors.New("error creating consumer")
	}
	group := commontesting.NewMockConsumerGroup()
	manager.AddExistingGroup(groupID, group, topics, logger, handler, options...)
	return group, nil
}

func TestNewConsumerGroupManager(t *testing.T) {
	server := controltesting.GetMockServerHandler()
	manager := NewConsumerGroupManager(server, &mockKafkaConsumerGroupFactory{})
	assert.NotNil(t, manager)
	assert.NotNil(t, server.Router[commands.StopConsumerGroupOpCode])
	assert.NotNil(t, server.Router[commands.StartConsumerGroupOpCode])
}

func TestNewPassthroughManager(t *testing.T) {
	manager := NewPassthroughManager()
	assert.NotNil(t, manager)
	assert.Nil(t, manager.Errors(""))
	assert.False(t, manager.IsValid(""))
	assert.Nil(t, manager.CloseConsumerGroup(""))
	assert.Nil(t, manager.StartConsumerGroup("", []string{}, nil, nil))
	assert.NotNil(t, manager.Consume("", context.TODO(), []string{}, nil))
	group := commontesting.NewMockConsumerGroup()
	manager.AddExistingGroup("test-group-id", group, []string{}, nil, nil)
	waitGroup := sync.WaitGroup{}
	waitGroup.Add(1)
	go func() {
		_ = manager.Consume("test-group-id", context.TODO(), []string{}, nil)
		waitGroup.Done()
	}()
	assert.Nil(t, group.Close())
	waitGroup.Wait()
}

func TestManagedGroup(t *testing.T) {
	group := managedGroup{}
	assert.False(t, group.isStopped())
	group.waitForStart()
	group.Stop()
	assert.True(t, group.isStopped())
}

func TestManagedGroup_transferErrors(t *testing.T) {
	mockGroup := &commontesting.MockConsumerGroup{ErrorChan: make(chan error)}
	group := managedGroup{
		factory: &mockKafkaConsumerGroupFactory{},
		topics:  nil,
		logger:  nil,
		handler: KafkaConsumerHandler(nil),
		options: nil,
		groupId: "testid",
		group:   mockGroup,
		errors:  make(chan error),
		stopped: false,
	}

	// Run through an error channel "stop/start" cycle
	group.transferErrors()

	// First cycle
	mockGroup.ErrorChan <- fmt.Errorf("first")
	err := <-group.errors
	assert.NotNil(t, err)
	assert.Equal(t, "first", err.Error())
	group.Stop()
	close(mockGroup.ErrorChan)       // Close on a "stopped" consumergroup should wait for a restart
	time.Sleep(5 * time.Millisecond) // Let the error handling loop call WaitForStart()

	// Second cycle
	mockGroup.ErrorChan = make(chan error)
	group.Start() // Signals the error transfer loop to start processing again
	mockGroup.ErrorChan <- fmt.Errorf("second")
	err = <-group.errors
	assert.NotNil(t, err)
	assert.Equal(t, "second", err.Error())
	close(mockGroup.ErrorChan) // Close on a consumergroup that is not "stopped"

	time.Sleep(5 * time.Millisecond) // Give the goroutine a chance to exit
}

func TestAddExistingGroup(t *testing.T) {
	manager := NewConsumerGroupManager(controltesting.GetMockServerHandler(), &mockKafkaConsumerGroupFactory{})
	group := &commontesting.MockConsumerGroup{ErrorChan: make(chan error)}
	manager.AddExistingGroup("testid", group, []string{}, nil, nil)
	group.ErrorChan <- fmt.Errorf("test")
	assert.Equal(t, "test", (<-manager.Errors("testid")).Error())
	close(group.ErrorChan)
}

func TestStartConsumerGroup(t *testing.T) {
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
			manager := NewConsumerGroupManager(controltesting.GetMockServerHandler(), &mockKafkaConsumerGroupFactory{CreateErr: testCase.factoryErr})
			err := manager.StartConsumerGroup("testid", []string{}, nil, nil)
			assert.Equal(t, testCase.factoryErr, err != nil)
		})
	}
}

func TestCloseConsumerGroup(t *testing.T) {
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
			manager, group, _ := getManagerWithMockGroup(testCase.groupId, false)
			if group != nil {
				group.CloseErr = testCase.closeErr
			}
			err := manager.CloseConsumerGroup(testCase.groupId)
			assert.Equal(t, testCase.expectErr, err != nil)
		})
	}
}

func TestConsume(t *testing.T) {
	for _, testCase := range []struct {
		name      string
		groupId   string
		stopped   bool
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
	} {
		t.Run(testCase.name, func(t *testing.T) {
			manager, _, _ := getManagerWithMockGroup(testCase.groupId, false)

			mgdGroup := manager.(*kafkaConsumerGroupManagerImpl).groups[testCase.groupId]
			if testCase.stopped {
				mgdGroup.Stop()
			}
			waitGroup := sync.WaitGroup{}
			waitGroup.Add(1)
			go func() {
				err := manager.Consume(testCase.groupId, context.TODO(), nil, nil)
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
				mgdGroup.Start()
				assert.Nil(t, mgdGroup.group.Close())
			}
			waitGroup.Wait()
		})
	}
}

func TestErrors(t *testing.T) {
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
			manager, _, _ := getManagerWithMockGroup(testCase.groupId, false)
			valid := manager.IsValid(testCase.groupId)
			mgrErrors := manager.Errors(testCase.groupId)
			assert.Equal(t, testCase.expectErr, mgrErrors == nil)
			assert.Equal(t, !testCase.expectErr, valid)
		})
	}
}

func TestNotifications(t *testing.T) {
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
			manager, group, serverHandler := getManagerWithMockGroup(testCase.groupId, testCase.factoryErr)
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
			handler.HandleServiceMessage(context.TODO(), ctrl.NewServiceMessage(&msg, func(err error) {
				assert.Equal(t, testCase.expectErr, err != nil)
			}))

			if group != nil {
				assert.Equal(t, testCase.expectStop, impl.groups[testCase.groupId].isStopped())
			}
		})
	}
}

// getManagerWithMockGroup creates a KafkaConsumerGroupManager and optionally seeds it with a mock consumer group
func getManagerWithMockGroup(groupId string, factoryErr bool) (KafkaConsumerGroupManager, *commontesting.MockConsumerGroup, *controltesting.MockServerHandler) {
	serverHandler := controltesting.GetMockServerHandler()
	manager := NewConsumerGroupManager(serverHandler, &mockKafkaConsumerGroupFactory{CreateErr: factoryErr})
	if groupId != "" {
		group := commontesting.NewMockConsumerGroup()
		manager.AddExistingGroup(groupId, group, []string{}, nil, nil)
		return manager, group, serverHandler
	}
	return manager, nil, serverHandler
}
