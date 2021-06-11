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
	"encoding"
	"fmt"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	ctrl "knative.dev/control-protocol/pkg"
	"knative.dev/control-protocol/pkg/message"
	ctrlservice "knative.dev/control-protocol/pkg/service"
	"knative.dev/eventing-kafka/pkg/common/controlprotocol"
	"knative.dev/eventing-kafka/pkg/common/controlprotocol/commands"
)

// mockControlProtocolService is a stub-only mock of the Control Protocol Service
type mockControlProtocolService struct{}

var _ ctrl.Service = (*mockControlProtocolService)(nil)

func (mockControlProtocolService) SendAndWaitForAck(_ ctrl.OpCode, _ encoding.BinaryMarshaler) error {
	return nil
}
func (mockControlProtocolService) MessageHandler(_ ctrl.MessageHandler) {}
func (mockControlProtocolService) ErrorHandler(_ ctrl.ErrorHandler)     {}

// mockServerHandler is a mock of the ServerHandler that only stores results from AddAsyncHandler
type mockServerHandler struct {
	router ctrlservice.MessageRouter
}

var _ controlprotocol.ServerHandler = (*mockServerHandler)(nil)

func (s *mockServerHandler) AddAsyncHandler(opcode ctrl.OpCode, resultOpcode ctrl.OpCode, payloadType message.AsyncCommand, handler controlprotocol.AsyncHandlerFunc) {
	s.router[opcode] = ctrlservice.NewAsyncCommandHandler(&mockControlProtocolService{}, payloadType, resultOpcode, handler)
}

func (s *mockServerHandler) Shutdown()                                               {}
func (s *mockServerHandler) AddSyncHandler(_ ctrl.OpCode, _ ctrl.MessageHandlerFunc) {}
func (s *mockServerHandler) RemoveHandler(_ ctrl.OpCode)                             {}

func getMockServerHandler() *mockServerHandler {
	return &mockServerHandler{router: make(ctrlservice.MessageRouter)}
}

type mockKafkaConsumerFactory struct {
	errOnStartup bool
}

var _ KafkaConsumerGroupFactory = (*mockKafkaConsumerFactory)(nil)

func (c mockKafkaConsumerFactory) StartConsumerGroup(_ KafkaConsumerGroupManager, _ string, _ []string, _ *zap.SugaredLogger, _ KafkaConsumerHandler, _ ...SaramaConsumerHandlerOption) (sarama.ConsumerGroup, error) {
	if c.errOnStartup {
		return nil, fmt.Errorf("test error")
	}
	return &mockConsumerGroup{}, nil
}

// mockConsumerGroupErrors allows direct manipulation of the channel returned by Errors()
type mockConsumerGroupErrors struct {
	errors   chan error
	closeErr error
}

var _ sarama.ConsumerGroup = (*mockConsumerGroupErrors)(nil)

func (m *mockConsumerGroupErrors) Consume(_ context.Context, _ []string, _ sarama.ConsumerGroupHandler) error {
	return nil
}
func (m *mockConsumerGroupErrors) Errors() <-chan error { return m.errors }
func (m *mockConsumerGroupErrors) Close() error         { return m.closeErr }

func TestNewConsumerGroupManager(t *testing.T) {
	server := getMockServerHandler()
	manager := NewConsumerGroupManager(server, &mockKafkaConsumerFactory{})
	assert.NotNil(t, manager)
	assert.NotNil(t, server.router[commands.StopConsumerGroupOpCode])
	assert.NotNil(t, server.router[commands.StartConsumerGroupOpCode])
}

func TestNewPassthroughManager(t *testing.T) {
	manager := NewPassthroughManager()
	assert.NotNil(t, manager)
	assert.Nil(t, manager.Errors(""))
	assert.False(t, manager.IsValid(""))
	assert.Nil(t, manager.CloseConsumerGroup(""))
	assert.Nil(t, manager.StartConsumerGroup("", []string{}, nil, nil))
	assert.NotNil(t, manager.Consume("", context.TODO(), []string{}, nil))
	manager.AddExistingGroup("", &mockConsumerGroup{}, []string{}, nil, nil)
	assert.Nil(t, manager.Consume("", context.TODO(), []string{}, nil))
}

func TestManagedGroup(t *testing.T) {
	group := managedGroup{}
	assert.False(t, group.isStopped())
	group.waitForStart()
	group.Stop()
	assert.True(t, group.isStopped())
}

func TestManagedGroup_transferErrors(t *testing.T) {
	mockGroup := &mockConsumerGroupErrors{errors: make(chan error)}
	group := managedGroup{
		factory: &mockKafkaConsumerFactory{},
		topics:  nil,
		logger:  nil,
		handler: nil,
		options: nil,
		groupId: "testid",
		group:   mockGroup,
		errors:  make(chan error),
		stopped: false,
	}

	// Run through an error channel "stop/start" cycle
	group.transferErrors()

	// First cycle
	mockGroup.errors <- fmt.Errorf("first")
	err := <-group.errors
	assert.NotNil(t, err)
	assert.Equal(t, "first", err.Error())
	group.Stop()
	close(mockGroup.errors)          // Close on a "stopped" consumergroup should wait for a restart
	time.Sleep(5 * time.Millisecond) // Let the error handling loop call WaitForStart()

	// Second cycle
	mockGroup.errors = make(chan error)
	group.Start() // Signals the error transfer loop to start processing again
	mockGroup.errors <- fmt.Errorf("second")
	err = <-group.errors
	assert.NotNil(t, err)
	assert.Equal(t, "second", err.Error())
	close(mockGroup.errors) // Close on a consumergroup that is not "stopped"

	time.Sleep(5 * time.Millisecond) // Give the goroutine a chance to exit
}

func TestAddExistingGroup(t *testing.T) {
	manager := NewConsumerGroupManager(getMockServerHandler(), &mockKafkaConsumerFactory{})
	group := &mockConsumerGroupErrors{errors: make(chan error)}
	manager.AddExistingGroup("testid", group, []string{}, nil, nil)
	group.errors <- fmt.Errorf("test")
	assert.Equal(t, "test", (<-manager.Errors("testid")).Error())
	close(group.errors)
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
			manager := NewConsumerGroupManager(getMockServerHandler(), &mockKafkaConsumerFactory{errOnStartup: testCase.factoryErr})
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
			closeErr:  fmt.Errorf("test-error"),
			expectErr: true,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			manager, group, _ := getManagerWithMockGroup(testCase.groupId, false)
			if group != nil {
				group.closeErr = testCase.closeErr
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
		expectErr bool
	}{
		{
			name:      "Nonexistent GroupID",
			expectErr: true,
		},
		{
			name:    "Existing GroupID, Started",
			groupId: "test-group-id",
		},
		{
			name:    "Existing GroupID, Stopped",
			groupId: "test-group-id",
			stopped: true,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			manager, _, _ := getManagerWithMockGroup(testCase.groupId, false)

			if testCase.stopped {
				// this case requires a goroutine so that we can "restart" the group out-of-band
				mgdGroup := manager.(*kafkaConsumerGroupManagerImpl).groups[testCase.groupId]
				mgdGroup.Stop()
				go func() {
					err := manager.Consume(testCase.groupId, context.TODO(), nil, nil)
					assert.Nil(t, err)
				}()
				time.Sleep(5 * time.Millisecond) // Give Consume() a chance to call waitForStart()
				mgdGroup.Start()
			} else {
				err := manager.Consume(testCase.groupId, context.TODO(), nil, nil)
				assert.Equal(t, testCase.expectErr, err != nil)
			}
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
			errors := manager.Errors(testCase.groupId)
			assert.Equal(t, testCase.expectErr, errors == nil)
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
			if testCase.closeErr {
				group.closeErr = fmt.Errorf("test error")
			}

			handler, ok := serverHandler.router[testCase.opcode]
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
func getManagerWithMockGroup(groupId string, factoryErr bool) (KafkaConsumerGroupManager, *mockConsumerGroupErrors, *mockServerHandler) {
	serverHandler := getMockServerHandler()
	manager := NewConsumerGroupManager(serverHandler, &mockKafkaConsumerFactory{errOnStartup: factoryErr})
	if groupId != "" {
		group := mockConsumerGroupErrors{errors: make(chan error)}
		manager.AddExistingGroup(groupId, &group, []string{}, nil, nil)
		return manager, &group, serverHandler
	}
	return manager, nil, serverHandler
}
