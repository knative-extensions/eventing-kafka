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
	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	ctrl "knative.dev/control-protocol/pkg"
	"knative.dev/control-protocol/pkg/message"
	ctrlservice "knative.dev/control-protocol/pkg/service"
	"knative.dev/eventing-kafka/pkg/common/controlprotocol"
	"knative.dev/eventing-kafka/pkg/common/controlprotocol/commands"
	"testing"
	"time"
)

type mockServerHandler struct {
	handlers map[ctrl.OpCode]controlprotocol.AsyncHandlerFunc
}

var _ controlprotocol.ServerHandler = (*mockServerHandler)(nil)

func (s *mockServerHandler) AddAsyncHandler(opcode ctrl.OpCode, _ message.AsyncCommand, handler controlprotocol.AsyncHandlerFunc) {
	s.handlers[opcode] = handler
}

func (s *mockServerHandler) Shutdown() {}
func (s *mockServerHandler) AddSyncHandler(_ ctrl.OpCode, _ ctrl.MessageHandlerFunc) {}
func (s *mockServerHandler) RemoveHandler(_ ctrl.OpCode) {}

func getMockServerHandler() *mockServerHandler {
	return &mockServerHandler{handlers:make(map[ctrl.OpCode]controlprotocol.AsyncHandlerFunc)}
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

func (m *mockConsumerGroupErrors) Consume(_ context.Context, _ []string, _ sarama.ConsumerGroupHandler) error { return nil }
func (m *mockConsumerGroupErrors) Errors() <-chan error { return m.errors }
func (m *mockConsumerGroupErrors) Close() error { return m.closeErr }

func TestNewConsumerGroupManager(t *testing.T) {
	server := getMockServerHandler()
	manager := NewConsumerGroupManager(server, &mockKafkaConsumerFactory{})
	assert.NotNil(t, manager)
	assert.NotNil(t, server.handlers[commands.StopConsumerGroupOpCode])
	assert.NotNil(t, server.handlers[commands.StartConsumerGroupOpCode])
	server.handlers[commands.StopConsumerGroupOpCode](context.TODO(), ctrlservice.AsyncCommandMessage{})
	server.handlers[commands.StartConsumerGroupOpCode](context.TODO(), ctrlservice.AsyncCommandMessage{})
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
	err := <- group.errors
	assert.NotNil(t, err)
	assert.Equal(t, "first", err.Error())
	group.Stop()
	close(mockGroup.errors)  // Close on a "stopped" consumergroup should wait for a restart
	time.Sleep(5 * time.Millisecond) // Let the error handling loop call WaitForStart()

	// Second cycle
	mockGroup.errors = make(chan error)
	group.Start() // Signals the error transfer loop to start processing again
	mockGroup.errors <- fmt.Errorf("second")
	err = <- group.errors
	assert.NotNil(t, err)
	assert.Equal(t, "second", err.Error())
	close(mockGroup.errors)  // Close on a consumergroup that is not "stopped"

	time.Sleep(5 * time.Millisecond) // Give the goroutine a chance to exit
}

func TestAddExistingGroup(t *testing.T) {
	manager := NewConsumerGroupManager(getMockServerHandler(), &mockKafkaConsumerFactory{})
	group := &mockConsumerGroupErrors{errors: make(chan error)}
	manager.AddExistingGroup("testid", group, []string{}, nil, nil)
	group.errors <- fmt.Errorf("test")
	assert.Equal(t, "test", (<- manager.Errors("testid")).Error())
	close(group.errors)
}

func TestStartConsumerGroup(t *testing.T) {
	for _, testCase := range []struct{
		name       string
		factoryErr bool
	}{
		{
			name: "No error",
		},
		{
			name: "Factory error",
			factoryErr: true,
		},
	}{
		t.Run(testCase.name, func(t *testing.T) {
			manager := NewConsumerGroupManager(getMockServerHandler(), &mockKafkaConsumerFactory{errOnStartup: testCase.factoryErr})
			err := manager.StartConsumerGroup("testid", []string{}, nil, nil)
			if testCase.factoryErr {
				assert.NotNil(t, err)
			} else {
				assert.Nil(t, err)
			}
		})
	}
}

func TestCloseConsumerGroup(t *testing.T) {
	for _, testCase := range []struct{
		name      string
		groupid   string
		expectErr bool
		closeErr  error
	}{
		{
			name: "Nonexistent GroupID",
			expectErr: true,
		},
		{
			name: "Existing GroupID",
			groupid: "test-group-id",
		},
		{
			name: "Existing GroupID, Error Closing Group",
			groupid: "test-group-id",
			closeErr: fmt.Errorf("test-error"),
			expectErr: true,
		},
	}{
		t.Run(testCase.name, func(t *testing.T) {
			manager := NewConsumerGroupManager(getMockServerHandler(), &mockKafkaConsumerFactory{})
			if testCase.groupid != "" {
				group := &mockConsumerGroupErrors{closeErr: testCase.closeErr}
				manager.AddExistingGroup(testCase.groupid, group, []string{}, nil, nil)
			}
			err := manager.CloseConsumerGroup(testCase.groupid)
			if testCase.expectErr {
				assert.NotNil(t, err)
			} else {
				assert.Nil(t, err)
			}
		})
	}
}

func TestConsume(t *testing.T) {
	for _, testCase := range []struct{
		name      string
		groupid   string
		stopped   bool
		expectErr bool
	}{
		{
			name: "Nonexistent GroupID",
			expectErr: true,
		},
		{
			name: "Existing GroupID, Started",
			groupid: "test-group-id",
		},
		{
			name:    "Existing GroupID, Stopped",
			groupid: "test-group-id",
			stopped: true,
		},
	}{
		t.Run(testCase.name, func(t *testing.T) {
			manager := NewConsumerGroupManager(getMockServerHandler(), &mockKafkaConsumerFactory{})
			if testCase.groupid != "" {
				group := &mockConsumerGroupErrors{}
				manager.AddExistingGroup(testCase.groupid, group, []string{}, nil, nil)
			}
			if testCase.stopped {
				// this case requires a goroutine so that we can "restart" the group out-of-band
				mgdGroup := manager.(*kafkaConsumerGroupManagerImpl).groups[testCase.groupid]
				mgdGroup.Stop()
				go func() {
					err := manager.Consume(testCase.groupid, context.TODO(), nil, nil)
					assert.Nil(t, err)
				}()
				time.Sleep(5 * time.Millisecond)  // Give Consume() a chance to call waitForStart()
				mgdGroup.Start()
			} else {
				err := manager.Consume(testCase.groupid, context.TODO(), nil, nil)
				if testCase.expectErr {
					assert.NotNil(t, err)
				} else {
					assert.Nil(t, err)
				}
			}
		})
	}
}

func TestErrors(t *testing.T) {
	for _, testCase := range []struct{
		name      string
		groupId   string
		expectErr bool
	}{
		{
			name:    "Existing GroupID",
			groupId: "test-group-id",
		},
		{
			name: "Nonexistent GroupID",
			expectErr: true,
		},
	}{
		t.Run(testCase.name, func(t *testing.T) {
			manager := NewConsumerGroupManager(getMockServerHandler(), &mockKafkaConsumerFactory{})
			if testCase.groupId != "" {
				manager.AddExistingGroup(testCase.groupId, &mockConsumerGroupErrors{errors: make(chan error)}, []string{}, nil, nil)
			}
			valid := manager.IsValid(testCase.groupId)
			errors := manager.Errors(testCase.groupId)
			if testCase.expectErr {
				assert.False(t, valid)
				assert.Nil(t, errors)
			} else {
				assert.True(t, valid)
				assert.NotNil(t, errors)
			}
		})
	}
}
