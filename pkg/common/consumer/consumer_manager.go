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

	"github.com/Shopify/sarama"
	"go.uber.org/zap"
	ctrlservice "knative.dev/control-protocol/pkg/service"

	"knative.dev/eventing-kafka/pkg/common/controlprotocol"
	"knative.dev/eventing-kafka/pkg/common/controlprotocol/commands"
)

// NewConsumerGroupFnType Is A Function Definition Type For Wrapper Variables (Typesafe Stubbing For Tests)
type NewConsumerGroupFnType = func(brokers []string, groupId string, config *sarama.Config) (sarama.ConsumerGroup, error)

// KafkaConsumerGroupManager keeps track of Sarama consumer groups and handles messages from control-protocol clients
type KafkaConsumerGroupManager interface {
	Reconfigure(brokers []string, config *sarama.Config)
	StartConsumerGroup(groupID string, topics []string, logger *zap.SugaredLogger, handler KafkaConsumerHandler, options ...SaramaConsumerHandlerOption) error
	CloseConsumerGroup(groupId string) error
	Errors(groupId string) <-chan error
	IsValid(groupId string) bool
}

// groupMap is a mapping of GroupIDs to managed Consumer Group pointers (fields in the managedGroup
// are modified directly by the kafkaConsumerGroupManagerImpl receiver functions; the pointer makes
// removing and re-adding them to the map unnecessary in those cases)
type groupMap map[string]*managedGroup

// kafkaConsumerGroupManagerImpl is the primary implementation of a KafkaConsumerGroupManager, which
// handles control protocol messages and stopping/starting ("pausing/resuming") of ConsumerGroups.
type kafkaConsumerGroupManagerImpl struct {
	server    controlprotocol.ServerHandler
	factory   *kafkaConsumerGroupFactoryImpl
	groups    groupMap
	groupLock sync.RWMutex // Synchronizes write access to the groupMap
}

// Verify that the kafkaConsumerGroupManagerImpl satisfies the KafkaConsumerGroupManager interface
var _ KafkaConsumerGroupManager = (*kafkaConsumerGroupManagerImpl)(nil)

// NewConsumerGroupManager returns a new kafkaConsumerGroupManagerImpl as a KafkaConsumerGroupManager interface
func NewConsumerGroupManager(serverHandler controlprotocol.ServerHandler, brokers []string, config *sarama.Config) KafkaConsumerGroupManager {

	manager := &kafkaConsumerGroupManagerImpl{
		server:    serverHandler,
		groups:    make(groupMap),
		groupLock: sync.RWMutex{},
	}
	manager.Reconfigure(brokers, config)

	// Add a handler that understands the StopConsumerGroupOpCode and stops the requested group
	serverHandler.AddAsyncHandler(
		commands.StopConsumerGroupOpCode,
		commands.StopConsumerGroupResultOpCode,
		&commands.ConsumerGroupAsyncCommand{},
		func(ctx context.Context, commandMessage ctrlservice.AsyncCommandMessage) {
			processAsyncGroupNotification(commandMessage, manager.stopConsumerGroup)
		})

	// Add a handler that understands the StartConsumerGroupOpCode and starts the requested group
	serverHandler.AddAsyncHandler(
		commands.StartConsumerGroupOpCode,
		commands.StartConsumerGroupResultOpCode,
		&commands.ConsumerGroupAsyncCommand{},
		func(ctx context.Context, commandMessage ctrlservice.AsyncCommandMessage) {
			processAsyncGroupNotification(commandMessage, manager.startConsumerGroup)
		})

	return manager
}

// Reconfigure will incorporate a new set of brokers and Sarama config settings into the manager
// without requiring a new control-protocol server or losing the current map of managed groups
func (m *kafkaConsumerGroupManagerImpl) Reconfigure(brokers []string, config *sarama.Config) {
	m.factory = &kafkaConsumerGroupFactoryImpl{addrs: brokers, config: config}
}

// StartConsumerGroup uses the consumer factory to create a new ConsumerGroup, and start consuming.
func (m *kafkaConsumerGroupManagerImpl) StartConsumerGroup(groupId string, topics []string, logger *zap.SugaredLogger, handler KafkaConsumerHandler, options ...SaramaConsumerHandlerOption) error {
	group, err := m.factory.createConsumerGroup(groupId)
	if err != nil {
		return err
	}

	ctx := context.Background()
	// Add the Sarama ConsumerGroup we obtained from the factory to the managed group map,
	// so that it can be stopped and started via control-protocol messages.
	m.groupLock.Lock()
	m.groups[groupId] = &managedGroup{
		group:  group,
		errors: make(chan error),
	}
	m.groupLock.Unlock()

	// Begin listening on the group's Errors() channel and write them to the managedGroup's errors channel
	m.groups[groupId].transferErrors(ctx)

	// consume is passed in to the KafkaConsumerGroupFactory so that it will call the manager's
	// consume() function instead of the one on the internal sarama ConsumerGroup.  This allows the
	// manager to continue to block in the Consume call while a group goes through a stop/start cycle.
	consume := func(ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error {
		return m.consume(ctx, groupId, topics, handler)
	}

	_ = m.factory.startExistingConsumerGroup(ctx, group, consume, topics, logger, handler, options...)
	return nil
}

// CloseConsumerGroup calls the Close function on the ConsumerGroup embedded in the managedGroup
// associated with the given groupId, and also closes its managed errors channel
func (m *kafkaConsumerGroupManagerImpl) CloseConsumerGroup(groupId string) error {
	groupInfo, ok := m.groups[groupId]
	if !ok {
		return fmt.Errorf("could not close consumer group with id '%s' - group is not present in the managed map", groupId)
	}
	// Make sure a managed group is "started" before closing the inner ConsumerGroup; otherwise anything
	// waiting for the manager to restart the group will never return.
	groupInfo.Start()
	err := groupInfo.group.Close()
	if err != nil {
		return err
	}
	// Remove this groupId from the map so that manager functions may not be called on it
	delete(m.groups, groupId)
	return nil
}

// Errors returns the errors channel of the managedGroup associated with the given groupId.  This channel
// is different than using the Errors() channel of a ConsumerGroup directly, as it will remain open during
//  a stop/start ("pause/resume") cycle
func (m *kafkaConsumerGroupManagerImpl) Errors(groupId string) <-chan error {
	groupInfo, ok := m.groups[groupId]
	if !ok {
		return nil
	}
	return groupInfo.errors
}

// IsValid returns true if the given groupId corresponds to a managed ConsumerGroup
func (m *kafkaConsumerGroupManagerImpl) IsValid(groupId string) bool {
	if _, ok := m.groups[groupId]; ok {
		return true
	}
	return false
}

// Consume calls the Consume method of a managed consumer group, using a loop to call it again if that
// group is restarted by the manager.  If the Consume call is terminated by some other mechanism, the
// result will be returned to the caller.
func (m *kafkaConsumerGroupManagerImpl) consume(ctx context.Context, groupId string, topics []string, handler sarama.ConsumerGroupHandler) error {
	groupInfo, ok := m.groups[groupId]
	if !ok {
		return fmt.Errorf("consume called on nonexistent groupId '%s'", groupId)
	}
	for {
		// Call the internal sarama ConsumerGroup's Consume function directly
		err := groupInfo.group.Consume(ctx, topics, handler)
		if !groupInfo.isStopped() {
			// This ConsumerGroup wasn't stopped by the manager, so pass the error along to the caller
			return err
		}
		// Wait for the managed ConsumerGroup to be restarted
		if !groupInfo.waitForStart(ctx) {
			// Context was canceled; abort
			return fmt.Errorf("context was canceled waiting for group '%s' to start", groupId)
		}
	}
}

// stopConsumerGroups closes the managed ConsumerGroup identified by the provided groupId, and marks it
// as "stopped" (that is, "able to be restarted" as opposed to being closed by something outside the manager)
func (m *kafkaConsumerGroupManagerImpl) stopConsumerGroup(groupId string) error {
	groupInfo, ok := m.groups[groupId]
	if ok {
		// The managedGroup must be stopped before closing the internal ConsumerGroup, otherwise
		// the consume function would return control to the factory.
		groupInfo.Stop()
		// Close the inner sarama ConsumerGroup, which will cause our consume() function to stop
		// and wait for the managedGroup to start again.
		err := groupInfo.group.Close()
		if err != nil {
			// Don't leave the group "stopped" if the Close call failed; it would be misleading
			groupInfo.Start()
			return err
		}
		return nil
	}
	return fmt.Errorf("stop requested for consumer group not in managed list: %s", groupId)
}

// startConsumerGroups creates a new Consumer Group based on the groupId provided
func (m *kafkaConsumerGroupManagerImpl) startConsumerGroup(groupId string) error {
	m.groupLock.Lock() // Don't allow m.groups to be modified while processing a start request
	defer m.groupLock.Unlock()
	if _, ok := m.groups[groupId]; ok {
		group, err := m.factory.createConsumerGroup(groupId)
		if err != nil {
			return err
		}
		m.groups[groupId].group = group
		m.groups[groupId].Start()
		return nil
	}
	return fmt.Errorf("start requested for consumer group not in managed list: %s", groupId)
}

// processAsyncGroupNotification calls the provided groupFunction with whatever GroupId is contained
// in the commandMessage, after verifying that the command version is correct.  It then calls the
// appropriate Async response function on the commandMessage (NotifyFailed or NotifySuccess)
func processAsyncGroupNotification(commandMessage ctrlservice.AsyncCommandMessage, groupFunction func(groupId string) error) {
	if cmd, ok := commandMessage.ParsedCommand().(*commands.ConsumerGroupAsyncCommand); ok {
		if cmd.Version != commands.ConsumerGroupAsyncCommandVersion {
			commandMessage.NotifyFailed(fmt.Errorf("version mismatch; expected %d but got %d", commands.ConsumerGroupAsyncCommandVersion, cmd.Version))
		} else {
			// Calling NotifyFailed with a nil error is the same as calling NotifySuccess
			commandMessage.NotifyFailed(groupFunction(cmd.GroupId))
		}
	}
}

// managedGroup contains information about a Sarama ConsumerGroup that is required to start (i.e. re-create) it
type managedGroup struct {
	group     sarama.ConsumerGroup
	errors    chan error
	restartCh chan struct{}
}

// Stop sets the state of the managed group to "stopped" by creating the restartCh channel that
// will be closed when the group is restarted
func (m *managedGroup) Stop() {
	// If the restartCh is not nil, the channel is already stopped
	if m.restartCh == nil {
		m.restartCh = make(chan struct{})
	}
}

// Start sets the state of the managed group to "not stopped" by closing the restartCh channel
func (m *managedGroup) Start() {
	// If the restartCh is nil, the channel is not stopped
	if m.restartCh != nil {
		close(m.restartCh)
		m.restartCh = nil
	}
}

// isStopped is an accessor for the restartCh channel's status, which if non-nil indicates that
// the KafkaConsumerGroupManager stopped ("paused") the managed ConsumerGroup (versus it having
// been closed outside the manager)
func (m *managedGroup) isStopped() bool {
	return m.restartCh != nil
}

// waitForStart will block until a stopped ("paused") ConsumerGroup has been restarted by the
// KafkaConsumerGroupManager, returning true in that case or false if the context's cancel function is called
func (m *managedGroup) waitForStart(ctx context.Context) bool {
	if m.restartCh == nil {
		return true // group is already started; don't block on nil channel read
	}
	// Wait for either the restartCh (closed during the Start function) or the provided
	// context has its cancel function called.
	select {
	case <-m.restartCh:
		return true
	case <-ctx.Done():
		return false
	}
}

// transferErrors starts a goroutine that reads errors from the managedGroup's internal group.Errors() channel
// and sends them to the m.errors channel.  This is done so that when the group.Errors() channel is closed during
// a stop ("pause") of the group, the m.errors channel can remain open (so that users of the manager do not
// receive a closed error channel during stop/start events).
func (m *managedGroup) transferErrors(ctx context.Context) {
	go func() {
		for {
			for groupErr := range m.group.Errors() {
				m.errors <- groupErr
			}
			if !m.isStopped() {
				// If the error channel was closed without the consumergroup being stopped,
				// or if we were unable to wait for the group to be restarted, that is outside
				// of the manager's responsibility, so we are finished transferring errors.
				close(m.errors)
				return
			}
			// Wait for the manager to restart the Consumer Group before calling m.group.Errors() again
			if !m.waitForStart(ctx) {
				// Context was canceled; abort
				return
			}
		}
	}()
}
