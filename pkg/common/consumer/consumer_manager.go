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
	"time"

	"go.uber.org/multierr"
	"go.uber.org/zap"
	ctrlservice "knative.dev/control-protocol/pkg/service"
	"knative.dev/eventing-kafka/pkg/common/controlprotocol/commands"

	"github.com/Shopify/sarama"
	"k8s.io/apimachinery/pkg/util/wait"
	"knative.dev/eventing-kafka/pkg/common/controlprotocol"
)

// NewConsumerGroupFnType Is A Function Definition Types For A Wrapper Variables (Typesafe Stubbing For Tests)
type NewConsumerGroupFnType = func(brokers []string, groupId string, config *sarama.Config) (sarama.ConsumerGroup, error)

// KafkaConsumerGroupManager keeps track of Sarama consumer groups and handles messages from control-protocol clients
type KafkaConsumerGroupManager interface {
	AddExistingGroup(groupID string, group sarama.ConsumerGroup, topics []string, logger *zap.SugaredLogger, handler KafkaConsumerHandler, options ...SaramaConsumerHandlerOption)
	StartConsumerGroup(groupID string, topics []string, logger *zap.SugaredLogger, handler KafkaConsumerHandler, options ...SaramaConsumerHandlerOption) error
	CloseConsumerGroup(groupId string) error
	IsValid(groupId string) bool
	Errors(groupId string) <-chan error
	Consume(groupId string, ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error
	// Close ?   If the dispatcher tears down the CG, this needs to know about it
}

// passthroughManager does not handle the management of ConsumerGroups, the control protocol, or and start/stop
// functionality. Its purpose is to implement the KafkaConsumerGroupManager interface in a transparent way so that
// components that call the StartConsumerGroup() function in the KafkaConsumerGroupFactory can use it instead of the
// kafkaConsumerGroupManagerImpl if no management is desired.
type passthroughManager struct {
	groups map[string]sarama.ConsumerGroup
}

// Verify that the passthroughManager satisfies the KafkaConsumerGroupManager interface
var _ KafkaConsumerGroupManager = (*passthroughManager)(nil)

// AddExistingGroup places the given ConsumerGroup into the local groups map associated with the given groupID
func (p passthroughManager) AddExistingGroup(groupID string, group sarama.ConsumerGroup, _ []string, _ *zap.SugaredLogger, _ KafkaConsumerHandler, _ ...SaramaConsumerHandlerOption) {
	p.groups[groupID] = group
}

// Consume is called by the KafkaConsumerGroupFactory, so it needs to call the stored ConsumerGroup's Consume function
func (p passthroughManager) Consume(groupId string, ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error {
	group, ok := p.groups[groupId]
	if !ok {
		return fmt.Errorf("no group with id %s was added to the manager", groupId)
	}
	return group.Consume(ctx, topics, handler)
}

// StartConsumerGroup and the other functions here exist solely to satisfy the KafkaConsumerGroupManager
// interface.  They are not called by the KafkaConsumerGroupFactory and so do not need full implementations (yet).
func (p passthroughManager) StartConsumerGroup(_ string, _ []string, _ *zap.SugaredLogger, _ KafkaConsumerHandler, _ ...SaramaConsumerHandlerOption) error {
	return nil
}
func (p passthroughManager) CloseConsumerGroup(_ string) error { return nil }
func (p passthroughManager) IsValid(_ string) bool             { return false }
func (p passthroughManager) Errors(_ string) <-chan error      { return nil }

// NewPassthroughManager returns a passthroughManager as a KafkaConsumerGroupManager interface
func NewPassthroughManager() KafkaConsumerGroupManager {
	return passthroughManager{groups: make(map[string]sarama.ConsumerGroup)}
}

// managedGroup contains information about a Sarama ConsumerGroup that is required to start (i.e. re-create) it
type managedGroup struct {
	factory KafkaConsumerGroupFactory
	topics  []string
	logger  *zap.SugaredLogger
	handler KafkaConsumerHandler
	options []SaramaConsumerHandlerOption
	groupId string
	group   sarama.ConsumerGroup
	errors  chan error
	stopped bool
}

// isStopped is an accessor for the "stopped" flag that indicates whether the KafkaConsumerGroupManager stopped
// ("paused") the managed ConsumerGroup (versus it having been closed outside the manager)
func (m *managedGroup) isStopped() bool {
	return m.stopped
}

// waitForStart will block until a stopped ("paused") ConsumerGroup has been restarted by the
// KafkaConsumerGroupManager
func (m *managedGroup) waitForStart() error {
	// TODO:  Should wait for a proper signal/channel, not poll the stopped field
	// TODO:  Should have a way to shutdown if the managedGroup is closed
	return wait.PollInfinite(10*time.Millisecond, func() (bool, error) {
		return !m.stopped, nil
	})
}

// transferErrors starts a goroutine that reads errors from the managedGroup's internal group.Errors() channel
// and sends them to the m.errors channel.  This is done so that when the group.Errors() channel is closed during
// a stop ("pause") of the group, the m.errors channel can remain open (so that users of the manager do not
// receive a closed error channel during stop/start events).
func (m *managedGroup) transferErrors() {
	go func() {
		for {
			for groupErr := range m.group.Errors() {
				m.errors <- groupErr
			}
			if !m.isStopped() || m.waitForStart() != nil {
				// If the error channel was closed without the consumergroup being stopped,
				// or if we were unable to wait for the group to be restarted, that is outside
				// of the manager's responsibility, so we are finished transferring errors.
				close(m.errors)
				break
			}
		}
	}()
}

// groupMap is a mapping of GroupIDs to managed Consumer Group pointers (fields in the managedGroup
// are modified directly by the kafkaConsumerGroupManagerImpl receiver functions; the pointer makes
// removing and re-adding them to the map unnecessary in those cases)
type groupMap map[string]*managedGroup

// kafkaConsumerGroupManagerImpl is the primary implementation of a KafkaConsumerGroupManager, which
// handles control protocol messages and stopping/starting ("pausing/resuming") of ConsumerGroups.
type kafkaConsumerGroupManagerImpl struct {
	server    controlprotocol.ServerHandler
	factory   KafkaConsumerGroupFactory
	groups    groupMap
	groupLock sync.RWMutex // Synchronizes write access to the groupMap
}

// Verify that the kafkaConsumerGroupManagerImpl satisfies the KafkaConsumerGroupManager interface
var _ KafkaConsumerGroupManager = (*kafkaConsumerGroupManagerImpl)(nil)

// NewConsumerGroupManager returns a new kafkaConsumerGroupManagerImpl as a KafkaConsumerGroupManager interface
func NewConsumerGroupManager(serverHandler controlprotocol.ServerHandler, groupFactory KafkaConsumerGroupFactory) KafkaConsumerGroupManager {

	manager := &kafkaConsumerGroupManagerImpl{
		server:    serverHandler,
		factory:   groupFactory,
		groups:    make(groupMap),
		groupLock: sync.RWMutex{},
	}

	// Add a handler that understands the StopConsumerGroupOpCode and stops the requested group
	serverHandler.AddAsyncHandler(commands.StopConsumerGroupOpCode, &commands.ConsumerGroupAsyncCommand{},
		func(ctx context.Context, commandMessage ctrlservice.AsyncCommandMessage) {
			processAsyncGroupNotification(commandMessage, manager.stopConsumerGroups)
		})

	// Add a handler that understands the StartConsumerGroupOpCode and starts the requested group
	serverHandler.AddAsyncHandler(commands.StartConsumerGroupOpCode, &commands.ConsumerGroupAsyncCommand{},
		func(ctx context.Context, commandMessage ctrlservice.AsyncCommandMessage) {
			processAsyncGroupNotification(commandMessage, manager.startConsumerGroups)
		})

	return manager
}

// AddExistingGroup adds an existing Sarama ConsumerGroup to the managed group map,
// so that it can be stopped and started via control-protocol messages.
func (m *kafkaConsumerGroupManagerImpl) AddExistingGroup(groupID string, group sarama.ConsumerGroup, topics []string, logger *zap.SugaredLogger, handler KafkaConsumerHandler, options ...SaramaConsumerHandlerOption) {
	// Synchronize access to groups map
	m.groupLock.Lock()
	m.groups[groupID] = &managedGroup{
		factory: m.factory,
		topics:  topics,
		logger:  logger,
		handler: handler,
		options: options,
		groupId: groupID,
		group:   group,
		stopped: false,
		errors:  make(chan error),
	}
	m.groupLock.Unlock()

	// Listen on the group's Errors() channel and write them to the managedGroup's errors channel
	m.groups[groupID].transferErrors()
}

// StartConsumerGroup uses the consumer factory to create a new ConsumerGroup, and start consuming.
func (m *kafkaConsumerGroupManagerImpl) StartConsumerGroup(groupID string, topics []string, logger *zap.SugaredLogger, handler KafkaConsumerHandler, options ...SaramaConsumerHandlerOption) error {
	// We don't use the resulting ConsumerGroup because the AddExistingGroup function will be
	// called by the factory, and adding the group an additional time here would serve no purpose.
	_, err := m.factory.StartConsumerGroup(m, groupID, topics, logger, handler, options...)
	if err != nil {
		return err
	}
	return nil
}

// CloseConsumerGroup calls the Close function on the ConsumerGroup embedded in the managedGroup
// associate with the given groupId, and also closes its managed errors channel.
func (m *kafkaConsumerGroupManagerImpl) CloseConsumerGroup(groupId string) error {
	groupInfo, ok := m.groups[groupId]
	if !ok {
		return fmt.Errorf("could not close consumer group with id '%s' - group is not present in the managed map", groupId)
	}
	err := groupInfo.group.Close()
	if err != nil {
		return err
	}
	close(groupInfo.errors)
	// Remove this groupId from the map so that Consume, etc cannot be called on it
	delete(m.groups, groupId)
	return nil
}

// Consume calls the Consume method of a managed consumer group, using a loop to call it again if that
// group is restarted by the manager.  If the Consume call is terminated by some other mechanism, the
// result will be returned to the caller.
func (m *kafkaConsumerGroupManagerImpl) Consume(groupId string, ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error {
	groupInfo, ok := m.groups[groupId]
	if !ok {
		return fmt.Errorf("consume called on nonexistent groupId '%s'", groupId)
	}
	for {
		err := groupInfo.group.Consume(ctx, topics, handler)
		if !groupInfo.isStopped() {
			// This ConsumerGroup wasn't stopped by the manager, so pass the error along to the caller
			return err
		}
		// Wait for the managed ConsumerGroup to be restarted
		err = groupInfo.waitForStart()
		if err != nil {
			return err
		}
	}
}

// Errors returns the errors channel of the managedGroup associated with the given groupId.  This channel
// is different than using the Errors() channel of a ConsumerGroup directly, as it will remain open during
//  a stop/start ("pause/resume") cycle.
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

// stopConsumerGroups closes all of the managed Consumer Groups identified by the provided groupIds, and marks them
// as "stopped" (that is, "able to be restarted" as opposed to being closed by something outside the manager)
func (m *kafkaConsumerGroupManagerImpl) stopConsumerGroups(groupIds ...string) error {
	var multiErr error
	for _, id := range groupIds {
		groupInfo, ok := m.groups[id]
		if ok {
			groupInfo.stopped = true // TODO:  Thread-safety
			err := groupInfo.group.Close()
			if err != nil {
				multierr.AppendInto(&multiErr, err)
			}
		} else {
			multierr.AppendInto(&multiErr, fmt.Errorf("stop requested for consumer group not in managed list: %s", id))
		}
	}
	return nil
}

// startConsumerGroups creates new Consumer Groups based on the groupIds provided
func (m *kafkaConsumerGroupManagerImpl) startConsumerGroups(groupIds ...string) error {
	var multiErr error
	for _, id := range groupIds {
		groupInfo, ok := m.groups[id]
		if ok {
			group, err := m.factory.StartConsumerGroup(m, id, groupInfo.topics, groupInfo.logger, groupInfo.handler, groupInfo.options...)
			if err != nil {
				multierr.AppendInto(&multiErr, err)
			}
			m.groups[id].group = group
			m.groups[id].stopped = false
		} else {
			multierr.AppendInto(&multiErr, fmt.Errorf("start requested for consumer group not in managed list: %s", id))
		}
	}
	return multiErr
}

// processAsyncGroupNotification calls the provided groupFunction with whatever GroupId is contained
// in the commandMessage, after verifying that the command version is correct.  It then calls the
// appropriate Async response function on the commandMessage (NotifyFailed or NotifySuccess)
func processAsyncGroupNotification(commandMessage ctrlservice.AsyncCommandMessage, groupFunction func(groupIds ...string) error) {
	if cmd, ok := commandMessage.ParsedCommand().(*commands.ConsumerGroupAsyncCommand); ok {
		if cmd.Version != commands.ConsumerGroupAsyncCommandVersion {
			commandMessage.NotifyFailed(fmt.Errorf("version mismatch; expected %d but got %d", commands.ConsumerGroupAsyncCommandVersion, cmd.Version))
		}
		err := groupFunction(cmd.GroupId)
		if err != nil {
			// commandMessage.NotifyFailed(err)  // EDV: TODO:  Put this back
		} else {
			// commandMessage.NotifySuccess()  // EDV: TODO:  Put this back
		}
	}
}
