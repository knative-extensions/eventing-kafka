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

/*
The KafkaConsumerGroupManager is an optional piece of support infrastructure that may be used to
automatically manage the starting and stopping (or "pausing and resuming") of Sarama ConsumerGroups
in response to messages sent via the control-protocol.
The manager uses the KafkaConsumerGroupFactory for underlying ConsumerGroup creation.

Usage:
- Create a ServerHandler and call NewConsumerGroupManager()
- Use the manager's StartConsumerGroup() and CloseConsumerGroup() functions instead of creating
  and closing sarama ConsumerGroups directly
- Errors() obtains an error channel for the managed group (persists between stop/start actions)
- IsManaged() returns true if a given GroupId is under management
- Reconfigure() allows you to change consumer factory settings (automatically stopping and
  restarting all managed ConsumerGroups)
*/

package consumer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	ctrlservice "knative.dev/control-protocol/pkg/service"

	"knative.dev/eventing-kafka/pkg/common/controlprotocol"
	"knative.dev/eventing-kafka/pkg/common/controlprotocol/commands"
)

const (
	defaultLockTimeout = 5 * time.Minute
)

// GroupLockedError is the error returned if the a locked managed group is given a different token for access
var GroupLockedError = fmt.Errorf("managed group lock failed: locked by a different token")

// KafkaConsumerGroupManager keeps track of Sarama consumer groups and handles messages from control-protocol clients
type KafkaConsumerGroupManager interface {
	Reconfigure(brokers []string, config *sarama.Config) error
	StartConsumerGroup(groupId string, topics []string, logger *zap.SugaredLogger, handler KafkaConsumerHandler, options ...SaramaConsumerHandlerOption) error
	CloseConsumerGroup(groupId string) error
	Errors(groupId string) <-chan error
	IsManaged(groupId string) bool
}

// groupMap is a mapping of GroupIDs to managed Consumer Group pointers (fields in the managedGroup
// are modified directly by the kafkaConsumerGroupManagerImpl receiver functions; the pointer makes
// removing and re-adding them to the map unnecessary in those cases)
type groupMap map[string]*managedGroup

// kafkaConsumerGroupManagerImpl is the primary implementation of a KafkaConsumerGroupManager, which
// handles control protocol messages and stopping/starting ("pausing/resuming") of ConsumerGroups.
type kafkaConsumerGroupManagerImpl struct {
	logger    *zap.Logger
	server    controlprotocol.ServerHandler
	factory   *kafkaConsumerGroupFactoryImpl
	groups    groupMap
	groupLock sync.RWMutex // Synchronizes write access to the groupMap
}

// Verify that the kafkaConsumerGroupManagerImpl satisfies the KafkaConsumerGroupManager interface
var _ KafkaConsumerGroupManager = (*kafkaConsumerGroupManagerImpl)(nil)

// NewConsumerGroupManager returns a new kafkaConsumerGroupManagerImpl as a KafkaConsumerGroupManager interface
func NewConsumerGroupManager(logger *zap.Logger, serverHandler controlprotocol.ServerHandler, brokers []string, config *sarama.Config) KafkaConsumerGroupManager {

	manager := &kafkaConsumerGroupManagerImpl{
		logger:    logger,
		server:    serverHandler,
		groups:    make(groupMap),
		factory:   &kafkaConsumerGroupFactoryImpl{addrs: brokers, config: config},
		groupLock: sync.RWMutex{},
	}

	logger.Info("Registering Consumer Group Manager Control-Protocol Handlers")

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
// without requiring a new control-protocol server or losing the current map of managed groups.
// It will stop and start all of the managed groups in the group map.
func (m *kafkaConsumerGroupManagerImpl) Reconfigure(brokers []string, config *sarama.Config) error {
	m.logger.Info("Reconfigure Consumer Group Manager - Stopping All Managed Consumer Groups")
	var multiErr error
	groupsToRestart := make([]string, 0, len(m.groups))
	for groupId := range m.groups {
		err := m.stopConsumerGroup(getInternalLockCommand(true), groupId)
		if err != nil {
			// If we couldn't stop a group, or failed to obtain a lock, note it as an error.  However,
			// in a practical sense, the new brokers/config will be used when whatever locked the group
			// restarts it anyway.
			multierr.AppendInto(&multiErr, err)
		} else {
			// Only attempt to restart groups that this function stopped
			groupsToRestart = append(groupsToRestart, groupId)
		}
	}

	m.factory = &kafkaConsumerGroupFactoryImpl{addrs: brokers, config: config}

	// Restart any groups this function stopped
	m.logger.Info("Reconfigure Consumer Group Manager - Starting All Managed Consumer Groups")
	for _, groupId := range groupsToRestart {
		err := m.startConsumerGroup(getInternalLockCommand(false), groupId)
		if err != nil {
			multierr.AppendInto(&multiErr, err)
		}
	}
	return multiErr
}

// getInternalLockCommand returns a CommandLock object with a constant lock token used internally by
// the consumer manager.
func getInternalLockCommand(lock bool) *commands.CommandLock {
	return &commands.CommandLock{
		Token:       "consumer-manager-internal-token",
		Timeout:     time.Minute,
		LockBefore:  lock,
		UnlockAfter: !lock,
	}
}

// StartConsumerGroup uses the consumer factory to create a new ConsumerGroup, add it to the list
// of managed groups (for start/stop functionality) and start the Consume loop.
func (m *kafkaConsumerGroupManagerImpl) StartConsumerGroup(groupId string, topics []string, logger *zap.SugaredLogger, handler KafkaConsumerHandler, options ...SaramaConsumerHandlerOption) error {
	groupLogger := m.logger.With(zap.String("GroupId", groupId))
	groupLogger.Info("Creating New Managed ConsumerGroup")
	group, err := m.factory.createConsumerGroup(groupId)
	if err != nil {
		groupLogger.Error("Failed To Create New Managed ConsumerGroup")
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	// Add the Sarama ConsumerGroup we obtained from the factory to the managed group map,
	// so that it can be stopped and started via control-protocol messages.
	m.groupLock.Lock()
	m.groups[groupId] = createManagedGroup(m.logger, group, cancel)
	m.groupLock.Unlock()

	// Begin listening on the group's Errors() channel and write them to the managedGroup's errors channel
	m.groups[groupId].transferErrors(ctx)

	// consume is passed in to the KafkaConsumerGroupFactory so that it will call the manager's
	// consume() function instead of the one on the internal sarama ConsumerGroup.  This allows the
	// manager to continue to block in the Consume call while a group goes through a stop/start cycle.
	consume := func(ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error {
		logger.Debug("Consuming Messages On managed Consumer Group", zap.String("GroupId", groupId))
		return m.consume(ctx, groupId, topics, handler)
	}

	// The only thing we really want from the factory is the cancel function for the customConsumerGroup
	customGroup := m.factory.startExistingConsumerGroup(group, consume, topics, logger, handler, options...)
	m.groups[groupId].cancelConsume = customGroup.cancel
	return nil
}

// CloseConsumerGroup calls the Close function on the ConsumerGroup embedded in the managedGroup
// associated with the given groupId, and also closes its managed errors channel.  It then removes the
// group from management.
func (m *kafkaConsumerGroupManagerImpl) CloseConsumerGroup(groupId string) error {
	groupLogger := m.logger.With(zap.String("GroupId", groupId))
	groupLogger.Info("Closing ConsumerGroup and removing from management")
	managedGrp, ok := m.groups[groupId]
	if !ok {
		groupLogger.Warn("CloseConsumerGroup called on unmanaged group")
		return fmt.Errorf("could not close consumer group with id '%s' - group is not present in the managed map", groupId)
	}
	// Make sure a managed group is "started" before closing the inner ConsumerGroup; otherwise anything
	// waiting for the manager to restart the group will never return.
	managedGrp.closeRestartChannel()
	if managedGrp.cancelErrors != nil {
		m.logger.Warn("The cancelErrors function of the managed group is nil")
		managedGrp.cancelErrors() // This will terminate any transferError call that is waiting for a restart
	}
	if managedGrp.cancelConsume != nil {
		m.logger.Warn("The cancelConsume function of the managed group is nil")
		managedGrp.cancelConsume() // This will stop the factory's consume loop after the ConsumerGroup is closed
	}
	err := managedGrp.group.Close()
	if err != nil {
		groupLogger.Error("Failed To Close Managed ConsumerGroup", zap.Error(err))
		return err
	}

	// Remove this groupId from the map so that manager functions may not be called on it
	m.groupLock.Lock()
	delete(m.groups, groupId)
	m.groupLock.Unlock()

	return nil
}

// Errors returns the errors channel of the managedGroup associated with the given groupId.  This channel
// is different than using the Errors() channel of a ConsumerGroup directly, as it will remain open during
//  a stop/start ("pause/resume") cycle
func (m *kafkaConsumerGroupManagerImpl) Errors(groupId string) <-chan error {
	if !m.IsManaged(groupId) {
		return nil
	}
	return m.groups[groupId].errors
}

// IsManaged returns true if the given groupId corresponds to a managed ConsumerGroup
func (m *kafkaConsumerGroupManagerImpl) IsManaged(groupId string) bool {
	m.groupLock.RLock()
	defer m.groupLock.RUnlock()
	if _, ok := m.groups[groupId]; ok {
		return true
	}
	return false
}

// Consume calls the Consume method of a managed consumer group, using a loop to call it again if that
// group is restarted by the manager.  If the Consume call is terminated by some other mechanism, the
// result will be returned to the caller.
func (m *kafkaConsumerGroupManagerImpl) consume(ctx context.Context, groupId string, topics []string, handler sarama.ConsumerGroupHandler) error {
	managedGrp := m.getGroup(groupId)
	if managedGrp == nil {
		return fmt.Errorf("consume called on nonexistent groupId '%s'", groupId)
	}
	for {
		// Call the internal sarama ConsumerGroup's Consume function directly
		err := managedGrp.group.Consume(ctx, topics, handler)
		if !managedGrp.isStopped() {
			m.logger.Debug("Managed Consume Finished Without Stop", zap.String("GroupId", groupId), zap.Error(err))
			// This ConsumerGroup wasn't stopped by the manager, so pass the error along to the caller
			return err
		}
		// Wait for the managed ConsumerGroup to be restarted
		m.logger.Debug("Consume is waiting for managed group restart")
		if !managedGrp.waitForStart(ctx) {
			// Context was canceled; abort
			m.logger.Debug("Managed Consume Canceled", zap.String("GroupId", groupId))
			return fmt.Errorf("context was canceled waiting for group '%s' to start", groupId)
		}
	}
}

// stopConsumerGroups closes the managed ConsumerGroup identified by the provided groupId, and marks it
// as "stopped" (that is, "able to be restarted" as opposed to being closed by something outside the manager)
func (m *kafkaConsumerGroupManagerImpl) stopConsumerGroup(lock *commands.CommandLock, groupId string) error {
	if err := m.processLock(lock, groupId, true); err != nil {
		return err
	}

	groupLogger := m.logger.With(zap.String("GroupId", groupId))
	groupLogger.Info("Stopping Managed ConsumerGroup")

	managedGrp := m.getGroup(groupId)
	if managedGrp == nil {
		groupLogger.Info("ConsumerGroup Not Managed - Ignoring Stop Request")
		return fmt.Errorf("stop requested for consumer group not in managed list: %s", groupId)
	}

	// The managedGroup's start channel must be created (that is, the managedGroup must be marked
	// as "stopped") before closing the internal ConsumerGroup, otherwise the consume function would
	// return control to the factory.
	managedGrp.createRestartChannel()
	// Close the inner sarama ConsumerGroup, which will cause our consume() function to stop
	// and wait for the managedGroup to start again.
	err := managedGrp.group.Close()
	if err != nil {
		groupLogger.Error("Failed To Close Managed ConsumerGroup", zap.Error(err))
		// Don't leave the start channel open if the group.Close() call failed; that would be misleading
		managedGrp.closeRestartChannel()
		return err
	}

	return m.processLock(lock, groupId, false)
}

// startConsumerGroups creates a new Consumer Group based on the groupId provided
func (m *kafkaConsumerGroupManagerImpl) startConsumerGroup(lock *commands.CommandLock, groupId string) error {
	if err := m.processLock(lock, groupId, true); err != nil {
		return err
	}

	groupLogger := m.logger.With(zap.String("GroupId", groupId))
	groupLogger.Info("Starting Managed ConsumerGroup")
	if !m.IsManaged(groupId) {
		groupLogger.Info("ConsumerGroup Not Managed - Ignoring Start Request")
		return fmt.Errorf("start requested for consumer group not in managed list: %s", groupId)
	}

	group, err := m.factory.createConsumerGroup(groupId)
	if err != nil {
		groupLogger.Error("Failed To Restart Managed ConsumerGroup", zap.Error(err))
		return err
	}

	m.groupLock.Lock()
	m.groups[groupId].group = group
	m.groups[groupId].closeRestartChannel() // Closing this allows the waitForStart function to finish
	m.groupLock.Unlock()

	return m.processLock(lock, groupId, false)
}

// getGroup returns a group from the groups map using the groupLock mutex
func (m *kafkaConsumerGroupManagerImpl) getGroup(groupId string) *managedGroup {
	m.groupLock.RLock()
	defer m.groupLock.RUnlock()
	return m.groups[groupId]
}

// processLock handles setting and removing the managedGroup's lock status.
// For the managedGroup with the given groupId, if that group exists and the provided CommandLock fields warrant it,
// this function will:
// - if "before" is true, set the lockedBy field of the group
// - if "before" is false, remove the lockedBy field of the group
// Returns an error if the lock.Token field doesn't match an existing token in the managedGroup (for either case), as
// this indicates that the group is locked by a different sender
func (m *kafkaConsumerGroupManagerImpl) processLock(lock *commands.CommandLock, groupId string, before bool) error {
	if lock == nil {
		return nil // No lock processing was requested
	}

	group := m.getGroup(groupId)
	if group == nil {
		return nil // Can't lock a nonexistent group
	}

	if !group.canUnlock(lock.Token) {
		m.logger.Info("Managed group access denied; already locked with a different token",
			zap.String("Token", lock.Token), zap.String("GroupId", groupId), zap.Bool("Before", before))
		return GroupLockedError // Already locked by a different command token
	}

	if before && lock.LockBefore {
		timeout := lock.Timeout
		if timeout == 0 {
			timeout = defaultLockTimeout
		}
		group.resetLockTimer(lock.Token, timeout)
	} else if !before && lock.UnlockAfter {
		group.removeLock()
	}

	return nil // Lock succeeded
}

// processAsyncGroupNotification calls the provided groupFunction with whatever GroupId is contained
// in the commandMessage, after verifying that the command version is correct.  It then calls the
// appropriate Async response function on the commandMessage (NotifyFailed or NotifySuccess)
func processAsyncGroupNotification(commandMessage ctrlservice.AsyncCommandMessage, groupFunction func(groupId string) error) {
	if cmd, ok := commandMessage.ParsedCommand().(*commands.ConsumerGroupAsyncCommand); ok {
		if cmd.Version != commands.ConsumerGroupAsyncCommandVersion {
			commandMessage.NotifyFailed(fmt.Errorf("version mismatch; expected %d but got %d", commands.ConsumerGroupAsyncCommandVersion, cmd.Version))
		} else {
			err := groupFunction(cmd.GroupId)
			if err != nil {
				commandMessage.NotifyFailed(err)
			} else {
				commandMessage.NotifySuccess()
			}
		}
	}
}
