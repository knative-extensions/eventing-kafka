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
	"k8s.io/apimachinery/pkg/types"
	ctrlservice "knative.dev/control-protocol/pkg/service"
	"knative.dev/pkg/logging"

	"knative.dev/eventing-kafka/pkg/common/controlprotocol"
	"knative.dev/eventing-kafka/pkg/common/controlprotocol/commands"
)

const (
	defaultLockTimeout = 5 * time.Minute
	internalToken      = "internal-token"
)

// SubscriberStatus keeps track of the difference between active, failed, and stopped subscribers
type SubscriberStatus struct {
	Stopped bool  // A stopped subscriber is active but suspended ("paused") and is not processing events
	Error   error // A subscriber with a non-nil error has failed
}

// SubscriberStatusMap defines the map type which holds a collection of Subscribers by UID and their status
type SubscriberStatusMap map[types.UID]SubscriberStatus

// FailedCount returns the count of subscribers represented by this map which have an errors associated with them
func (s SubscriberStatusMap) FailedCount() int {
	failed := 0
	for _, status := range s {
		if status.Error != nil {
			failed++
		}
	}
	return failed
}

// EventIndex is the type of Event used when sending ManagerEvent structs via the notifyChannels list
type EventIndex int

// Events
const (
	GroupCreated EventIndex = iota + 1
	GroupStopped
	GroupStarted
	GroupClosed
)

// ManagerEvent is the struct used by the notification channel
type ManagerEvent struct {
	Event   EventIndex
	GroupId string
}

// KafkaConsumerGroupManager keeps track of Sarama consumer groups and handles messages from control-protocol clients
type KafkaConsumerGroupManager interface {
	Reconfigure(brokers []string, config *sarama.Config) *ReconfigureError
	StartConsumerGroup(ctx context.Context, groupId string, topics []string, handler KafkaConsumerHandler, ref types.NamespacedName, options ...SaramaConsumerHandlerOption) error
	CloseConsumerGroup(groupId string) error
	Errors(groupId string) <-chan error
	IsManaged(groupId string) bool
	IsStopped(groupId string) bool
	GetNotificationChannel() <-chan ManagerEvent
	ClearNotifications()
}

// groupMap is a mapping of GroupIDs to managed Consumer Group interfaces
type groupMap map[string]managedGroup

// kafkaConsumerGroupManagerImpl is the primary implementation of a KafkaConsumerGroupManager, which
// handles control protocol messages and stopping/starting ("pausing/resuming") of ConsumerGroups.
type kafkaConsumerGroupManagerImpl struct {
	logger         *zap.Logger
	server         controlprotocol.ServerHandler
	factory        *kafkaConsumerGroupFactoryImpl
	groups         groupMap
	groupLock      sync.RWMutex // Synchronizes write access to the groupMap
	notifyChannels []chan ManagerEvent
	eventLock      sync.Mutex
	offsetsChecker ConsumerGroupOffsetsChecker
}

// Verify that the kafkaConsumerGroupManagerImpl satisfies the KafkaConsumerGroupManager interface
var _ KafkaConsumerGroupManager = (*kafkaConsumerGroupManagerImpl)(nil)

// ReconfigureError is a custom error type returned by the Reconfigure() function.
type ReconfigureError struct {
	MultiError error    // A MultiError with all ConsumerGroup failures.
	GroupIds   []string // The ConsumerGroup IDs which failed to stop/start.
}

// Error implements the golang error interface.
func (r ReconfigureError) Error() string {
	var multiErrString string
	if r.MultiError != nil {
		multiErrString = r.MultiError.Error()
	}
	return fmt.Sprintf("Reconfigure Failed: MultiErr='%s', GroupIds='%+v'", multiErrString, r.GroupIds)
}

// Ensure ReconfigureError Is A Valid Error
var _ error = (*ReconfigureError)(nil)

// NewConsumerGroupManager returns a new kafkaConsumerGroupManagerImpl as a KafkaConsumerGroupManager interface
func NewConsumerGroupManager(logger *zap.Logger, serverHandler controlprotocol.ServerHandler, brokers []string, config *sarama.Config, offsetsChecker ConsumerGroupOffsetsChecker, enqueue func(ref types.NamespacedName)) KafkaConsumerGroupManager {

	manager := &kafkaConsumerGroupManagerImpl{
		logger:         logger,
		server:         serverHandler,
		groups:         make(groupMap),
		factory:        &kafkaConsumerGroupFactoryImpl{addrs: brokers, config: config, offsetsChecker: offsetsChecker, enqueue: enqueue},
		groupLock:      sync.RWMutex{},
		eventLock:      sync.Mutex{},
		offsetsChecker: offsetsChecker,
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

// GetNotificationChannel creates a new ManagerEvent channel and returns it.  Manager events will be
// broadcast to all channels created this way.
func (m *kafkaConsumerGroupManagerImpl) GetNotificationChannel() <-chan ManagerEvent {
	eventChan := make(chan ManagerEvent)
	m.eventLock.Lock()
	m.notifyChannels = append(m.notifyChannels, eventChan)
	m.eventLock.Unlock()
	return eventChan
}

// ClearNotifications closes and removes all channels from the notification list
func (m *kafkaConsumerGroupManagerImpl) ClearNotifications() {
	for _, eventChan := range m.notifyChannels {
		close(eventChan)
	}
	m.eventLock.Lock()
	m.notifyChannels = nil
	m.eventLock.Unlock()
}

// notify will send the given ManagerEvent to all channels in the notifyChannels list
func (m *kafkaConsumerGroupManagerImpl) notify(event ManagerEvent) {
	m.logger.Debug("Notifying channels", zap.Int("count", len(m.notifyChannels)), zap.Any("event", event))
	for _, eventChan := range m.notifyChannels {
		// Don't block if the receiver isn't listening
		select {
		case eventChan <- event:
		default:
		}
	}
}

// Reconfigure will incorporate a new set of brokers and Sarama config settings into the manager
// without requiring a new control-protocol server or losing the current map of managed groups.
// It will stop and start all the managed groups in the group map.
func (m *kafkaConsumerGroupManagerImpl) Reconfigure(brokers []string, config *sarama.Config) *ReconfigureError {

	m.logger.Info("Reconfigure Consumer Group Manager - Stopping All Managed Consumer Groups")

	var multiErr error
	var failedGroups []string
	groupsToRestart := make([]string, 0, len(m.groups))
	for groupId := range m.groups {

		// Do NOT Attempt To Reconfigure Stopped ConsumerGroups! (Would unintentionally start previously stopped ConsumerGroups)
		if !m.IsStopped(groupId) {
			err := m.stopConsumerGroup(&commands.CommandLock{Token: internalToken, LockBefore: true}, groupId)
			if err != nil {
				// If we couldn't stop a group, or failed to obtain a lock, note it as an error.  However,
				// in a practical sense, the new brokers/config will be used when whatever locked the group
				// restarts it anyway.
				multierr.AppendInto(&multiErr, err)

				// Remove The Group From The Manager So That It Will Be Restarted On Next Reconciliation
				m.removeGroup(groupId)
				failedGroups = append(failedGroups, groupId)
			} else {
				// Only attempt to restart groups that this function stopped
				groupsToRestart = append(groupsToRestart, groupId)
			}
		}
	}

	m.factory = &kafkaConsumerGroupFactoryImpl{addrs: brokers, config: config, offsetsChecker: m.offsetsChecker}

	// Restart any groups this function stopped
	m.logger.Info("Reconfigure Consumer Group Manager - Starting All Managed Consumer Groups")
	for _, groupId := range groupsToRestart {
		err := m.startConsumerGroup(&commands.CommandLock{Token: internalToken, UnlockAfter: true}, groupId)
		if err != nil {
			multierr.AppendInto(&multiErr, err)

			// Remove The Group From The Manager So That It Will Be Restarted On Next Reconciliation
			m.removeGroup(groupId)
			failedGroups = append(failedGroups, groupId)
		}
	}

	// Create & Return ReconfigureError If Necessary
	if multiErr != nil {
		return &ReconfigureError{MultiError: multiErr, GroupIds: failedGroups}
	} else {
		return nil
	}
}

// StartConsumerGroup uses the consumer factory to create a new ConsumerGroup, add it to the list
// of managed groups (for start/stop functionality) and start the Consume loop.
func (m *kafkaConsumerGroupManagerImpl) StartConsumerGroup(ctx context.Context, groupId string, topics []string, handler KafkaConsumerHandler, ref types.NamespacedName, options ...SaramaConsumerHandlerOption) error {
	logger := logging.FromContext(ctx)

	groupLogger := m.logger.With(zap.String("GroupId", groupId))
	groupLogger.Info("Creating New Managed ConsumerGroup")
	group, err := m.factory.createConsumerGroup(groupId)
	if err != nil {
		groupLogger.Error("Failed To Create New Managed ConsumerGroup")
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())

	// consume is passed in to the KafkaConsumerGroupFactory so that it will call the manager's
	// consume() function instead of the one on the internal sarama ConsumerGroup.  This allows the
	// manager to continue to block in the Consume call while a group goes through a stop/start cycle.
	consume := func(ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error {
		logger.Debug("Consuming Messages On managed Consumer Group", zap.String("GroupId", groupId))
		return m.consume(ctx, groupId, topics, handler)
	}

	// The only things we really want from the factory are the cancel function for the customConsumerGroup
	// and the error channel (if the error channel isn't read it will fill up and block between consume calls)
	customGroup := m.factory.startExistingConsumerGroup(groupId, group, consume, topics, logger, handler, ref, options...)
	managedGrp := createManagedGroup(ctx, m.logger, group, customGroup.Errors(), cancel, customGroup.cancel)

	// Add the Sarama ConsumerGroup we obtained from the factory to the managed group map,
	// so that it can be stopped and started via control-protocol messages.
	m.setGroup(groupId, managedGrp)
	m.notify(ManagerEvent{Event: GroupCreated, GroupId: groupId})
	return nil
}

// CloseConsumerGroup calls the Close function on the ConsumerGroup embedded in the managedGroup
// associated with the given groupId, and also closes its managed errors channel.  It then removes the
// group from management.
func (m *kafkaConsumerGroupManagerImpl) CloseConsumerGroup(groupId string) error {
	groupLogger := m.logger.With(zap.String("GroupId", groupId))
	groupLogger.Info("Closing ConsumerGroup and removing from management")
	managedGrp := m.getGroup(groupId)
	if managedGrp == nil {
		groupLogger.Warn("CloseConsumerGroup called on unmanaged group")
		return fmt.Errorf("could not close consumer group with id '%s' - group is not present in the managed map", groupId)
	}
	if err := managedGrp.close(); err != nil {
		groupLogger.Error("Failed To Close Managed ConsumerGroup", zap.Error(err))
		return err
	}

	// Remove this groupId from the map so that manager functions may not be called on it
	m.removeGroup(groupId)
	m.notify(ManagerEvent{Event: GroupClosed, GroupId: groupId})

	return nil
}

// Errors returns the errors channel of the managedGroup associated with the given groupId.  This channel
// is different than using the Errors() channel of a ConsumerGroup directly, as it will remain open during
//  a stop/start ("pause/resume") cycle
func (m *kafkaConsumerGroupManagerImpl) Errors(groupId string) <-chan error {
	group := m.getGroup(groupId)
	if group == nil {
		return nil
	}
	return group.errors()
}

// IsManaged returns true if the given groupId corresponds to a managed ConsumerGroup
func (m *kafkaConsumerGroupManagerImpl) IsManaged(groupId string) bool {
	return m.getGroup(groupId) != nil
}

// IsStopped returns true if the given groupId corresponds to a stopped ConsumerGroup
func (m *kafkaConsumerGroupManagerImpl) IsStopped(groupId string) bool {
	group := m.getGroup(groupId)
	if group == nil {
		return false // If it's not a managed group, it can't be stopped
	}

	return group.isStopped()
}

// Consume calls the Consume method of a managed consumer group, using a loop to call it again if that
// group is restarted by the manager.  If the Consume call is terminated by some other mechanism, the
// result will be returned to the caller.
func (m *kafkaConsumerGroupManagerImpl) consume(ctx context.Context, groupId string, topics []string, handler sarama.ConsumerGroupHandler) error {
	managedGrp := m.getGroup(groupId)
	if managedGrp == nil {
		return fmt.Errorf("consume called on nonexistent groupId '%s'", groupId)
	}
	return managedGrp.consume(ctx, topics, handler)
}

// stopConsumerGroups closes the managed ConsumerGroup identified by the provided groupId, and marks it
// as "stopped" (that is, "able to be restarted" as opposed to being closed by something outside the manager)
func (m *kafkaConsumerGroupManagerImpl) stopConsumerGroup(lock *commands.CommandLock, groupId string) error {
	groupLogger := m.logger.With(zap.String("GroupId", groupId))

	// Lock the managedGroup before stopping it, if lock.LockBefore is true
	if err := m.lockBefore(lock, groupId); err != nil {
		groupLogger.Error("Failed to lock consumer group prior to stopping", zap.Error(err))
		return err
	}

	groupLogger.Info("Stopping Managed ConsumerGroup")

	managedGrp := m.getGroup(groupId)
	if managedGrp == nil {
		groupLogger.Info("ConsumerGroup Not Managed - Ignoring Stop Request")
		return fmt.Errorf("stop requested for consumer group not in managed list: %s", groupId)
	}

	if err := managedGrp.stop(); err != nil {
		groupLogger.Error("Failed to stop managed consumer group", zap.Error(err))
		return err
	}

	// Unlock the managedGroup after stopping it, if lock.UnlockAfter is true
	if err := m.unlockAfter(lock, groupId); err != nil {
		groupLogger.Error("Failed to unlock consumer group after stopping", zap.Error(err))
		return err
	}
	m.notify(ManagerEvent{Event: GroupStopped, GroupId: groupId})
	return nil
}

// startConsumerGroups creates a new Consumer Group based on the groupId provided
func (m *kafkaConsumerGroupManagerImpl) startConsumerGroup(lock *commands.CommandLock, groupId string) error {
	groupLogger := m.logger.With(zap.String("GroupId", groupId))

	// Lock the managedGroup before starting it, if lock.LockBefore is true
	if err := m.lockBefore(lock, groupId); err != nil {
		groupLogger.Error("Failed to lock consumer group prior to starting", zap.Error(err))
		return err
	}

	groupLogger.Info("Starting Managed ConsumerGroup")
	managedGrp := m.getGroup(groupId)
	if managedGrp == nil {
		groupLogger.Info("ConsumerGroup Not Managed - Ignoring Start Request")
		return fmt.Errorf("start requested for consumer group not in managed list: %s", groupId)
	}

	createGroup := func() (sarama.ConsumerGroup, error) {
		return m.factory.createConsumerGroup(groupId)
	}

	// Instruct the managed group to use this new ConsumerGroup
	err := managedGrp.start(createGroup)
	if err != nil {
		groupLogger.Error("Failed To Restart Managed ConsumerGroup", zap.Error(err))
		return err
	}

	// Unlock the managedGroup after starting it, if lock.UnlockAfter is true
	if err = m.unlockAfter(lock, groupId); err != nil {
		groupLogger.Error("Failed to unlock consumer group after starting", zap.Error(err))
		return err
	}
	m.notify(ManagerEvent{Event: GroupStarted, GroupId: groupId})
	return nil
}

// getGroup returns a group from the groups map using the groupLock mutex
func (m *kafkaConsumerGroupManagerImpl) getGroup(groupId string) managedGroup {
	m.groupLock.RLock()
	defer m.groupLock.RUnlock()
	return m.groups[groupId]
}

// setGroup associates a group with a groupId in the groups map using the groupLock mutex
func (m *kafkaConsumerGroupManagerImpl) setGroup(groupId string, group managedGroup) {
	m.groupLock.Lock()
	defer m.groupLock.Unlock()
	m.groups[groupId] = group
}

// getGroup removes a group from the groups map by groupId, using the groupLock mutex
func (m *kafkaConsumerGroupManagerImpl) removeGroup(groupId string) {
	m.groupLock.Lock()
	defer m.groupLock.Unlock()
	delete(m.groups, groupId)
}

// lockBefore will lock the managedGroup corresponding to the groupId, if lock.LockBefore is true
func (m *kafkaConsumerGroupManagerImpl) lockBefore(lock *commands.CommandLock, groupId string) error {
	group := m.getGroup(groupId)
	if group == nil {
		m.logger.Warn("Attempted to lock a nonexistent group ID", zap.String("GroupId", groupId))
		return nil // Can't lock a nonexistent group
	}
	return group.processLock(lock, true)
}

// unlockAfter will unlock the managedGroup corresponding to the groupId, if lock.UnlockAfter is true
func (m *kafkaConsumerGroupManagerImpl) unlockAfter(lock *commands.CommandLock, groupId string) error {
	group := m.getGroup(groupId)
	if group == nil {
		m.logger.Warn("Attempted to unlock a nonexistent group ID", zap.String("GroupId", groupId))
		return nil // Can't unlock a nonexistent group
	}
	return group.processLock(lock, false)
}

// processAsyncGroupNotification calls the provided groupFunction with whatever GroupId is contained
// in the commandMessage, after verifying that the command version is correct.  It then calls the
// appropriate Async response function on the commandMessage (NotifyFailed or NotifySuccess)
func processAsyncGroupNotification(commandMessage ctrlservice.AsyncCommandMessage, groupFunction func(lock *commands.CommandLock, groupId string) error) {
	cmd, ok := commandMessage.ParsedCommand().(*commands.ConsumerGroupAsyncCommand)
	if !ok {
		return
	}
	if cmd.Version != commands.ConsumerGroupAsyncCommandVersion {
		commandMessage.NotifyFailed(fmt.Errorf("version mismatch; expected %d but got %d", commands.ConsumerGroupAsyncCommandVersion, cmd.Version))
		return
	}
	err := groupFunction(cmd.Lock, cmd.GroupId)
	if err != nil {
		commandMessage.NotifyFailed(err)
		return
	}
	commandMessage.NotifySuccess()
}
