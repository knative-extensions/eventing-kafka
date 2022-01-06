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
	"time"

	"knative.dev/eventing-kafka/pkg/common/controlprotocol/commands"

	"github.com/Shopify/sarama"
	"go.uber.org/zap"
)

// GroupLockedError is the error returned if the a locked managed group is given a different token for access
var GroupLockedError = fmt.Errorf("managed group lock failed: locked by a different token")

// createSaramaGroupFn represents the mechanism the managedGroup should use when creating a Sarama ConsumerGroup
type createSaramaGroupFn func() (sarama.ConsumerGroup, error)

// managedGroup contains information about a Sarama ConsumerGroup that is required to start (i.e. re-create) it
type managedGroup interface {
	consume(context.Context, []string, sarama.ConsumerGroupHandler) error
	start(createSaramaGroupFn) error
	stop() error
	close() error
	errors() chan error
	processLock(*commands.CommandLock, bool) error
	isStopped() bool
}

// managedGroupImpl implements the managedGroup interface
type managedGroupImpl struct {
	logger             *zap.Logger
	saramaGroup        sarama.ConsumerGroup // The Sarama ConsumerGroup which is under management
	transferredErrors  chan error           // An error channel that will replicate the errors from the Sarama ConsumerGroup
	restartWaitChannel chan struct{}        // A channel that will be closed when a stopped group is restarted
	stopped            atomic.Value         // Boolean value indicating that the managed group is stopped
	cancelErrors       func()               // Called by the manager's CloseConsumerGroup to terminate the error forwarding
	cancelConsume      func()               // Called by the manager during CloseConsumerGroup
	lockedBy           atomic.Value         // The LockToken of the ConsumerGroupAsyncCommand that requested the lock
	cancelLockTimeout  func()               // Called internally to stop the lock timeout when a lock is removed
	groupMutex         sync.RWMutex         // Used to synchronize access to the internal sarama ConsumerGroup
}

// createManagedGroup associates a Sarama ConsumerGroup and cancel function (usually from the factory)
// inside a new managedGroup struct.  If a timeout is given (nonzero), the lockId will be reset to an
// empty string (i.e. "unlocked") after that time has passed.
func createManagedGroup(ctx context.Context, logger *zap.Logger, group sarama.ConsumerGroup, errors <-chan error, cancelErrors func(), cancelConsume func()) managedGroup {

	managedGrp := &managedGroupImpl{
		logger:            logger,
		saramaGroup:       group,
		transferredErrors: make(chan error),
		cancelErrors:      cancelErrors,
		cancelConsume:     cancelConsume,
		groupMutex:        sync.RWMutex{},
	}

	// Atomic values must be initialized with their desired type before being accessed, or a nil
	// interface will be returned that is not of that type (string or bool in these cases)
	managedGrp.lockedBy.Store("")   // Empty token string indicates "unlocked"
	managedGrp.stopped.Store(false) // A managed group defaults to "started" when created

	// Begin listening on the provided errors channel and write them to the managedGroup's errors channel
	managedGrp.transferErrors(ctx, errors)

	return managedGrp
}

// stop stops the managed group (which means closing the internal ConsumerGroup and marking the managed group as stopped)
func (m *managedGroupImpl) stop() error {
	// The managedGroup's start channel must be created (that is, the managedGroup must be marked
	// as "stopped") before closing the internal ConsumerGroup, otherwise the consume function would
	// return control to the factory.
	m.createRestartChannel()

	// Close the inner sarama ConsumerGroup, which will cause our consume() function to stop
	// and wait for the managedGroup to start again.
	if err := m.getSaramaGroup().Close(); err != nil {
		// Don't leave the start channel open if the group.Close() call failed; that would be misleading
		m.closeRestartChannel()
		return err
	}
	return nil
}

// start creates a Sarama ConsumerGroup using the provided createGroup function and marks the
// managedGroup as "started" by closing the restartWaitChannel
func (m *managedGroupImpl) start(createGroup createSaramaGroupFn) error {
	group, err := createGroup()
	if err != nil {
		return err
	}
	m.setSaramaGroup(group)
	m.closeRestartChannel() // Closing this allows the waitForStart function to finish
	return nil
}

// close shuts down the internal sarama ConsumerGroup, canceling the consume and/or error transfer
// loops first.  This is distinct from "stop" which expects the consume/error loops to continue while
// waiting for a restart.
func (m *managedGroupImpl) close() error {
	// Make sure a managed group is "started" before closing the inner ConsumerGroup; otherwise anything
	// waiting for the manager to restart the group will never return.
	m.closeRestartChannel()

	// Stop the error transfer and consume loops of this managedGroup, if cancel functions exist
	if m.cancelErrors == nil {
		m.logger.Warn("The cancelErrors function of the managed group is nil")
	} else {
		m.cancelErrors() // This will terminate any transferError call that is waiting for a restart
	}
	if m.cancelConsume == nil {
		m.logger.Warn("The cancelConsume function of the managed group is nil")
	} else {
		m.cancelConsume() // This will stop the factory's consume loop after the ConsumerGroup is closed
	}
	return m.getSaramaGroup().Close()
}

// consume calls the Consume function on the managed ConsumerGroup, supporting the stop/start functionality
func (m *managedGroupImpl) consume(ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error {
	for {
		// Call the internal sarama ConsumerGroup's Consume function directly
		err := m.getSaramaGroup().Consume(ctx, topics, handler)
		if !m.isStopped() {
			m.logger.Warn("Managed Consume Finished Without Stop", zap.Error(err))
			// This ConsumerGroup wasn't stopped by the manager, so pass the error along to the caller
			return err
		}
		// Wait for the managed ConsumerGroup to be restarted
		m.logger.Debug("Consume is waiting for managed group restart")
		if !m.waitForStart(ctx) {
			// Context was canceled; abort
			m.logger.Debug("Managed Consume Canceled")
			return fmt.Errorf("context was canceled waiting for group to start")
		}
	}
}

// getErrors returns the error channel, which is relayed from the internal managed ConsumerGroup
func (m *managedGroupImpl) errors() chan error {
	return m.transferredErrors
}

// getSaramaGroup returns the internal Sarama ConsumerGroup, protected by the mutex
func (m *managedGroupImpl) getSaramaGroup() sarama.ConsumerGroup {
	m.groupMutex.RLock()
	defer m.groupMutex.RUnlock()
	return m.saramaGroup
}

// setSaramaGroup sets the internal Sarama ConsumerGroup, protected by the mutex
func (m *managedGroupImpl) setSaramaGroup(group sarama.ConsumerGroup) {
	m.groupMutex.Lock()
	defer m.groupMutex.Unlock()
	m.saramaGroup = group
}

// resetLock will set the "lockedBy" field to the given lockToken (need not be the same as the
// existing one) and reset the timer to the provided timeout.  After the timer expires, the lockToken
// will be set to an empty string (representing "unlocked")
func (m *managedGroupImpl) resetLock(lockToken string, timeout time.Duration) {

	// Stop any existing timer (without releasing the lock) so that it won't inadvertently do an unlock later
	if m.cancelLockTimeout != nil {
		m.cancelLockTimeout()
	}

	if lockToken != "" && timeout != 0 {
		// Use the provided lockToken, which will prevent any caller with a different token from executing commands
		lockTimer := time.NewTimer(timeout)
		// Create a cancel function that will be used by further calls to resetLock, or to removeLock(),
		// for the purpose of stopping the lockTimer without clearing the token
		ctx, cancel := context.WithCancel(context.Background())
		m.cancelLockTimeout = cancel
		// Mark this group as "locked" by the provided token
		m.lockedBy.Store(lockToken)
		m.logger.Info("Managed group locked", zap.String("token", lockToken), zap.Duration("Timeout", timeout))

		// Reset the lockedBy field to an empty string when the lockTimer expires.  We create a new routine
		// each time (instead of calling lockTimer.Reset) because an existing timer may have expired long ago
		// and exited the goroutine.
		go func() {
			select {
			case <-lockTimer.C:
				m.logger.Debug("Managed Group lock timer expired")
				m.removeLock()
			case <-ctx.Done():
				m.logger.Debug("Managed Group lock timer canceled")
				if lockTimer.Stop() {
					// Drain the timer channel
					<-lockTimer.C
				}
			}
		}()

	} else {
		// If a lockToken and a timeout were not both provided, remove any existing lock
		// (locking a managed group for literally forever is not supported, although an
		// arbitrarily long time may be used).
		m.removeLock()
	}
}

// canUnlock returns true if the provided token is sufficient to unlock the group (that is,
// if it either matches the existing token, or if the existing token is empty)
func (m *managedGroupImpl) canUnlock(token string) bool {
	lockToken := m.lockedBy.Load()
	return lockToken == "" || lockToken == token
}

// removeLock sets the lockedBy token to an empty string, meaning "unlocked"
func (m *managedGroupImpl) removeLock() {
	if m.lockedBy.Load() != "" {
		m.logger.Debug("Managed Group lock removed")
		m.lockedBy.Store("")
		m.cancelLockTimeout() // Make sure an existing timer doesn't re-clear the token later
	}
}

// processLock handles setting and removing the managedGroup's lock status:
// - Lock the group, if "lock" is true and cmdLock.LockBefore is true
// - Unlock the group, if "lock" is false and cmdLock.UnlockAfter is true
// Returns an error if the lock.Token field doesn't match the existing token (for any case even if
// no CommandLock is provided), as this indicates that the group is locked by the other token.
func (m *managedGroupImpl) processLock(cmdLock *commands.CommandLock, lock bool) error {

	token := ""
	if cmdLock != nil {
		token = cmdLock.Token
	}

	if !m.canUnlock(token) {
		m.logger.Info("Managed group access denied; already locked with a different token",
			zap.String("Token", token))
		return GroupLockedError // Already locked by a different command token
	}

	if cmdLock == nil {
		return nil // If neither a lock nor unlock were requested, no need to go any further
	}

	if lock && cmdLock.LockBefore {
		timeout := cmdLock.Timeout
		if timeout == 0 {
			timeout = defaultLockTimeout
		}
		m.resetLock(cmdLock.Token, timeout)
	} else if !lock && cmdLock.UnlockAfter {
		m.removeLock()
	}

	return nil // Lock succeeded
}

// isStopped is an accessor for the restartWaitChannel channel's status, which if non-nil indicates
// that the KafkaConsumerGroupManager stopped ("paused") the managed ConsumerGroup (versus it having
// been closed outside the manager)
func (m *managedGroupImpl) isStopped() bool {
	return m.stopped.Load().(bool)
}

// createRestartChannel sets the state of the managed group to "stopped" by creating the restartWaitChannel
// channel that will be closed when the group is restarted.
func (m *managedGroupImpl) createRestartChannel() {
	// Don't re-create the channel if it already exists (the managed group is already stopped)
	if !m.isStopped() {
		m.restartWaitChannel = make(chan struct{})
		m.stopped.Store(true)
	}
}

// closeRestartChannel sets the state of the managed group to "started" by closing the restartWaitChannel channel.
func (m *managedGroupImpl) closeRestartChannel() {
	// If the managed group is already started, don't try to close the restart wait channel
	if m.isStopped() {
		m.stopped.Store(false)
		close(m.restartWaitChannel)
	}
}

// waitForStart will block until a stopped ("paused") ConsumerGroup has been restarted by the
// KafkaConsumerGroupManager, returning true in that case or false if the context's cancel function is called
func (m *managedGroupImpl) waitForStart(ctx context.Context) bool {
	if !m.isStopped() {
		return true // group is already started; don't try to read from closed channel
	}
	// Wait for either the restartWaitChannel to be closed, or for the provided context to have its cancel function called.
	select {
	case <-m.restartWaitChannel:
		return true
	case <-ctx.Done():
		return false
	}
}

// transferErrors starts a goroutine that reads errors from the provided errors channel and sends them to the m.errors
// channel.  Typically, this provided channel comes from the merged errors channel created in the KafkaConsumerGroupFactory
// This is done so that when the group.Errors() channel is closed during a stop ("pause") of the group, the m.errors
// channel can remain open (so that users of the manager do not receive a closed error channel during stop/start events).
func (m *managedGroupImpl) transferErrors(ctx context.Context, errors <-chan error) {
	go func() {
		for {
			m.logger.Info("Starting managed group error transfer")
			for groupErr := range errors {
				m.transferredErrors <- groupErr
			}
			if !m.isStopped() {
				// If the error channel was closed without the consumergroup being marked as stopped,
				// or if we were unable to wait for the group to be restarted, that is outside
				// the manager's responsibility, so we are finished transferring errors.
				m.logger.Warn("Consumer group's error channel was closed unexpectedly")
				close(m.transferredErrors)
				return
			}
			// Wait for the manager to restart the Consumer Group before calling m.group.Errors() again
			m.logger.Info("Error transfer is waiting for managed group restart")
			if !m.waitForStart(ctx) {
				// Abort if the context was canceled
				m.logger.Info("Wait for managed group restart was canceled")
				return
			}
		}
	}()
}
