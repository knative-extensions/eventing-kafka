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
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"
	"go.uber.org/zap"
)

// managedGroup contains information about a Sarama ConsumerGroup that is required to start (i.e. re-create) it
type managedGroup struct {
	logger             *zap.Logger
	group              sarama.ConsumerGroup // The Sarama ConsumerGroup which is under management
	errors             chan error           // An error channel that will replicate the errors from the Sarama ConsumerGroup
	restartWaitChannel chan struct{}        // A channel that will be closed when a stopped group is restarted
	stopped            atomic.Value         // Boolean value indicating that the managed group is stopped
	cancelErrors       func()               // Called by the manager's CloseConsumerGroup to terminate the error forwarding
	cancelConsume      func()               // Called by the manager during CloseConsumerGroup
	lockedBy           atomic.Value         // The LockToken of the ConsumerGroupAsyncCommand that requested the lock
	cancelLockTimeout  func()               // Called internally to stop the lock timeout when a lock is removed
}

// createManagedGroup associates a Sarama ConsumerGroup and cancel function (usually from the factory)
// inside a new managedGroup struct.  If a timeout is given (nonzero), the lockId will be reset to an
// empty string (i.e. "unlocked") after that time has passed.
func createManagedGroup(logger *zap.Logger, group sarama.ConsumerGroup, cancel func()) *managedGroup {

	managedGrp := managedGroup{
		logger:       logger,
		group:        group,
		errors:       make(chan error),
		cancelErrors: cancel,
	}

	// Atomic values must be initialized with their desired type before being accessed, or a nil
	// interface will be returned that is not of that type (string or bool in these cases)
	managedGrp.lockedBy.Store("")   // Empty token string indicates "unlocked"
	managedGrp.stopped.Store(false) // A managed group defaults to "started" when created

	return &managedGrp
}

// resetLockTimer will set the "lockedBy" field to the given lockToken (need not be the same as the
// existing one) and reset the timer to the provided timeout.  After the timer expires, the lockToken
// will be set to an empty string (representing "unlocked")
func (m *managedGroup) resetLockTimer(lockToken string, timeout time.Duration) {

	// Stop any existing timer (without releasing the lock) so that it won't inadvertently do an unlock later
	if m.cancelLockTimeout != nil {
		m.cancelLockTimeout()
	}

	if lockToken != "" && timeout != 0 {
		// Use the provided lockToken, which will prevent any caller with a different token from executing commands
		lockTimer := time.NewTimer(timeout)
		// Create a cancel function that will be used by further calls to resetLockTimer, or to removeLock(),
		// for the purpose of stopping the lockTimer without clearing the token
		ctx, cancel := context.WithCancel(context.Background())
		m.cancelLockTimeout = cancel
		// Mark this group as "locked" by the provided token
		m.lockedBy.Store(lockToken)
		m.logger.Info("Managed group locked", zap.String("token", lockToken))

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
func (m *managedGroup) canUnlock(token string) bool {
	lockToken := m.lockedBy.Load()
	return lockToken == "" || lockToken == token
}

// removeLock sets the lockedBy token to an empty string, meaning "unlocked"
func (m *managedGroup) removeLock() {
	if m.lockedBy.Load() != "" {
		m.logger.Debug("Managed Group lock removed")
		m.lockedBy.Store("")
		m.cancelLockTimeout() // Make sure an existing timer doesn't re-clear the token later
	}
}

// isStopped is an accessor for the restartWaitChannel channel's status, which if non-nil indicates
// that the KafkaConsumerGroupManager stopped ("paused") the managed ConsumerGroup (versus it having
// been closed outside the manager)
func (m *managedGroup) isStopped() bool {
	return m.stopped.Load().(bool)
}

// createRestartChannel sets the state of the managed group to "stopped" by creating the restartWaitChannel
// channel that will be closed when the group is restarted.
func (m *managedGroup) createRestartChannel() {
	// Don't re-create the channel if it already exists (the managed group is already stopped)
	if !m.isStopped() {
		m.restartWaitChannel = make(chan struct{})
		m.stopped.Store(true)
	}
}

// closeRestartChannel sets the state of the managed group to "started" by closing the restartWaitChannel channel.
func (m *managedGroup) closeRestartChannel() {
	// If the managed group is already started, don't try to close the restart wait channel
	if m.isStopped() {
		m.stopped.Store(false)
		close(m.restartWaitChannel)
	}
}

// waitForStart will block until a stopped ("paused") ConsumerGroup has been restarted by the
// KafkaConsumerGroupManager, returning true in that case or false if the context's cancel function is called
func (m *managedGroup) waitForStart(ctx context.Context) bool {
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

// transferErrors starts a goroutine that reads errors from the managedGroup's internal group.Errors() channel
// and sends them to the m.errors channel.  This is done so that when the group.Errors() channel is closed during
// a stop ("pause") of the group, the m.errors channel can remain open (so that users of the manager do not
// receive a closed error channel during stop/start events).
func (m *managedGroup) transferErrors(ctx context.Context) {
	go func() {
		for {
			m.logger.Debug("Starting managed group error transfer")
			for groupErr := range m.group.Errors() {
				m.errors <- groupErr
			}
			if !m.isStopped() {
				// If the error channel was closed without the consumergroup being marked as stopped,
				// or if we were unable to wait for the group to be restarted, that is outside
				// of the manager's responsibility, so we are finished transferring errors.
				close(m.errors)
				return
			}
			// Wait for the manager to restart the Consumer Group before calling m.group.Errors() again
			m.logger.Debug("Error transfer is waiting for managed group restart")
			if !m.waitForStart(ctx) {
				// Abort if the context was canceled
				return
			}
		}
	}()
}
