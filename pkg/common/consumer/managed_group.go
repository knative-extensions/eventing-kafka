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

	"github.com/Shopify/sarama"
)

// managedGroup contains information about a Sarama ConsumerGroup that is required to start (i.e. re-create) it
type managedGroup struct {
	group              sarama.ConsumerGroup
	errors             chan error
	restartWaitChannel chan struct{}
	stopped            atomic.Value // Boolean value indicating channel is stopped
	cancelErrors       func()
	cancelConsume      func()
}

// createManagedGroup associates a Sarama ConsumerGroup and cancel function (usually from the factory)
// inside a new managedGroup struct
func createManagedGroup(group sarama.ConsumerGroup, cancel func()) *managedGroup {
	managedGrp := managedGroup{
		group:        group,
		errors:       make(chan error),
		cancelErrors: cancel,
	}
	// Initialize the atomic boolean value to false, otherwise it will be a nil interface
	// (restartWaitChannel is only accessed when "stopped" is true)
	managedGrp.stopped.Store(false)
	return &managedGrp
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
			if !m.waitForStart(ctx) {
				// Abort if the context was canceled
				return
			}
		}
	}()
}
