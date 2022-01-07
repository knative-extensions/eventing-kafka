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
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	logtesting "knative.dev/pkg/logging/testing"

	"knative.dev/eventing-kafka/pkg/common/controlprotocol/commands"
	kafkatesting "knative.dev/eventing-kafka/pkg/common/kafka/testing"
)

const shortTimeout = 200 * time.Millisecond

func TestManagedGroup(t *testing.T) {
	for _, testCase := range []struct {
		name     string
		restart  bool
		cancel   bool
		expected bool
	}{
		{
			name:     "Wait for Started Group",
			expected: true,
		},
		{
			name:     "Wait for Restarted Group",
			restart:  true,
			expected: true,
		},
		{
			name:     "Cancel Wait",
			cancel:   true,
			expected: false,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			// Test stop/start of a managedGroup
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			mockGroup := kafkatesting.NewMockConsumerGroup()
			errorChannel := make(chan error)
			mockGroup.On("Errors").Return(errorChannel)
			group := createManagedGroup(ctx, logtesting.TestLogger(t).Desugar(), mockGroup, errorChannel, cancel, func() {}).(*managedGroupImpl)
			waitGroup := sync.WaitGroup{}
			assert.False(t, group.isStopped())

			if testCase.restart || testCase.cancel {
				group.createRestartChannel()
				assert.True(t, group.isStopped())
			}

			waitGroup.Add(1)
			go func() {
				assert.Equal(t, testCase.expected, group.waitForStart(ctx))
				waitGroup.Done()
			}()
			time.Sleep(5 * time.Millisecond) // Let the waitForStart function begin

			if testCase.restart {
				group.closeRestartChannel()
			} else if testCase.cancel {
				cancel()
			}
			waitGroup.Wait() // Let the waitForStart function finish
			close(errorChannel)
			time.Sleep(shortTimeout) // Let the transferErrors goroutine finish
		})
	}

}

func TestProcessLock(t *testing.T) {
	defer restoreNewConsumerGroup(newConsumerGroup) // must use if calling getManagerWithMockGroup in the test
	const newToken = "new-token"

	for _, testCase := range []struct {
		name            string
		lock            *commands.CommandLock
		existingToken   string
		expectLock      string
		expectUnlock    string
		expectErrBefore error
		expectErrAfter  error
		expectTimerStop bool
	}{
		{
			name: "Nil LockCommand",
		},
		{
			name:            "Different Lock Token",
			existingToken:   "existing-token",
			expectLock:      "existing-token",
			expectUnlock:    "existing-token",
			lock:            &commands.CommandLock{LockBefore: true, UnlockAfter: true, Token: newToken},
			expectErrBefore: GroupLockedError,
			expectErrAfter:  GroupLockedError,
		},
		{
			name:            "No Lock Provided, ManagedGroup Locked",
			existingToken:   "existing-token",
			expectLock:      "existing-token",
			expectUnlock:    "existing-token",
			expectErrBefore: GroupLockedError,
			expectErrAfter:  GroupLockedError,
		},
		{
			name:            "Empty Token, ManagedGroup Locked",
			existingToken:   "existing-token",
			expectLock:      "existing-token",
			expectUnlock:    "existing-token",
			lock:            &commands.CommandLock{LockBefore: true, UnlockAfter: true, Token: ""},
			expectErrBefore: GroupLockedError,
			expectErrAfter:  GroupLockedError,
		},
		{
			name:            "Different Lock Token, No LockBefore Or UnlockAfter Specified",
			existingToken:   "existing-token",
			expectLock:      "existing-token",
			expectUnlock:    "existing-token",
			lock:            &commands.CommandLock{Token: newToken},
			expectErrBefore: GroupLockedError,
			expectErrAfter:  GroupLockedError,
		},
		{
			name:          "Same Lock Token",
			existingToken: newToken,
			expectLock:    newToken,
			lock:          &commands.CommandLock{LockBefore: true, UnlockAfter: true, Token: newToken},
		},
		{
			name:       "Zero Timeout",
			lock:       &commands.CommandLock{LockBefore: true, UnlockAfter: true, Token: newToken},
			expectLock: newToken,
		},
		{
			name:            "Explicit Timeout",
			lock:            &commands.CommandLock{LockBefore: true, UnlockAfter: true, Token: newToken, Timeout: shortTimeout},
			expectLock:      newToken,
			expectTimerStop: true,
		},
		{
			name:         "LockBefore Only",
			lock:         &commands.CommandLock{LockBefore: true, Token: newToken},
			expectLock:   newToken,
			expectUnlock: newToken,
		},
		{
			name: "UnlockAfter Only",
			lock: &commands.CommandLock{UnlockAfter: true, Token: newToken},
		},
		{
			name: "No Lock Or Unlock",
			lock: &commands.CommandLock{Token: newToken},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			_, managedGrp := createMockAndManagedGroups(t)
			if testCase.existingToken != "" {
				managedGrp.lockedBy.Store(testCase.existingToken)
			}
			errBefore := managedGrp.processLock(testCase.lock, true)
			assert.Equal(t, testCase.expectLock, managedGrp.lockedBy.Load())
			if testCase.expectTimerStop {
				time.Sleep(2 * shortTimeout)
				assert.Equal(t, "", managedGrp.lockedBy.Load())
			}
			errAfter := managedGrp.processLock(testCase.lock, false)
			assert.Equal(t, testCase.expectUnlock, managedGrp.lockedBy.Load())
			assert.Equal(t, testCase.expectErrBefore, errBefore)
			assert.Equal(t, testCase.expectErrAfter, errAfter)

			close(managedGrp.errors())
			time.Sleep(shortTimeout) // Let the transferErrors goroutine finish
		})
	}
}

func TestResetLockTimer(t *testing.T) {
	for _, testCase := range []struct {
		name          string
		cancelTimer   func()
		existingToken string
		token         string
		timeout       time.Duration
		existingTimer bool
		expiredTimer  bool
		expected      string
	}{
		{
			name:          "Running Timer",
			timeout:       shortTimeout,
			token:         "123",
			expected:      "123",
			existingTimer: true,
		},
		{
			name:         "Expired Timer",
			timeout:      shortTimeout,
			token:        "123",
			expected:     "123",
			expiredTimer: true,
		},
		{
			name:        "Existing Cancel Function",
			timeout:     shortTimeout,
			token:       "123",
			expected:    "123",
			cancelTimer: func() {},
		},
		{
			name:          "Different LockToken",
			timeout:       shortTimeout,
			token:         "123",
			expected:      "123",
			existingToken: "456",
		},
		{
			name:    "Empty LockToken, With Duration",
			timeout: shortTimeout,
		},
		{
			name: "Empty LockToken, No Duration",
		},
		{
			name:     "Valid LockToken, With Duration",
			token:    "123",
			expected: "123",
			timeout:  shortTimeout,
		},
		{
			name:     "Valid LockToken, No Duration",
			token:    "123",
			expected: "",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			time.Sleep(time.Millisecond)
			_, managedGrp := createMockAndManagedGroups(t)
			if testCase.cancelTimer != nil {
				managedGrp.cancelLockTimeout = testCase.cancelTimer
			}
			if testCase.existingTimer {
				managedGrp.resetLock("existing-token", time.Second) // Long enough not to expire during the test
				assert.False(t, managedGrp.canUnlock(testCase.token))
				time.Sleep(time.Millisecond) // Let the timer loop start executing
			} else if testCase.expiredTimer {
				managedGrp.resetLock("existing-token", time.Microsecond) // Expire the timer immediately
				time.Sleep(shortTimeout / 2)
			} else {
				assert.True(t, managedGrp.canUnlock(testCase.token))
			}
			if testCase.existingToken != "" {
				managedGrp.lockedBy.Store(testCase.existingToken)
				assert.Equal(t, testCase.existingToken == testCase.token, managedGrp.canUnlock(testCase.token))
			}
			managedGrp.resetLock(testCase.token, testCase.timeout)
			if testCase.existingTimer || testCase.expiredTimer {
				time.Sleep(time.Millisecond) // Let the timer loop finish executing
			}
			assert.Equal(t, testCase.expected, managedGrp.lockedBy.Load())
			if testCase.timeout != 0 && testCase.token != "" {
				time.Sleep(2 * testCase.timeout)
				assert.Equal(t, "", managedGrp.lockedBy.Load())
			}
			close(managedGrp.errors())
			time.Sleep(shortTimeout) // Let the transferErrors goroutine finish
		})
	}
}

func TestManagedGroupConsume(t *testing.T) {

	for _, testCase := range []struct {
		name      string
		stopped   bool
		cancel    bool
		expectErr string
	}{
		{
			name:      "Started",
			expectErr: "kafka: tried to use a consumer group that was closed",
		},
		{
			name:      "Stopped",
			stopped:   true,
			expectErr: "kafka: tried to use a consumer group that was closed",
		},
		{
			name:      "Canceled",
			stopped:   true,
			cancel:    true,
			expectErr: "context was canceled waiting for group to start",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			mockGroup, mgdGroup := createMockAndManagedGroups(t)
			mockGroup.On("Consume", ctx, []string{"topic"}, nil).Return(sarama.ErrClosedConsumerGroup)
			mockGroup.On("Close").Return(nil)

			if testCase.stopped {
				mgdGroup.createRestartChannel()
			}
			waitGroup := sync.WaitGroup{}
			waitGroup.Add(1)
			go func() {
				err := mgdGroup.consume(ctx, []string{"topic"}, nil)
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
				if testCase.cancel {
					cancel()
				} else {
					mgdGroup.closeRestartChannel()
				}
				assert.Nil(t, mgdGroup.saramaGroup.Close()) // Stops the MockConsumerGroup's Consume() call
			}
			waitGroup.Wait() // Allows the goroutine with the consume call to finish
			close(mgdGroup.errors())
			time.Sleep(shortTimeout) // Let the transferErrors goroutine finish
		})
	}
}

func TestStopStart(t *testing.T) {

	for _, testCase := range []struct {
		name        string
		errStopping bool
		errStarting bool
		stopped     bool
	}{
		{
			name: "Initially Started",
		},
		{
			name:        "Initially Started, Error Stopping",
			errStopping: true,
		},
		{
			name:    "Initially Stopped",
			stopped: true,
		},
		{
			name:        "Initially Stopped, Error Starting",
			stopped:     true,
			errStarting: true,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			mockGroup, mgdGroup := createMockAndManagedGroups(t)
			errStopped := fmt.Errorf("error stopping")
			if !testCase.errStopping {
				errStopped = nil
			}
			mockGroup.On("Close").Return(errStopped)

			if testCase.stopped {
				// Simulate a stopped group
				mgdGroup.createRestartChannel()
			} else {
				mgdGroup.stopped.Store(testCase.stopped)
			}

			err := mgdGroup.start(func() (sarama.ConsumerGroup, error) {
				startErr := fmt.Errorf("error starting")
				if !testCase.errStarting {
					startErr = nil
				}
				return mockGroup, startErr
			})
			assert.Equal(t, testCase.errStarting, err != nil)

			// Verify that the group is not stopped (unless there was an error)
			assert.Equal(t, testCase.errStarting, mgdGroup.stopped.Load().(bool))

			err = mgdGroup.stop()

			assert.Equal(t, !testCase.errStopping || testCase.stopped, mgdGroup.stopped.Load().(bool))
			assert.Equal(t, testCase.errStopping, err != nil)

			mockGroup.AssertExpectations(t)
			close(mgdGroup.errors())
			time.Sleep(shortTimeout) // Let the transferErrors goroutine finish
		})
	}
}

func TestClose(t *testing.T) {

	for _, testCase := range []struct {
		name   string
		cancel bool
	}{
		{
			name:   "With Cancel Functions",
			cancel: true,
		},
		{
			name: "Without Cancel Functions",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			mockGroup, mgdGroup := createMockAndManagedGroups(t)
			mockGroup.On("Close").Return(nil)

			cancelConsumeCalled := false
			cancelErrorsCalled := false
			if testCase.cancel {
				mgdGroup.cancelConsume = func() { cancelConsumeCalled = true }
				mgdGroup.cancelErrors = func() { cancelErrorsCalled = true }
			} else {
				mgdGroup.cancelConsume = nil
				mgdGroup.cancelErrors = nil
			}

			err := mgdGroup.close()
			assert.Nil(t, err)
			assert.Equal(t, testCase.cancel, cancelConsumeCalled)
			assert.Equal(t, testCase.cancel, cancelErrorsCalled)
			close(mgdGroup.errors())
			time.Sleep(shortTimeout) // Let the transferErrors goroutine finish
		})
	}
}

func TestTransferErrors(t *testing.T) {
	for _, testCase := range []struct {
		name       string
		stopGroup  bool
		startGroup bool
		cancel     bool
	}{
		{
			name: "Close Channel Without Stop",
		},
		{
			name:      "Close Channel After Stop",
			stopGroup: true,
		},
		{
			name:      "Cancel Context",
			stopGroup: true,
			cancel:    true,
		},
		{
			name:       "Restart After Stop",
			stopGroup:  true,
			startGroup: true,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			time.Sleep(time.Millisecond)

			// errorChan simulates the merged error channel usually provided via customConsumerGroup.handlerErrorChannel
			errorChan := make(chan error)

			// mockGrp represents the internal Sarama ConsumerGroup
			mockGrp := kafkatesting.NewMockConsumerGroup()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			// Call the transferErrors function (via createManagedGroup) with the simulated customConsumerGroup error channel
			managedGrp := createManagedGroup(ctx, logtesting.TestLogger(t).Desugar(), mockGrp, errorChan, func() {}, func() {})
			managedGrpImpl := managedGrp.(*managedGroupImpl)

			// Send an error to the simulated customConsumerGroup error channel
			errorChan <- fmt.Errorf("test-error")

			// Verify that the error appears in the managed group's error channel
			err := <-managedGrp.errors()
			assert.NotNil(t, err)
			assert.Equal(t, "test-error", err.Error())

			if testCase.stopGroup {
				managedGrpImpl.createRestartChannel()
			}
			if testCase.cancel {
				cancel()
			}
			mockGrp.AssertExpectations(t)

			if testCase.startGroup {
				time.Sleep(shortTimeout) // Let the error handling loop move forward
				// Simulate the effects of startConsumerGroup (new ConsumerGroup, same managedConsumerGroup)
				mockGrp = kafkatesting.NewMockConsumerGroup()
				managedGrpImpl.saramaGroup = mockGrp
				managedGrpImpl.closeRestartChannel()

				time.Sleep(shortTimeout) // Let the waitForStart function finish

				// Verify that error transfer continues to work after the restart
				errorChan <- fmt.Errorf("test-error-2")
				err = <-managedGrp.errors()
				assert.NotNil(t, err)
				assert.Equal(t, "test-error-2", err.Error())
				mockGrp.AssertExpectations(t)
			}
			close(managedGrp.errors())
			time.Sleep(shortTimeout) // Let the transferErrors goroutine finish
		})
	}
}

//
// Mock managedGroup
//

// mockManagedGroup implements the managedGroup interface
type mockManagedGroup struct {
	mock.Mock
}

func (m *mockManagedGroup) consume(ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error {
	return m.Called(ctx, topics, handler).Error(0)
}

func (m *mockManagedGroup) start(createGroup createSaramaGroupFn) error {
	return m.Called(createGroup).Error(0)
}

func (m *mockManagedGroup) stop() error {
	return m.Called().Error(0)
}

func (m *mockManagedGroup) close() error {
	return m.Called().Error(0)
}

func (m *mockManagedGroup) errors() chan error {
	return m.Called().Get(0).(chan error)
}

func (m *mockManagedGroup) processLock(cmdLock *commands.CommandLock, lock bool) error {
	return m.Called(cmdLock, lock).Error(0)
}

func (m *mockManagedGroup) isStopped() bool {
	return m.Called().Bool(0)
}
