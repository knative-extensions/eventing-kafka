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

	"github.com/stretchr/testify/assert"
	logtesting "knative.dev/pkg/logging/testing"

	kafkatesting "knative.dev/eventing-kafka/pkg/common/kafka/testing"
)

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
			group := createManagedGroup(logtesting.TestLogger(t).Desugar(), nil, cancel)
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
		})
	}

}

func TestResetLockTimer(t *testing.T) {
	const shortTimeout = 50 * time.Millisecond

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
			_, managedGrp := createTestGroup(t)
			if testCase.cancelTimer != nil {
				managedGrp.cancelLockTimeout = testCase.cancelTimer
			}
			if testCase.existingTimer {
				managedGrp.resetLockTimer("existing-token", time.Second) // Long enough not to expire during the test
				assert.False(t, managedGrp.canUnlock(testCase.token))
				time.Sleep(time.Millisecond) // Let the timer loop start executing
			} else if testCase.expiredTimer {
				managedGrp.resetLockTimer("existing-token", time.Microsecond) // Expire the timer immediately
				time.Sleep(shortTimeout / 2)
			} else {
				assert.True(t, managedGrp.canUnlock(testCase.token))
			}
			if testCase.existingToken != "" {
				managedGrp.lockedBy.Store(testCase.existingToken)
				assert.Equal(t, testCase.existingToken == testCase.token, managedGrp.canUnlock(testCase.token))
			}
			managedGrp.resetLockTimer(testCase.token, testCase.timeout)
			if testCase.existingTimer || testCase.expiredTimer {
				time.Sleep(time.Millisecond) // Let the timer loop finish executing
			}
			assert.Equal(t, testCase.expected, managedGrp.lockedBy.Load())
			if testCase.timeout != 0 && testCase.token != "" {
				time.Sleep(2 * testCase.timeout)
				assert.Equal(t, "", managedGrp.lockedBy.Load())
			}
		})
	}
}

func TestTransferErrors(t *testing.T) {
	const shortTimeout = 50 * time.Millisecond

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
			mockGrp, managedGrp := createTestGroup(t)
			mockGrp.On("Errors").Return(mockGrp.ErrorChan)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			managedGrp.transferErrors(ctx)

			mockGrp.ErrorChan <- fmt.Errorf("test-error")
			err := <-managedGrp.errors
			assert.NotNil(t, err)
			assert.Equal(t, "test-error", err.Error())
			if testCase.stopGroup {
				managedGrp.createRestartChannel()
			}
			if testCase.cancel {
				cancel()
			}
			close(mockGrp.ErrorChan)
			mockGrp.AssertExpectations(t)

			time.Sleep(shortTimeout) // Let the error handling loop move forward
			if testCase.startGroup {
				// Simulate the effects of startConsumerGroup (new ConsumerGroup, same managedConsumerGroup)
				mockGrp = kafkatesting.NewMockConsumerGroup()
				mockGrp.On("Errors").Return(mockGrp.ErrorChan)
				managedGrp.group = mockGrp
				managedGrp.closeRestartChannel()

				time.Sleep(shortTimeout) // Let the waitForStart function finish
				// Verify that errors work again after restart
				mockGrp.ErrorChan <- fmt.Errorf("test-error-2")
				err = <-managedGrp.errors
				assert.NotNil(t, err)
				assert.Equal(t, "test-error-2", err.Error())
				close(mockGrp.ErrorChan)
				mockGrp.AssertExpectations(t)
			}
		})
	}
}
