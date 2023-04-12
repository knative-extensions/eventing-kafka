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

func (m *mockManagedGroup) isStopped() bool {
	return m.Called().Bool(0)
}
