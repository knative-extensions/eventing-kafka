/*
Copyright 2020 The Knative Authors

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

package testing

import (
	"context"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/mock"
)

//
// Mock Sarama ConsumerGroup Implementation
//

var _ sarama.ConsumerGroup = &MockConsumerGroup{}

type MockConsumerGroup struct {
	mock.Mock
	ErrorChan   chan error
	consumeChan chan struct{}
}

func NewMockConsumerGroup() *MockConsumerGroup {
	mockGroup := &MockConsumerGroup{
		ErrorChan:   make(chan error),
		consumeChan: make(chan struct{}),
	}
	return mockGroup
}

func (m *MockConsumerGroup) Consume(ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error {
	args := m.Called(ctx, topics, handler)
	<-m.consumeChan // Block To Simulate Real Execution
	return args.Error(0)
}

func (m *MockConsumerGroup) Errors() <-chan error {
	args := m.Called()
	return args.Get(0).(chan error)
}

func (m *MockConsumerGroup) Close() error {
	args := m.Called()
	close(m.ErrorChan)
	close(m.consumeChan)
	return args.Error(0)
}

func (m *MockConsumerGroup) Pause(partitions map[string][]int32) {
	m.Called(partitions)
}

func (m *MockConsumerGroup) Resume(partitions map[string][]int32) {
	m.Called(partitions)
}

func (m *MockConsumerGroup) PauseAll() {
	m.Called()
}

func (m *MockConsumerGroup) ResumeAll() {
	m.Called()
}
