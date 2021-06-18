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

package testing

import (
	"context"
	"fmt"

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
	Closed      bool
	CloseErr    bool
}

func NewStubbedMockConsumerGroup() *MockConsumerGroup {
	mockGroup := &MockConsumerGroup{
		ErrorChan:   make(chan error),
		consumeChan: make(chan struct{}),
		Closed:      false,
	}

	// Stub out the mock functions since this mock returns "real" data
	mockGroup.On("Consume", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	mockGroup.On("Errors").Return(nil)
	mockGroup.On("Close").Return(nil)
	return mockGroup
}

func (m *MockConsumerGroup) Consume(ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error {
	_ = m.Called(ctx, topics, handler)
	<-m.consumeChan                      // Block To Simulate Real Execution
	return sarama.ErrClosedConsumerGroup // Return ConsumerGroup Closed "Error" For Clean Shutdown
}

func (m *MockConsumerGroup) Errors() <-chan error {
	_ = m.Called()
	return m.ErrorChan
}

func (m *MockConsumerGroup) Close() error {
	_ = m.Called()
	close(m.ErrorChan)
	close(m.consumeChan)
	m.Closed = true
	if m.CloseErr {
		return fmt.Errorf("error closing consumer group")
	}
	return nil
}
