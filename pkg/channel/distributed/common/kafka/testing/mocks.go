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
	"testing"

	"github.com/Shopify/sarama"
)

//
// Mock ConsumerGroup Implementation
//

// Verify The Mock ConsumerGroup Implements The Interface
var _ sarama.ConsumerGroup = &MockConsumerGroup{}

// Define The Mock ConsumerGroup
type MockConsumerGroup struct {
	t           *testing.T
	errorChan   chan error
	consumeChan chan struct{}
	Closed      bool
}

// Mock ConsumerGroup Constructor
func NewMockConsumerGroup(t *testing.T) *MockConsumerGroup {
	return &MockConsumerGroup{
		t:           t,
		errorChan:   make(chan error),
		consumeChan: make(chan struct{}),
		Closed:      false,
	}
}

func (m *MockConsumerGroup) Consume(ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error {
	<-m.consumeChan                      // Block To Simulate Real Execution
	return sarama.ErrClosedConsumerGroup // Return ConsumerGroup Closed "Error" For Clean Shutdown
}

func (m *MockConsumerGroup) Errors() <-chan error {
	return m.errorChan
}

func (m *MockConsumerGroup) Close() error {
	close(m.errorChan)
	close(m.consumeChan)
	m.Closed = true
	return nil
}
