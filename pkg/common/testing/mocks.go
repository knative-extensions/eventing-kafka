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
	"encoding"
	"fmt"

	"github.com/Shopify/sarama"
	ctrl "knative.dev/control-protocol/pkg"
)

//
// Mock Control-Protocol Service
//

// MockControlProtocolService is a stub-only mock of the Control Protocol Service
type MockControlProtocolService struct{}

var _ ctrl.Service = (*MockControlProtocolService)(nil)

func (MockControlProtocolService) SendAndWaitForAck(_ ctrl.OpCode, _ encoding.BinaryMarshaler) error {
	return nil
}
func (MockControlProtocolService) MessageHandler(_ ctrl.MessageHandler) {}
func (MockControlProtocolService) ErrorHandler(_ ctrl.ErrorHandler)     {}

//
// Mock ConsumerGroup
//

var _ sarama.ConsumerGroup = &MockConsumerGroup{}

type MockConsumerGroup struct {
	// Export the ErrorChan so that tests can send errors to it directly
	ErrorChan   chan error
	consumeChan chan struct{}
	Closed      bool
	CloseErr    bool
}

func NewMockConsumerGroup() *MockConsumerGroup {
	return &MockConsumerGroup{
		ErrorChan:   make(chan error),
		consumeChan: make(chan struct{}),
		Closed:      false,
	}
}

func (m *MockConsumerGroup) Consume(_ context.Context, _ []string, _ sarama.ConsumerGroupHandler) error {
	<-m.consumeChan                      // Block To Simulate Real Execution
	return sarama.ErrClosedConsumerGroup // Return ConsumerGroup Closed "Error" For Clean Shutdown
}

func (m *MockConsumerGroup) Errors() <-chan error {
	return m.ErrorChan
}

func (m *MockConsumerGroup) Close() error {
	close(m.ErrorChan)
	if m.consumeChan != nil {
		close(m.consumeChan)
	}
	m.Closed = true
	if m.CloseErr {
		return fmt.Errorf("error closing consumer group")
	}
	return nil
}
