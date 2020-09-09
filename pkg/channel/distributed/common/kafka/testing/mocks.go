package testing

import (
	"context"
	"github.com/Shopify/sarama"
	"testing"
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
