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
	"errors"

	"github.com/Shopify/sarama"
	"go.uber.org/zap"

	"knative.dev/eventing-kafka/pkg/common/consumer"
)

//
// Mock KafkaConsumerGroupFactory
//

type MockKafkaConsumerGroupFactory struct {
	// CreateErr will return an error when creating a consumer
	CreateErr bool
}

func (c MockKafkaConsumerGroupFactory) StartConsumerGroup(groupID string, topics []string, logger *zap.SugaredLogger, handler consumer.KafkaConsumerHandler, options ...consumer.SaramaConsumerHandlerOption) (sarama.ConsumerGroup, error) {
	if c.CreateErr {
		return nil, errors.New("error creating consumer")
	}
	return NewMockConsumerGroup(), nil
}

var _ consumer.KafkaConsumerGroupFactory = (*MockKafkaConsumerGroupFactory)(nil)

//
// Mock ConsumerGroup
//

var _ sarama.ConsumerGroup = &MockConsumerGroup{}

type MockConsumerGroup struct {
	errorChan   chan error
	consumeChan chan struct{}
	Closed      bool
}

func NewMockConsumerGroup() *MockConsumerGroup {
	return &MockConsumerGroup{
		errorChan:   make(chan error),
		consumeChan: make(chan struct{}),
		Closed:      false,
	}
}

func (m *MockConsumerGroup) Consume(_ context.Context, _ []string, _ sarama.ConsumerGroupHandler) error {
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
