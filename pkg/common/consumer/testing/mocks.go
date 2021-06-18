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
	"errors"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"

	"knative.dev/eventing-kafka/pkg/common/consumer"
	kafkatesting "knative.dev/eventing-kafka/pkg/common/kafka/testing"
)

//
// Mock KafkaConsumerGroupFactory
//

type MockKafkaConsumerGroupFactory struct {
	// CreateErr will return an error when creating a consumer
	mock.Mock
	CreateErr bool
}

func (c *MockKafkaConsumerGroupFactory) StartConsumerGroup(_ string, _ []string, _ *zap.SugaredLogger, _ consumer.KafkaConsumerHandler, _ ...consumer.SaramaConsumerHandlerOption) (sarama.ConsumerGroup, error) {
	if c.CreateErr {
		return nil, errors.New("error creating consumer")
	}
	return kafkatesting.NewStubbedMockConsumerGroup(), nil
}

var _ consumer.KafkaConsumerGroupFactory = (*MockKafkaConsumerGroupFactory)(nil)

//
// Mock KafkaConsumerGroupManager
//

// MockConsumerGroupManager implements the KafkaConsumerGroupManager interface
type MockConsumerGroupManager struct {
	mock.Mock
	Groups map[string]sarama.ConsumerGroup
}

func NewMockConsumerGroupManager() *MockConsumerGroupManager {
	return &MockConsumerGroupManager{Groups: make(map[string]sarama.ConsumerGroup)}
}

var _ consumer.KafkaConsumerGroupManager = (*MockConsumerGroupManager)(nil)

func (m *MockConsumerGroupManager) Reconfigure(brokers []string, config *sarama.Config) error {
	args := m.Called(brokers, config)
	return args.Error(0)
}

func (m *MockConsumerGroupManager) StartConsumerGroup(groupId string, topics []string, logger *zap.SugaredLogger,
	handler consumer.KafkaConsumerHandler, options ...consumer.SaramaConsumerHandlerOption) error {
	args := m.Called(groupId, topics, logger, handler, options)
	return args.Error(0)
}

func (m *MockConsumerGroupManager) CloseConsumerGroup(groupId string) error {
	args := m.Called(groupId)
	if group, ok := m.Groups[groupId]; ok {
		_ = group.Close()
	}
	return args.Error(0)
}

func (m *MockConsumerGroupManager) IsManaged(groupId string) bool {
	args := m.Called(groupId)
	return args.Bool(0)
}

func (m *MockConsumerGroupManager) Errors(groupId string) <-chan error {
	args := m.Called(groupId)
	return args.Get(0).(<-chan error)
}
