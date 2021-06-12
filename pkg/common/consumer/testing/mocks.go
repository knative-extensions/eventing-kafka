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
	"fmt"

	"github.com/Shopify/sarama"
	"go.uber.org/zap"
	"knative.dev/eventing-kafka/pkg/common/consumer"
	commontesting "knative.dev/eventing-kafka/pkg/common/testing"
)

//
// Mock KafkaConsumerGroupFactory
//

type MockKafkaConsumerGroupFactory struct {
	// CreateErr will return an error when creating a consumer
	CreateErr bool
}

func (c MockKafkaConsumerGroupFactory) StartConsumerGroup(manager consumer.KafkaConsumerGroupManager, groupID string, topics []string, logger *zap.SugaredLogger, handler consumer.KafkaConsumerHandler, options ...consumer.SaramaConsumerHandlerOption) (sarama.ConsumerGroup, error) {
	if c.CreateErr {
		return nil, errors.New("error creating consumer")
	}
	group := commontesting.NewMockConsumerGroup()
	manager.AddExistingGroup(groupID, group, topics, logger, handler, options...)
	return group, nil
}

var _ consumer.KafkaConsumerGroupFactory = (*MockKafkaConsumerGroupFactory)(nil)

//
// Mock KafkaConsumerGroupManager
//

// MockConsumerGroupManager implements the KafkaConsumerGroupManager interface
type MockConsumerGroupManager struct {
	Groups map[string]sarama.ConsumerGroup
}

func NewMockConsumerGroupManager() consumer.KafkaConsumerGroupManager {
	return &MockConsumerGroupManager{
		Groups: make(map[string]sarama.ConsumerGroup),
	}
}

var _ consumer.KafkaConsumerGroupManager = (*MockConsumerGroupManager)(nil)

func (m MockConsumerGroupManager) AddExistingGroup(groupId string, group sarama.ConsumerGroup,
	_ []string, _ *zap.SugaredLogger, _ consumer.KafkaConsumerHandler, _ ...consumer.SaramaConsumerHandlerOption) {
	m.Groups[groupId] = group
}

func (m MockConsumerGroupManager) CreateConsumerGroup(_ consumer.NewConsumerGroupFnType, _ []string, _ string, _ *sarama.Config) (sarama.ConsumerGroup, error) {
	return nil, nil
}

func (m MockConsumerGroupManager) StartConsumerGroup(_ string, _ []string,
	_ *zap.SugaredLogger, _ consumer.KafkaConsumerHandler, _ ...consumer.SaramaConsumerHandlerOption) error {
	return nil
}

func (m MockConsumerGroupManager) CloseConsumerGroup(groupId string) error {
	group, ok := m.Groups[groupId]
	if !ok {
		return fmt.Errorf("test error:  Group does not exist")
	}
	return group.Close()
}

func (m MockConsumerGroupManager) IsValid(groupId string) bool {
	_, ok := m.Groups[groupId]
	return ok
}

func (m MockConsumerGroupManager) Errors(_ string) <-chan error {
	return nil
}

func (m MockConsumerGroupManager) Consume(_ string, _ context.Context, _ []string, _ sarama.ConsumerGroupHandler) error {
	return nil
}
