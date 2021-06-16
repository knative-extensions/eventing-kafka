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

func (c MockKafkaConsumerGroupFactory) StartConsumerGroup(_ string, _ []string, _ *zap.SugaredLogger, _ consumer.KafkaConsumerHandler, _ ...consumer.SaramaConsumerHandlerOption) (sarama.ConsumerGroup, error) {
	if c.CreateErr {
		return nil, errors.New("error creating consumer")
	}
	return commontesting.NewMockConsumerGroup(), nil
}

var _ consumer.KafkaConsumerGroupFactory = (*MockKafkaConsumerGroupFactory)(nil)

//
// Mock KafkaConsumerGroupManager
//

// MockConsumerGroupManager implements the KafkaConsumerGroupManager interface
type MockConsumerGroupManager struct {
	Groups map[string]sarama.ConsumerGroup
}

func NewMockConsumerGroupManager() *MockConsumerGroupManager {
	return &MockConsumerGroupManager{
		Groups: make(map[string]sarama.ConsumerGroup),
	}
}

var _ consumer.KafkaConsumerGroupManager = (*MockConsumerGroupManager)(nil)

func (m MockConsumerGroupManager) Reconfigure(_ []string, _ *sarama.Config) {}

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
