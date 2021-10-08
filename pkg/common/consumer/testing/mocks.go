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

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/mock"
	"k8s.io/apimachinery/pkg/types"
	"knative.dev/eventing-kafka/pkg/common/consumer"
)

//
// Mock KafkaConsumerGroupFactory
//

type MockKafkaConsumerGroupFactory struct {
	// CreateErr will return an error when creating a consumer
	mock.Mock
}

func (c *MockKafkaConsumerGroupFactory) StartConsumerGroup(ctx context.Context, groupId string, topics []string, handler consumer.KafkaConsumerHandler, ref types.NamespacedName, options ...consumer.SaramaConsumerHandlerOption) (sarama.ConsumerGroup, error) {
	args := c.Called(ctx, groupId, topics, handler, ref, options)
	return args.Get(0).(sarama.ConsumerGroup), args.Error(1)
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

func (m *MockConsumerGroupManager) Reconfigure(brokers []string, config *sarama.Config) *consumer.ReconfigureError {
	return m.Called(brokers, config).Get(0).(*consumer.ReconfigureError)
}

func (m *MockConsumerGroupManager) StartConsumerGroup(ctx context.Context, groupId string, topics []string,
	handler consumer.KafkaConsumerHandler, channelRef types.NamespacedName, options ...consumer.SaramaConsumerHandlerOption) error {
	return m.Called(ctx, groupId, topics, handler, channelRef, options).Error(0)
}

func (m *MockConsumerGroupManager) CloseConsumerGroup(groupId string) error {
	if group, ok := m.Groups[groupId]; ok {
		_ = group.Close()
		delete(m.Groups, groupId)
	}
	return m.Called(groupId).Error(0)
}

func (m *MockConsumerGroupManager) IsManaged(groupId string) bool {
	return m.Called(groupId).Bool(0)
}

func (m *MockConsumerGroupManager) IsStopped(groupId string) bool {
	return m.Called(groupId).Bool(0)
}

func (m *MockConsumerGroupManager) Errors(groupId string) <-chan error {
	return m.Called(groupId).Get(0).(<-chan error)
}

func (m *MockConsumerGroupManager) GetNotificationChannel() <-chan consumer.ManagerEvent {
	return m.Called().Get(0).(<-chan consumer.ManagerEvent)
}

func (m *MockConsumerGroupManager) ClearNotifications() {
	_ = m.Called()
}
