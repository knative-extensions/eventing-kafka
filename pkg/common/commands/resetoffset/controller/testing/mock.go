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
	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/mock"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	listerscorev1 "k8s.io/client-go/listers/core/v1"
)

//
// Mock Sarama Client
//

var _ sarama.Client = (*MockClient)(nil)

type MockClient struct {
	mock.Mock
}

func (c *MockClient) Config() *sarama.Config {
	args := c.Called()
	return args.Get(0).(*sarama.Config)
}

func (c *MockClient) Controller() (*sarama.Broker, error) {
	args := c.Called()
	return args.Get(0).(*sarama.Broker), args.Error(1)
}

func (c *MockClient) RefreshController() (*sarama.Broker, error) {
	args := c.Called()
	return args.Get(0).(*sarama.Broker), args.Error(1)
}

func (c *MockClient) Brokers() []*sarama.Broker {
	args := c.Called()
	return args.Get(0).([]*sarama.Broker)
}

func (c *MockClient) Broker(brokerID int32) (*sarama.Broker, error) {
	args := c.Called(brokerID)
	return args.Get(0).(*sarama.Broker), args.Error(1)
}

func (c *MockClient) Topics() ([]string, error) {
	args := c.Called()
	return args.Get(0).([]string), args.Error(1)
}

func (c *MockClient) Partitions(topic string) ([]int32, error) {
	args := c.Called(topic)
	return args.Get(0).([]int32), args.Error(1)
}

func (c *MockClient) WritablePartitions(topic string) ([]int32, error) {
	args := c.Called(topic)
	return args.Get(0).([]int32), args.Error(1)
}

func (c *MockClient) Leader(topic string, partitionID int32) (*sarama.Broker, error) {
	args := c.Called(topic, partitionID)
	return args.Get(0).(*sarama.Broker), args.Error(1)
}

func (c *MockClient) Replicas(topic string, partitionID int32) ([]int32, error) {
	args := c.Called(topic, partitionID)
	return args.Get(0).([]int32), args.Error(1)
}

func (c *MockClient) InSyncReplicas(topic string, partitionID int32) ([]int32, error) {
	args := c.Called(topic, partitionID)
	return args.Get(0).([]int32), args.Error(1)
}

func (c *MockClient) OfflineReplicas(topic string, partitionID int32) ([]int32, error) {
	args := c.Called(topic, partitionID)
	return args.Get(0).([]int32), args.Error(1)
}

func (c *MockClient) RefreshBrokers(addrs []string) error {
	args := c.Called(addrs)
	return args.Error(0)
}

func (c *MockClient) RefreshMetadata(topics ...string) error {
	args := c.Called(topics)
	return args.Error(0)
}

func (c *MockClient) GetOffset(topic string, partitionID int32, time int64) (int64, error) {
	args := c.Called(topic, partitionID, time)
	return args.Get(0).(int64), args.Error(1)
}

func (c *MockClient) Coordinator(consumerGroup string) (*sarama.Broker, error) {
	args := c.Called(consumerGroup)
	return args.Get(0).(*sarama.Broker), args.Error(1)
}

func (c *MockClient) RefreshCoordinator(consumerGroup string) error {
	args := c.Called(consumerGroup)
	return args.Error(0)
}

func (c *MockClient) InitProducerID() (*sarama.InitProducerIDResponse, error) {
	args := c.Called()
	return args.Get(0).(*sarama.InitProducerIDResponse), args.Error(1)
}

func (c *MockClient) Close() error {
	args := c.Called()
	return args.Error(0)
}

func (c *MockClient) Closed() bool {
	args := c.Called()
	return args.Bool(0)
}

func (c *MockClient) TransactionCoordinator(transactionID string) (*sarama.Broker, error) {
	args := c.Called(transactionID)
	return args.Get(0).(*sarama.Broker), args.Error(1)
}

func (c *MockClient) RefreshTransactionCoordinator(transactionID string) error {
	args := c.Called(transactionID)
	return args.Error(0)
}

func (c *MockClient) LeastLoadedBroker() *sarama.Broker {
	args := c.Called()
	return args.Get(0).(*sarama.Broker)
}

type MockClientOption = func(*MockClient)

func NewMockClient(options ...MockClientOption) *MockClient {
	mockClient := &MockClient{}
	for _, option := range options {
		option(mockClient)
	}
	return mockClient
}

func WithClientMockPartitions(topic string, partitions []int32, err error) MockClientOption {
	return func(mockSaramaClient *MockClient) {
		mockSaramaClient.On("Partitions", topic).Return(partitions, err)
	}
}

func WithClientMockGetOffset(topic string, partition int32, offsetTime int64, offset int64, err error) MockClientOption {
	return func(mockSaramaClient *MockClient) {
		mockSaramaClient.On("GetOffset", topic, partition, offsetTime).Return(offset, err)
	}
}

func WithClientMockClosed(closed bool) MockClientOption {
	return func(mockSaramaClient *MockClient) {
		mockSaramaClient.On("Closed").Return(closed)
	}
}

func WithClientMockClose(err error) MockClientOption {
	return func(mockSaramaClient *MockClient) {
		mockSaramaClient.On("Close").Return(err)
	}
}

//
// Mock Sarama OffsetManager
//

var _ sarama.OffsetManager = (*MockOffsetManager)(nil)

type MockOffsetManager struct {
	mock.Mock
}

func (o *MockOffsetManager) ManagePartition(topic string, partition int32) (sarama.PartitionOffsetManager, error) {
	args := o.Called(topic, partition)
	arg0 := args.Get(0)
	var partitionOffsetManager sarama.PartitionOffsetManager
	if arg0 != nil {
		partitionOffsetManager = arg0.(sarama.PartitionOffsetManager)
	}
	return partitionOffsetManager, args.Error(1)
}

func (o *MockOffsetManager) Close() error {
	args := o.Called()
	return args.Error(0)
}

func (o *MockOffsetManager) Commit() {
	o.Called()
}

type MockOffsetManagerOption = func(*MockOffsetManager)

func NewMockOffsetManager(options ...MockOffsetManagerOption) *MockOffsetManager {
	mockOffsetManager := &MockOffsetManager{}
	for _, option := range options {
		option(mockOffsetManager)
	}
	return mockOffsetManager
}

func WithOffsetManagerMockManagePartition(topic string, partition int32, partitionOffsetManager sarama.PartitionOffsetManager, err error) MockOffsetManagerOption {
	return func(mockOffsetManager *MockOffsetManager) {
		mockOffsetManager.On("ManagePartition", topic, partition).Return(partitionOffsetManager, err)
	}
}

func WithOffsetManagerMockCommit() MockOffsetManagerOption {
	return func(mockOffsetManager *MockOffsetManager) {
		mockOffsetManager.On("Commit").Return()
	}
}

func WithOffsetManagerMockClose(err error) MockOffsetManagerOption {
	return func(mockOffsetManager *MockOffsetManager) {
		mockOffsetManager.On("Close").Return(err)
	}
}

//
// Mock Sarama PartitionOffsetManager
//

var _ sarama.PartitionOffsetManager = (*MockPartitionOffsetManager)(nil)

type MockPartitionOffsetManager struct {
	mock.Mock
}

func (p *MockPartitionOffsetManager) NextOffset() (int64, string) {
	args := p.Called()
	return args.Get(0).(int64), args.String(1)
}

func (p *MockPartitionOffsetManager) MarkOffset(offset int64, metadata string) {
	p.Called(offset, metadata)
}

func (p *MockPartitionOffsetManager) ResetOffset(offset int64, metadata string) {
	p.Called(offset, metadata)
}

func (p *MockPartitionOffsetManager) Errors() <-chan *sarama.ConsumerError {
	args := p.Called()
	return args.Get(0).(chan *sarama.ConsumerError)
}

func (p *MockPartitionOffsetManager) AsyncClose() {
	p.Called()
}

func (p *MockPartitionOffsetManager) Close() error {
	args := p.Called()
	return args.Error(0)
}

type MockPartitionOffsetManagerOption = func(*MockPartitionOffsetManager)

func NewMockPartitionOffsetManager(options ...MockPartitionOffsetManagerOption) *MockPartitionOffsetManager {
	mockPartitionOffsetManager := &MockPartitionOffsetManager{}
	for _, option := range options {
		option(mockPartitionOffsetManager)
	}
	return mockPartitionOffsetManager
}

func WithPartitionOffsetManagerMockNextOffset(offset int64, metadata string) MockPartitionOffsetManagerOption {
	return func(mockPartitionOffsetManager *MockPartitionOffsetManager) {
		mockPartitionOffsetManager.On("NextOffset").Return(offset, metadata)
	}
}

func WithPartitionOffsetManagerMockMarkOffset(offset int64, metadata string) MockPartitionOffsetManagerOption {
	return func(mockPartitionOffsetManager *MockPartitionOffsetManager) {
		mockPartitionOffsetManager.On("MarkOffset", offset, metadata).Return()
	}
}

func WithPartitionOffsetManagerMockResetOffset(offset int64, metadata string) MockPartitionOffsetManagerOption {
	return func(mockPartitionOffsetManager *MockPartitionOffsetManager) {
		mockPartitionOffsetManager.On("ResetOffset", offset, metadata).Return()
	}
}

func WithPartitionOffsetManagerMockErrors(errors ...*sarama.ConsumerError) MockPartitionOffsetManagerOption {
	return func(mockPartitionOffsetManager *MockPartitionOffsetManager) {
		errChan := make(chan *sarama.ConsumerError)
		if len(errors) > 0 {
			go func() {
				for _, err := range errors {
					errChan <- err
				}
				close(errChan)
			}()
		} else {
			close(errChan)
		}
		mockPartitionOffsetManager.On("Errors").Return(errChan)
	}
}

func WithPartitionOffsetManagerMockClose(err error) MockPartitionOffsetManagerOption {
	return func(mockPartitionOffsetManager *MockPartitionOffsetManager) {
		mockPartitionOffsetManager.On("Close").Return(err)
	}
}

func WithPartitionOffsetManagerMockAsyncClose() MockPartitionOffsetManagerOption {
	return func(mockPartitionOffsetManager *MockPartitionOffsetManager) {
		mockPartitionOffsetManager.On("AsyncClose").Return()
	}
}

//
// Mock K8S PodLister
//

var _ listerscorev1.PodLister = (*MockPodLister)(nil)

type MockPodLister struct {
	mock.Mock
}

func (l *MockPodLister) List(selector labels.Selector) (ret []*corev1.Pod, err error) {
	args := l.Called(selector)
	return args.Get(0).([]*corev1.Pod), args.Error(1)
}

func (l *MockPodLister) Pods(namespace string) listerscorev1.PodNamespaceLister {
	args := l.Called(namespace)
	return args.Get(0).(listerscorev1.PodNamespaceLister)
}

//
// Mock K8S PodNamespaceLister
//

var _ listerscorev1.PodNamespaceLister = (*MockPodNamespaceLister)(nil)

type MockPodNamespaceLister struct {
	mock.Mock
}

func (nl *MockPodNamespaceLister) List(selector labels.Selector) (ret []*corev1.Pod, err error) {
	args := nl.Called(selector)
	return args.Get(0).([]*corev1.Pod), args.Error(1)
}

func (nl *MockPodNamespaceLister) Get(name string) (*corev1.Pod, error) {
	args := nl.Called(name)
	return args.Get(0).(*corev1.Pod), args.Error(1)
}
