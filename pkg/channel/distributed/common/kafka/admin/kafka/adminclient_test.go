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

package kafka

import (
	"context"
	"strconv"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/constants"
	"knative.dev/pkg/logging"
	logtesting "knative.dev/pkg/logging/testing"
)

// Test The NewAdminClient() Functionality
func TestNewAdminClient(t *testing.T) {

	// Test Data
	brokers := []string{"TestBroker"}
	config := sarama.NewConfig()

	// Create A Context With Test Logger
	ctx := logging.WithLogger(context.TODO(), logtesting.TestLogger(t))

	// Create A Mock Sarama ClusterAdmin To Test Against
	mockClusterAdmin := &MockClusterAdmin{}

	// Mock The Sarama ClusterAdmin Creation For Testing
	newClusterAdminFnPlaceholder := NewClusterAdminFn
	NewClusterAdminFn = func(brokersArg []string, configArg *sarama.Config) (sarama.ClusterAdmin, error) {
		assert.Equal(t, brokers, brokersArg)
		assert.Equal(t, config, configArg)
		return mockClusterAdmin, nil
	}
	defer func() {
		NewClusterAdminFn = newClusterAdminFnPlaceholder
	}()

	// Perform The Test
	adminClient, err := NewAdminClient(ctx, brokers, config)

	// Verify The Results
	assert.Nil(t, err)
	assert.NotNil(t, adminClient)
}

// Test The CreateTopic() Functionality
func TestCreateTopic(t *testing.T) {

	// Test Data
	ctx := context.TODO()
	topicName := "TestTopicName"
	topicNumPartitions := int32(4)
	topicRetentionDays := int32(3)
	topicRetentionMillis := int64(topicRetentionDays * constants.MillisPerDay)
	topicRetentionMillisString := strconv.FormatInt(topicRetentionMillis, 10)

	// Create The Kafka TopicDetail For The Topic/EventHub To Be Created
	topicDetail := &sarama.TopicDetail{
		NumPartitions: topicNumPartitions,
		ConfigEntries: map[string]*string{constants.TopicDetailConfigRetentionMs: &topicRetentionMillisString},
	}

	// Create The Kafka TopicError To Return
	errMsg := "test CreateTopic() success"
	testTopicError := &sarama.TopicError{
		Err:    sarama.ErrNoError,
		ErrMsg: &errMsg,
	}

	// Create A Mock Sarama ClusterAdmin To Test Against
	mockClusterAdmin := &MockClusterAdmin{}
	mockClusterAdmin.On("CreateTopic", topicName, topicDetail).Return(testTopicError)

	// Test Logger
	logger := logtesting.TestLogger(t).Desugar()

	// Create A New Kafka AdminClient To Test
	adminClient := &KafkaAdminClient{
		logger:       logger,
		clusterAdmin: mockClusterAdmin,
	}

	// Perform The Test
	resultTopicError := adminClient.CreateTopic(ctx, topicName, topicDetail)

	// Verify The Results
	assert.NotNil(t, resultTopicError)
	assert.Equal(t, sarama.ErrNoError, resultTopicError.Err)
	assert.Equal(t, errMsg, *resultTopicError.ErrMsg)
	mockClusterAdmin.AssertExpectations(t)
}

// Test The CreateTopic() Without ClusterAdmin Functionality
func TestCreateTopicInvalidAdminClient(t *testing.T) {

	// Test Data
	ctx := context.TODO()
	topicName := "TestTopicName"
	topicNumPartitions := int32(4)
	topicRetentionDays := int32(3)
	topicRetentionMillis := int64(topicRetentionDays * constants.MillisPerDay)
	topicRetentionMillisString := strconv.FormatInt(topicRetentionMillis, 10)

	// Create The Kafka TopicDetail For The Topic/EventHub To Be Created
	topicDetail := &sarama.TopicDetail{
		NumPartitions: topicNumPartitions,
		ConfigEntries: map[string]*string{constants.TopicDetailConfigRetentionMs: &topicRetentionMillisString},
	}

	// The Expected Error Message
	errMsg := "unable to create topic due to invalid ClusterAdmin - check Kafka authorization secrets"

	// Test Logger
	logger := logtesting.TestLogger(t).Desugar()

	// Create A New Kafka AdminClient To Test (No ClusterAdmin)
	adminClient := &KafkaAdminClient{logger: logger}

	// Perform The Test
	resultTopicError := adminClient.CreateTopic(ctx, topicName, topicDetail)

	// Verify The Results
	assert.NotNil(t, resultTopicError)
	assert.Equal(t, sarama.ErrUnknown, resultTopicError.Err)
	assert.Equal(t, errMsg, *resultTopicError.ErrMsg)
}

// Test The DeleteTopic() Functionality
func TestDeleteTopic(t *testing.T) {

	// Test Data
	ctx := context.TODO()
	topicName := "TestTopicName"

	// Create The Kafka TopicError To Return
	errMsg := "test DeleteTopic() success"
	testTopicError := &sarama.TopicError{
		Err:    sarama.ErrNoError,
		ErrMsg: &errMsg,
	}

	// Create A Mock Sarama ClusterAdmin To Test Against
	mockClusterAdmin := &MockClusterAdmin{}
	mockClusterAdmin.On("DeleteTopic", topicName).Return(testTopicError)

	// Test Logger
	logger := logtesting.TestLogger(t).Desugar()

	// Create A New Kafka AdminClient To Test
	adminClient := &KafkaAdminClient{
		logger:       logger,
		clusterAdmin: mockClusterAdmin,
	}

	// Perform The Test
	resultTopicError := adminClient.DeleteTopic(ctx, topicName)

	// Verify The Results
	assert.NotNil(t, resultTopicError)
	assert.Equal(t, sarama.ErrNoError, resultTopicError.Err)
	assert.Equal(t, errMsg, *resultTopicError.ErrMsg)
	mockClusterAdmin.AssertExpectations(t)
}

// Test The DeleteTopic() Without AdminClient Functionality
func TestDeleteTopicInvalidAdminClient(t *testing.T) {

	// Test Data
	ctx := context.TODO()
	topicName := "TestTopicName"

	// The Expected Error Message
	errMsg := "unable to delete topic due to invalid ClusterAdmin - check Kafka authorization secrets"

	// Test Logger
	logger := logtesting.TestLogger(t).Desugar()

	// Create A New Kafka AdminClient To Test
	adminClient := &KafkaAdminClient{logger: logger}

	// Perform The Test
	resultTopicError := adminClient.DeleteTopic(ctx, topicName)

	// Verify The Results
	assert.NotNil(t, resultTopicError)
	assert.Equal(t, sarama.ErrUnknown, resultTopicError.Err)
	assert.Equal(t, errMsg, *resultTopicError.ErrMsg)
}

// Test The Close() Functionality
func TestClose(t *testing.T) {

	// Create A Mock Sarama ClusterAdmin To Test Against
	mockClusterAdmin := &MockClusterAdmin{}
	mockClusterAdmin.On("Close").Return(nil)

	// Test Logger
	logger := logtesting.TestLogger(t).Desugar()

	// Create A New Kafka AdminClient To Test
	adminClient := &KafkaAdminClient{
		logger:       logger,
		clusterAdmin: mockClusterAdmin,
	}

	// Perform The Test
	err := adminClient.Close()

	// Verify The Results
	assert.Nil(t, err)
	mockClusterAdmin.AssertExpectations(t)
}

// Test The Close() Without AdminClient Functionality
func TestCloseInvalidAdminClient(t *testing.T) {

	// Test Logger
	logger := logtesting.TestLogger(t).Desugar()

	// Create A New Kafka AdminClient To Test
	adminClient := &KafkaAdminClient{logger: logger}

	// Perform The Test
	err := adminClient.Close()
	assert.NotNil(t, err)

	// Verify The Results
	assert.Equal(t, "unable to close invalid ClusterAdmin - check Kafka authorization secrets", err.Error())
}

//
// Mock Sarama Kafka ClusterAdmin
//

// Verify The Mock Sarama ClusterAdmin Implements The Interface
var _ sarama.ClusterAdmin = &MockClusterAdmin{}

// The Mock Sarama ClusterAdmin
type MockClusterAdmin struct {
	mock.Mock
}

func (m *MockClusterAdmin) DescribeLogDirs(brokers []int32) (map[int32][]sarama.DescribeLogDirsResponseDirMetadata, error) {
	panic("implement me")
}

func (m *MockClusterAdmin) CreateTopic(topic string, detail *sarama.TopicDetail, validateOnly bool) error {
	args := m.Called(topic, detail)
	return args.Get(0).(*sarama.TopicError)
}

func (m *MockClusterAdmin) ListTopics() (map[string]sarama.TopicDetail, error) {
	panic("implement me")
}

func (m *MockClusterAdmin) DescribeTopics(topics []string) (metadata []*sarama.TopicMetadata, err error) {
	panic("implement me")
}

func (m *MockClusterAdmin) DeleteTopic(topic string) error {
	args := m.Called(topic)
	return args.Get(0).(*sarama.TopicError)
}

func (m *MockClusterAdmin) CreatePartitions(topic string, count int32, assignment [][]int32, validateOnly bool) error {
	panic("implement me")
}

func (m *MockClusterAdmin) AlterPartitionReassignments(topic string, assignment [][]int32) error {
	panic("implement me")
}

func (m *MockClusterAdmin) ListPartitionReassignments(topics string, partitions []int32) (topicStatus map[string]map[int32]*sarama.PartitionReplicaReassignmentsStatus, err error) {
	panic("implement me")
}

func (m *MockClusterAdmin) DeleteRecords(topic string, partitionOffsets map[int32]int64) error {
	panic("implement me")
}

func (m *MockClusterAdmin) DescribeConfig(resource sarama.ConfigResource) ([]sarama.ConfigEntry, error) {
	panic("implement me")
}

func (m *MockClusterAdmin) AlterConfig(resourceType sarama.ConfigResourceType, name string, entries map[string]*string, validateOnly bool) error {
	panic("implement me")
}

func (m *MockClusterAdmin) CreateACL(resource sarama.Resource, acl sarama.Acl) error {
	panic("implement me")
}

func (m *MockClusterAdmin) ListAcls(filter sarama.AclFilter) ([]sarama.ResourceAcls, error) {
	panic("implement me")
}

func (m *MockClusterAdmin) DeleteACL(filter sarama.AclFilter, validateOnly bool) ([]sarama.MatchingAcl, error) {
	panic("implement me")
}

func (m *MockClusterAdmin) ListConsumerGroups() (map[string]string, error) {
	panic("implement me")
}

func (m *MockClusterAdmin) DescribeConsumerGroups(groups []string) ([]*sarama.GroupDescription, error) {
	panic("implement me")
}

func (m *MockClusterAdmin) ListConsumerGroupOffsets(group string, topicPartitions map[string][]int32) (*sarama.OffsetFetchResponse, error) {
	panic("implement me")
}

func (m *MockClusterAdmin) DeleteConsumerGroup(group string) error {
	panic("implement me")
}

func (m *MockClusterAdmin) DescribeCluster() (brokers []*sarama.Broker, controllerID int32, err error) {
	panic("implement me")
}

func (m *MockClusterAdmin) DescribeUserScramCredentials(users []string) ([]*sarama.DescribeUserScramCredentialsResult, error) {
	panic("implement me")
}

func (m *MockClusterAdmin) DeleteUserScramCredentials(delete []sarama.AlterUserScramCredentialsDelete) ([]*sarama.AlterUserScramCredentialsResult, error) {
	panic("implement me")
}

func (m *MockClusterAdmin) UpsertUserScramCredentials(upsert []sarama.AlterUserScramCredentialsUpsert) ([]*sarama.AlterUserScramCredentialsResult, error) {
	panic("implement me")
}

func (m *MockClusterAdmin) Close() error {
	args := m.Called()
	return args.Error(0)
}
