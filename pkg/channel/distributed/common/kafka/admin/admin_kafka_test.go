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

package admin

import (
	"context"
	"os"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"knative.dev/eventing-kafka/pkg/channel/distributed/common/config"
	"knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/constants"
	commontesting "knative.dev/eventing-kafka/pkg/channel/distributed/common/testing"
	commonconstants "knative.dev/eventing-kafka/pkg/common/constants"
	injectionclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/logging"
	logtesting "knative.dev/pkg/logging/testing"
	"knative.dev/pkg/system"

	"strconv"
	"testing"
)

// Test The NewKafkaAdminClient() Constructor - Success Path
func TestNewKafkaAdminClientSuccess(t *testing.T) {

	// Test Data
	clientId := "TestClientId"
	namespace := "TestNamespace"
	kafkaSecretName := "TestKafkaSecretName"
	kafkaSecretBrokers := "TestKafkaSecretBrokers"
	kafkaSecretUsername := "TestKafkaSecretUsername"
	kafkaSecretPassword := "TestKafkaSecretPassword"
	saramaSettings := `
Net:
  TLS:
    Enable: true
  SASL:
    Enable: true
    Mechanism: PLAIN
    Version: 1
Metadata:
  RefreshFrequency: 300000000000
`

	// Setup Environment
	assert.Nil(t, os.Setenv(system.NamespaceEnvKey, commonconstants.KnativeEventingNamespace))

	// Create Test Kafka Secret And ConfigMap
	kafkaSecret := createKafkaSecret(kafkaSecretName, namespace, kafkaSecretBrokers, kafkaSecretUsername, kafkaSecretPassword)
	kafkaConfig := createKafkaConfig(config.SettingsConfigMapName, system.Namespace(), saramaSettings)

	// Create A Context With Test Logger & K8S Client
	ctx := logging.WithLogger(context.TODO(), logtesting.TestLogger(t))
	ctx = context.WithValue(ctx, injectionclient.Key{}, fake.NewSimpleClientset(kafkaSecret, kafkaConfig))

	// Create A Mock Sarama ClusterAdmin To Test Against
	mockClusterAdmin := &MockClusterAdmin{}

	// Mock The Sarama ClusterAdmin Creation For Testing
	newClusterAdminWrapperPlaceholder := NewClusterAdminWrapper
	NewClusterAdminWrapper = func(brokers []string, config *sarama.Config) (sarama.ClusterAdmin, error) {
		assert.Len(t, brokers, 1)
		assert.Equal(t, kafkaSecretBrokers, brokers[0])
		assert.NotNil(t, config)
		assert.Equal(t, clientId, config.ClientID)
		assert.Equal(t, constants.ConfigKafkaVersionDefault, config.Version)
		assert.Equal(t, kafkaSecretUsername, config.Net.SASL.User)
		assert.Equal(t, kafkaSecretPassword, config.Net.SASL.Password)
		return mockClusterAdmin, nil
	}
	defer func() {
		NewClusterAdminWrapper = newClusterAdminWrapperPlaceholder
	}()

	// Perform The Test
	adminClient, err := NewKafkaAdminClient(ctx, commontesting.GetDefaultSaramaConfig(t), clientId, namespace)

	// Verify The Results
	assert.Nil(t, err)
	assert.NotNil(t, adminClient)
}

// Test The NewKafkaAdminClient() Constructor - No Kafka Secrets Path
func TestNewKafkaAdminClientNoSecrets(t *testing.T) {

	// Test Data
	clientId := "TestClientId"
	namespace := "TestNamespace"

	// Create A Context With Test Logger & K8S Client
	ctx := logging.WithLogger(context.TODO(), logtesting.TestLogger(t))
	ctx = context.WithValue(ctx, injectionclient.Key{}, fake.NewSimpleClientset())

	// Perform The Test
	adminClient, err := NewKafkaAdminClient(ctx, commontesting.GetDefaultSaramaConfig(t), clientId, namespace)

	// Verify The Results
	assert.Nil(t, err)
	assert.NotNil(t, adminClient)
}

// Test The Kafka AdminClient CreateTopic() Functionality
func TestKafkaAdminClientCreateTopic(t *testing.T) {

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

// Test The Kafka AdminClient CreateTopic() Without ClusterAdmin Functionality
func TestKafkaAdminClientCreateTopicInvalidAdminClient(t *testing.T) {

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

// Test The Kafka AdminClient DeleteTopic() Functionality
func TestKafkaAdminClientDeleteTopic(t *testing.T) {

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

// Test The Kafka AdminClient DeleteTopic() Without AdminClient Functionality
func TestKafkaAdminClientDeleteTopicInvalidAdminClient(t *testing.T) {

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

// Test The Kafka AdminClient Close() Functionality
func TestKafkaAdminClientClose(t *testing.T) {

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

// Test The Kafka AdminClient Close() Without AdminClient Functionality
func TestKafkaAdminClientCloseInvalidAdminClient(t *testing.T) {

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

// Test The Kafka AdminClient GetKafkaSecretName() Functionality
func TestKafkaAdminClientGetKafkaSecretName(t *testing.T) {

	// Test Data
	topicName := "TestTopicName"
	secretName := "TestSecretName"

	// Test Logger
	logger := logtesting.TestLogger(t).Desugar()

	// Create A New Kafka AdminClient To Test
	adminClient := &KafkaAdminClient{logger: logger, kafkaSecret: secretName}

	// Perform The Test
	actualSecretName := adminClient.GetKafkaSecretName(topicName)

	// Verify The Results
	assert.Equal(t, secretName, actualSecretName)
}

//
// Utilities
//

// Create K8S Kafka Secret With Specified Config
func createKafkaSecret(name string, namespace string, brokers string, username string, password string) *corev1.Secret {
	return &corev1.Secret{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Secret",
			APIVersion: corev1.SchemeGroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				constants.KafkaSecretLabel: "true",
			},
		},
		Data: map[string][]byte{
			constants.KafkaSecretKeyBrokers:  []byte(brokers),
			constants.KafkaSecretKeyUsername: []byte(username),
			constants.KafkaSecretKeyPassword: []byte(password),
		},
	}
}

// Create K8S Kafka ConfigMap With Specified Config
func createKafkaConfig(name string, namespace string, saramaConfig string) *corev1.ConfigMap {
	return commontesting.GetTestSaramaConfigMapNamespaced(name, namespace, saramaConfig, "")
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

func (m *MockClusterAdmin) Close() error {
	args := m.Called()
	return args.Error(0)
}
