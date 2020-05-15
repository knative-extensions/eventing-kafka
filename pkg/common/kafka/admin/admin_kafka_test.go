package admin

import (
	"context"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"knative.dev/eventing-kafka/pkg/common/kafka/constants"
	injectionclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/logging"
	logtesting "knative.dev/pkg/logging/testing"
	"strconv"
	"testing"
)

// Test The NewKafkaAdminClient() Constructor - Success Path
func TestNewKafkaAdminClientSuccess(t *testing.T) {

	// Test Data
	namespace := "TestNamespace"
	kafkaSecretName := "TestKafkaSecretName"
	kafkaSecretBrokers := "TestKafkaSecretBrokers"
	kafkaSecretUsername := "TestKafkaSecretUsername"
	kafkaSecretPassword := "TestKafkaSecretPassword"

	// Create Test Kafka Secret
	kafkaSecret := createKafkaSecret(kafkaSecretName, namespace, kafkaSecretBrokers, kafkaSecretUsername, kafkaSecretPassword)

	// Create A Context With Test Logger & K8S Client
	ctx := logging.WithLogger(context.TODO(), logtesting.TestLogger(t))
	ctx = context.WithValue(ctx, injectionclient.Key{}, fake.NewSimpleClientset(kafkaSecret))

	// Perform The Test
	adminClient, err := NewKafkaAdminClient(ctx, namespace)

	// Verify The Results
	assert.Nil(t, err)
	assert.NotNil(t, adminClient)
}

// Test The NewKafkaAdminClient() Constructor - No Kafka Secrets Path
func TestNewKafkaAdminClientNoSecrets(t *testing.T) {

	// Test Data
	namespace := "TestNamespace"

	// Create A Context With Test Logger & K8S Client
	ctx := logging.WithLogger(context.TODO(), logtesting.TestLogger(t))
	ctx = context.WithValue(ctx, injectionclient.Key{}, fake.NewSimpleClientset())

	// Perform The Test
	adminClient, err := NewKafkaAdminClient(ctx, namespace)

	// Verify The Results
	assert.NotNil(t, err)
	assert.Nil(t, adminClient)
}

// Test The Kafka AdminClient CreateTopics() Functionality
func TestKafkaAdminClientCreateTopics(t *testing.T) {

	// Test Data
	ctx := context.TODO()
	topicName := "TestTopicName"
	topicNumPartitions := 4
	topicRetentionMillis := int64(3 * constants.MillisPerDay)

	// Create The Kafka TopicSpecification For The Topic/EventHub To Be Created
	topicSpecifications := []kafka.TopicSpecification{{
		Topic:         topicName,
		NumPartitions: topicNumPartitions,
		Config:        map[string]string{constants.TopicSpecificationConfigRetentionMs: strconv.FormatInt(topicRetentionMillis, 10)},
	}}

	// Create The Kafka TopicResults To Return
	topicResults := []kafka.TopicResult{{Topic: topicName, Error: kafka.NewError(kafka.ErrNoError, topicName, false)}}

	// Create A Mock Confluent AdminClient To Test Against
	mockConfluentAdminClient := &MockConfluentAdminClient{}
	mockConfluentAdminClient.On("CreateTopics", ctx, topicSpecifications, []kafka.CreateTopicsAdminOption(nil)).Return(topicResults, nil)

	// Test Logger
	logger := logtesting.TestLogger(t).Desugar()

	// Create A New Kafka AdminClient To Test
	adminClient := &KafkaAdminClient{
		logger:      logger,
		adminClient: mockConfluentAdminClient,
	}

	// Perform The Test
	kafkaTopicResults, err := adminClient.CreateTopics(ctx, topicSpecifications)

	// Verify The Results
	assert.Nil(t, err)
	assert.NotNil(t, kafkaTopicResults)
	assert.Len(t, kafkaTopicResults, 1)
	assert.Equal(t, topicName, kafkaTopicResults[0].Topic)
	assert.Equal(t, kafka.ErrNoError, kafkaTopicResults[0].Error.Code())
	mockConfluentAdminClient.AssertExpectations(t)
}

// Test The Kafka AdminClient DeleteTopics() Functionality
func TestKafkaAdminClientDeleteTopics(t *testing.T) {

	// Test Data
	ctx := context.TODO()
	topicName := "TestTopicName"

	// Create The Kafka TopicResults To Return
	topicResults := []kafka.TopicResult{{Topic: topicName, Error: kafka.NewError(kafka.ErrNoError, topicName, false)}}

	// Create A Mock Confluent AdminClient To Test Against
	mockConfluentAdminClient := &MockConfluentAdminClient{}
	mockConfluentAdminClient.On("DeleteTopics", ctx, []string{topicName}, []kafka.DeleteTopicsAdminOption(nil)).Return(topicResults, nil)

	// Test Logger
	logger := logtesting.TestLogger(t).Desugar()

	// Create A New Kafka AdminClient To Test
	adminClient := &KafkaAdminClient{
		logger:      logger,
		adminClient: mockConfluentAdminClient,
	}

	// Perform The Test
	kafkaTopicResults, err := adminClient.DeleteTopics(ctx, []string{topicName})

	// Verify The Results
	assert.Nil(t, err)
	assert.NotNil(t, kafkaTopicResults)
	assert.Len(t, kafkaTopicResults, 1)
	assert.Equal(t, topicName, kafkaTopicResults[0].Topic)
	assert.Equal(t, kafka.ErrNoError, kafkaTopicResults[0].Error.Code())
	mockConfluentAdminClient.AssertExpectations(t)
}

// Test The Kafka AdminClient Close() Functionality
func TestKafkaAdminClientClose(t *testing.T) {

	// Create A Mock Confluent AdminClient To Test Against
	mockConfluentAdminClient := &MockConfluentAdminClient{}
	mockConfluentAdminClient.On("Close").Return()

	// Test Logger
	logger := logtesting.TestLogger(t).Desugar()

	// Create A New Kafka AdminClient To Test
	adminClient := &KafkaAdminClient{
		logger:      logger,
		adminClient: mockConfluentAdminClient,
	}

	// Perform The Test
	adminClient.Close()

	// Verify The Results
	mockConfluentAdminClient.AssertExpectations(t)
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

//
// Mock Confluent Kafka AdminClient
//

// Verify The Mock Confluent AdminClient Implements The Interface
var _ ConfluentAdminClientInterface = &MockConfluentAdminClient{}

// The Mock Confluent AdminClient
type MockConfluentAdminClient struct {
	mock.Mock
}

func (m MockConfluentAdminClient) CreateTopics(ctx context.Context, topicSpecifications []kafka.TopicSpecification, options ...kafka.CreateTopicsAdminOption) ([]kafka.TopicResult, error) {
	args := m.Called(ctx, topicSpecifications, options)
	return args.Get(0).([]kafka.TopicResult), args.Error(1)
}

func (m MockConfluentAdminClient) DeleteTopics(ctx context.Context, topics []string, options ...kafka.DeleteTopicsAdminOption) ([]kafka.TopicResult, error) {
	args := m.Called(ctx, topics, options)
	return args.Get(0).([]kafka.TopicResult), args.Error(1)
}

func (m MockConfluentAdminClient) Close() {
	m.Called()
}
