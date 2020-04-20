package admin

import (
	"context"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/kyma-incubator/knative-kafka/pkg/common/kafka/constants"
	"github.com/stretchr/testify/assert"
	"testing"
)

// Mock AdminClient Reference
var mockAdminClient AdminClientInterface

// Test The CreateAdminClient() Kafka Functionality
func TestCreateAdminClientKafka(t *testing.T) {

	// Test Data
	ctx := context.TODO()
	adminClientType := Kafka
	mockAdminClient = &MockAdminClient{}

	// Replace the NewKafkaAdminClientWrapper To Provide Mock AdminClient & Defer Reset
	NewKafkaAdminClientWrapperRef := NewKafkaAdminClientWrapper
	NewKafkaAdminClientWrapper = func(ctxArg context.Context, namespaceArg string) (AdminClientInterface, error) {
		assert.Equal(t, ctx, ctxArg)
		assert.Equal(t, constants.KnativeEventingNamespace, namespaceArg)
		assert.Equal(t, adminClientType, adminClientType)
		return mockAdminClient, nil
	}
	defer func() { NewKafkaAdminClientWrapper = NewKafkaAdminClientWrapperRef }()

	// Perform The Test
	adminClient, err := CreateAdminClient(ctx, adminClientType)

	// Verify The Results
	assert.Nil(t, err)
	assert.NotNil(t, adminClient)
	assert.Equal(t, mockAdminClient, adminClient)
}

// Test The CreateAdminClient() EventHub Functionality
func TestCreateAdminClientEventHub(t *testing.T) {

	// Test Data
	ctx := context.TODO()
	adminClientType := EventHub
	mockAdminClient = &MockAdminClient{}

	// Replace the NewEventHubAdminClientWrapper To Provide Mock AdminClient & Defer Reset
	NewEventHubAdminClientWrapperRef := NewEventHubAdminClientWrapper
	NewEventHubAdminClientWrapper = func(ctxArg context.Context, namespaceArg string) (AdminClientInterface, error) {
		assert.Equal(t, ctx, ctxArg)
		assert.Equal(t, constants.KnativeEventingNamespace, namespaceArg)
		assert.Equal(t, adminClientType, adminClientType)
		return mockAdminClient, nil
	}
	defer func() { NewEventHubAdminClientWrapper = NewEventHubAdminClientWrapperRef }()

	// Perform The Test
	adminClient, err := CreateAdminClient(ctx, adminClientType)

	// Verify The Results
	assert.Nil(t, err)
	assert.NotNil(t, adminClient)
	assert.Equal(t, mockAdminClient, adminClient)
}

//
// Mock Confluent AdminClient
//

var _ AdminClientInterface = &MockAdminClient{}

type MockAdminClient struct {
	kafkaSecret string
}

func (c MockAdminClient) GetKafkaSecretName(topicName string) string {
	return c.kafkaSecret
}

func (c MockAdminClient) CreateTopics(context.Context, []kafka.TopicSpecification, ...kafka.CreateTopicsAdminOption) ([]kafka.TopicResult, error) {
	return nil, nil
}

func (c MockAdminClient) DeleteTopics(context.Context, []string, ...kafka.DeleteTopicsAdminOption) ([]kafka.TopicResult, error) {
	return nil, nil
}

func (c MockAdminClient) Close() {
	return
}
