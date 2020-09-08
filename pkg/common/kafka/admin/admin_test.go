package admin

import (
	"context"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
	"knative.dev/eventing-kafka/pkg/common/constants"
	kafkasarama "knative.dev/eventing-kafka/pkg/common/kafka/sarama"
	commontesting "knative.dev/eventing-kafka/pkg/common/testing"
)

// Mock AdminClient Reference
var mockAdminClient AdminClientInterface

// Test The CreateAdminClient() Kafka Functionality
func TestCreateAdminClientKafka(t *testing.T) {

	// Test Data
	ctx := context.TODO()
	clientId := "TestClientId"
	adminClientType := Kafka
	mockAdminClient = &MockAdminClient{}

	// Replace the NewKafkaAdminClientWrapper To Provide Mock AdminClient & Defer Reset
	NewKafkaAdminClientWrapperRef := NewKafkaAdminClientWrapper
	NewKafkaAdminClientWrapper = func(ctxArg context.Context, saramaConfig *sarama.Config, clientIdArg string, namespaceArg string) (AdminClientInterface, error) {
		assert.Equal(t, ctx, ctxArg)
		assert.Equal(t, clientId, clientIdArg)
		assert.Equal(t, constants.KnativeEventingNamespace, namespaceArg)
		assert.Equal(t, adminClientType, adminClientType)
		return mockAdminClient, nil
	}
	defer func() { NewKafkaAdminClientWrapper = NewKafkaAdminClientWrapperRef }()

	// Perform The Test
	adminClient, err := CreateAdminClient(ctx, commontesting.GetDefaultSaramaConfig(t, kafkasarama.NewSaramaConfig()), clientId, adminClientType)

	// Verify The Results
	assert.Nil(t, err)
	assert.NotNil(t, adminClient)
	assert.Equal(t, mockAdminClient, adminClient)
}

// Test The CreateAdminClient() EventHub Functionality
func TestCreateAdminClientEventHub(t *testing.T) {

	// Test Data
	ctx := context.TODO()
	clientId := "TestClientId"
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
	adminClient, err := CreateAdminClient(ctx, commontesting.GetDefaultSaramaConfig(t, kafkasarama.NewSaramaConfig()), clientId, adminClientType)

	// Verify The Results
	assert.Nil(t, err)
	assert.NotNil(t, adminClient)
	assert.Equal(t, mockAdminClient, adminClient)
}

// Test The CreateAdminClient Custom Functionality
func TestCreateAdminClientCustom(t *testing.T) {

	// Test Data
	ctx := context.TODO()
	clientId := "TestClientId"
	adminClientType := Custom
	mockAdminClient = &MockAdminClient{}

	// Replace the NewPluginAdminClientWrapper To Provide Mock AdminClient & Defer Reset
	NewCustomAdminClientWrapperRef := NewCustomAdminClientWrapper
	NewCustomAdminClientWrapper = func(ctxArg context.Context, namespaceArg string) (AdminClientInterface, error) {
		assert.Equal(t, ctx, ctxArg)
		assert.Equal(t, constants.KnativeEventingNamespace, namespaceArg)
		assert.Equal(t, adminClientType, adminClientType)
		return mockAdminClient, nil
	}
	defer func() { NewCustomAdminClientWrapper = NewCustomAdminClientWrapperRef }()

	// Perform The Test
	adminClient, err := CreateAdminClient(ctx, commontesting.GetDefaultSaramaConfig(t, kafkasarama.NewSaramaConfig()), clientId, adminClientType)

	// Verify The Results
	assert.Nil(t, err)
	assert.NotNil(t, adminClient)
	assert.Equal(t, mockAdminClient, adminClient)
}

// Test The CreateAdminClient Custom Functionality
func TestCreateAdminClientUnknown(t *testing.T) {

	// Test Data
	ctx := context.TODO()
	clientId := "TestClientId"
	adminClientType := Unknown

	// Perform The Test
	adminClient, err := CreateAdminClient(ctx, commontesting.GetDefaultSaramaConfig(t, kafkasarama.NewSaramaConfig()), clientId, adminClientType)

	// Verify The Results
	assert.NotNil(t, err)
	assert.Nil(t, adminClient)

}

//
// Mock AdminClient
//

var _ AdminClientInterface = &MockAdminClient{}

type MockAdminClient struct {
	kafkaSecret string
}

func (c MockAdminClient) GetKafkaSecretName(string) string {
	return c.kafkaSecret
}

func (c MockAdminClient) CreateTopic(context.Context, string, *sarama.TopicDetail) *sarama.TopicError {
	return nil
}

func (c MockAdminClient) DeleteTopic(context.Context, string) *sarama.TopicError {
	return nil
}

func (c MockAdminClient) Close() error {
	return nil
}
