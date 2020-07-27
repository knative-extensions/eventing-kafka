package producer

import (
	"context"
	"github.com/Shopify/sarama"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	gometrics "github.com/rcrowley/go-metrics"
	"github.com/stretchr/testify/assert"
	"knative.dev/eventing-kafka/pkg/channel/constants"
	channelhealth "knative.dev/eventing-kafka/pkg/channel/health"
	"knative.dev/eventing-kafka/pkg/channel/test"

	"knative.dev/eventing-kafka/pkg/common/metrics"
	//>>>>>>> master
	logtesting "knative.dev/pkg/logging/testing"
	"testing"
)

// Test The NewProducer Constructor
func TestNewProducer(t *testing.T) {

	// Create A Mock Kafka SyncProducer
	mockSyncProducer := test.NewMockSyncProducer(test.TopicName)

	// Create A Test Producer
	producer := createTestProducer(t, mockSyncProducer)

	// Verify The Results
	assert.True(t, producer.healthServer.ProducerReady())
}

// Test The ProduceKafkaMessage() Functionality
func TestProduceKafkaMessage(t *testing.T) {

	// Create Test Data
	mockSyncProducer := test.NewMockSyncProducer(test.TopicName)
	producer := createTestProducer(t, mockSyncProducer)
	channelReference := test.CreateChannelReference(test.ChannelName, test.ChannelNamespace)
	bindingMessage := test.CreateBindingMessage(cloudevents.VersionV1)

	// Perform The Test & Verify Results
	err := producer.ProduceKafkaMessage(context.Background(), channelReference, bindingMessage)
	assert.Nil(t, err)

	// Verify Message Was Produced Correctly
	producerMessage := mockSyncProducer.GetMessage()
	assert.NotNil(t, producerMessage)
	assert.Equal(t, test.TopicName, producerMessage.Topic)
	value, err := producerMessage.Value.Encode()
	assert.Nil(t, err)
	assert.Equal(t, test.EventDataJson, value)
	key, err := producerMessage.Key.Encode()
	assert.Nil(t, err)
	assert.Equal(t, test.PartitionKey, string(key))
	test.ValidateProducerMessageHeader(t, producerMessage.Headers, constants.KafkaHeaderKeyContentType, test.EventDataContentType)
	test.ValidateProducerMessageHeader(t, producerMessage.Headers, constants.CeKafkaHeaderKeySpecVersion, cloudevents.VersionV1)
	test.ValidateProducerMessageHeader(t, producerMessage.Headers, constants.CeKafkaHeaderKeyType, test.EventType)
	test.ValidateProducerMessageHeader(t, producerMessage.Headers, constants.CeKafkaHeaderKeyId, test.EventId)
	test.ValidateProducerMessageHeader(t, producerMessage.Headers, constants.CeKafkaHeaderKeySource, test.EventSource)
	test.ValidateProducerMessageHeader(t, producerMessage.Headers, constants.CeKafkaHeaderKeySubject, test.EventSubject)
	test.ValidateProducerMessageHeader(t, producerMessage.Headers, constants.CeKafkaHeaderKeyDataSchema, test.EventDataSchema)
	test.ValidateProducerMessageHeader(t, producerMessage.Headers, constants.CeKafkaHeaderKeyPartitionKey, test.PartitionKey)
}

// Test The Producer's Close() Functionality
func TestClose(t *testing.T) {

	// Create A Mock Kafka SyncProducer
	mockSyncProducer := test.NewMockSyncProducer(test.TopicName)

	// Create A Test Producer
	producer := createTestProducer(t, mockSyncProducer)

	// Perform The Test
	producer.Close()

	// Verify The Results
	assert.False(t, producer.healthServer.ProducerReady())
	assert.True(t, mockSyncProducer.Closed())
}

// Create A Producer With Specified KafkaProducer For Testing
func createTestProducer(t *testing.T, kafkaSyncProducer sarama.SyncProducer) *Producer {

	// Stub The Kafka Producer Creation Wrapper With Test Version Returning Specified SyncProducer
	createSyncProducerWrapperPlaceholder := createSyncProducerWrapper
	createSyncProducerWrapper = func(clientId string, brokers []string, username string, password string) (sarama.SyncProducer, gometrics.Registry, error) {
		assert.Equal(t, test.ClientId, clientId)
		assert.Equal(t, []string{test.KafkaBrokers}, brokers)
		assert.Equal(t, test.KafkaUsername, username)
		assert.Equal(t, test.KafkaPassword, password)
		registry := gometrics.NewRegistry()
		return kafkaSyncProducer, registry, nil
	}
	defer func() { createSyncProducerWrapper = createSyncProducerWrapperPlaceholder }()

	// Create A Test Logger
	logger := logtesting.TestLogger(t).Desugar()

	// Create New Metrics Server & StatsReporter
	healthServer := channelhealth.NewChannelHealthServer("12345")
	statsReporter := metrics.NewStatsReporter(logger)

	// Create The Producer
	producer, err := NewProducer(logger, test.ClientId, []string{test.KafkaBrokers}, test.KafkaUsername, test.KafkaPassword, statsReporter, healthServer)
	assert.Nil(t, err)
	assert.Equal(t, kafkaSyncProducer, producer.kafkaProducer)
	assert.Equal(t, healthServer, producer.healthServer)
	assert.Equal(t, statsReporter, producer.statsReporter)

	// Return The Producer
	return producer
}
