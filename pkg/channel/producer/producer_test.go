package producer

import (
	"github.com/cloudevents/sdk-go/v1/cloudevents"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/assert"
	"knative.dev/eventing-kafka/pkg/channel/constants"
	channelhealth "knative.dev/eventing-kafka/pkg/channel/health"
	"knative.dev/eventing-kafka/pkg/channel/test"
	kafkaproducer "knative.dev/eventing-kafka/pkg/common/kafka/producer"
	"knative.dev/eventing-kafka/pkg/common/prometheus"
	logtesting "knative.dev/pkg/logging/testing"
	"testing"
)

// Test The NewProducer Constructor
func TestNewProducer(t *testing.T) {

	// Test Data
	brokers := "TestBrokers"
	username := "TestUsername"
	password := "TestPassword"
	mockProducer := test.NewMockProducer(test.TopicName)

	// Stub The Kafka Producer Creation Wrapper With Test Version Returning MockProducer
	createProducerFunctionWrapper = func(brokers string, username string, password string) (kafkaproducer.ProducerInterface, error) {
		return mockProducer, nil
	}

	// Create A Test Logger
	logger := logtesting.TestLogger(t).Desugar()

	// Perform The Test
	healthServer := channelhealth.NewChannelHealthServer("12345")
	producer, err := NewProducer(logger, brokers, username, password, prometheus.NewMetricsServer(logger, "8888", "/metrics"), healthServer)

	// Verify The Results
	assert.Nil(t, err)
	assert.Equal(t, mockProducer, producer.kafkaProducer)
	assert.Equal(t, true, healthServer.ProducerReady())

	// Close The Producer (Or Subsequent Tests Will Fail Because processProducerEvents() GO Routine Is Still Running)
	producer.Close()
}

// Test The ProduceKafkaMessage() Functionality
func TestProduceKafkaMessage(t *testing.T) {

	// Test Data
	event := test.CreateCloudEvent(cloudevents.CloudEventsVersionV1)
	channelReference := test.CreateChannelReference(test.ChannelName, test.ChannelNamespace)
	mockProducer := test.NewMockProducer(test.TopicName)

	// Create A Test Producer
	producer := createTestProducer(t, mockProducer)

	// Perform The Test & Verify Results
	err := producer.ProduceKafkaMessage(event, channelReference)
	assert.Nil(t, err)

	// Block On The MockProducer's Channel & Verify Event Was Produced
	kafkaMessage := <-mockProducer.ProduceChannel()
	assert.NotNil(t, kafkaMessage)
	assert.Equal(t, test.PartitionKey, string(kafkaMessage.Key))
	assert.Equal(t, test.EventDataJson, kafkaMessage.Value)
	assert.Equal(t, test.TopicName, *kafkaMessage.TopicPartition.Topic)
	assert.Equal(t, kafka.PartitionAny, kafkaMessage.TopicPartition.Partition)
	assert.Equal(t, kafka.Offset(0), kafkaMessage.TopicPartition.Offset)
	assert.Nil(t, kafkaMessage.TopicPartition.Error)
	test.ValidateKafkaMessageHeader(t, kafkaMessage.Headers, constants.CeKafkaHeaderKeySpecVersion, cloudevents.CloudEventsVersionV1)
	test.ValidateKafkaMessageHeader(t, kafkaMessage.Headers, constants.CeKafkaHeaderKeyType, test.EventType)
	test.ValidateKafkaMessageHeader(t, kafkaMessage.Headers, constants.CeKafkaHeaderKeyId, test.EventId)
	test.ValidateKafkaMessageHeader(t, kafkaMessage.Headers, constants.CeKafkaHeaderKeySource, test.EventSource)
	test.ValidateKafkaMessageHeader(t, kafkaMessage.Headers, constants.CeKafkaHeaderKeyDataContentType, test.EventDataContentType)
	test.ValidateKafkaMessageHeader(t, kafkaMessage.Headers, constants.CeKafkaHeaderKeySubject, test.EventSubject)
	test.ValidateKafkaMessageHeader(t, kafkaMessage.Headers, constants.CeKafkaHeaderKeyDataSchema, test.EventDataSchema)
	test.ValidateKafkaMessageHeader(t, kafkaMessage.Headers, constants.CeKafkaHeaderKeyPartitionKey, test.PartitionKey)
}

// Test The Producer's Close() Functionality
func TestClose(t *testing.T) {

	// Replace The Package Singleton With A Mock Producer
	mockProducer := test.NewMockProducer(test.TopicName)

	// Create A Test Producer
	producer := createTestProducer(t, mockProducer)

	// Block On The StopChannel & Close The StoppedChannel (Play the part of processProducerEvents())
	go func() {
		<-producer.stopChannel
		close(producer.stoppedChannel)
	}()

	// Perform The Test
	producer.Close()

	// Verify The Mock Producer Was Closed
	assert.True(t, mockProducer.Closed())
}

// Create A Test Producer For Testing
func createTestProducer(t *testing.T, kafkaProducer kafkaproducer.ProducerInterface) *Producer {

	// Create A Test Logger
	logger := logtesting.TestLogger(t).Desugar()

	// Create A Test Producer
	producer := &Producer{
		logger:         logger,
		kafkaProducer:  kafkaProducer,
		stopChannel:    make(chan struct{}),
		stoppedChannel: make(chan struct{}),
		metrics:        prometheus.NewMetricsServer(logger, "8888", "/metrics"),
	}

	// Return The Test Producer
	return producer
}
