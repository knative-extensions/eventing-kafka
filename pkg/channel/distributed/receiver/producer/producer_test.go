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

package producer

import (
	"context"
	"testing"

	"github.com/Shopify/sarama"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	producertesting "knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/producer/testing"
	"knative.dev/eventing-kafka/pkg/channel/distributed/receiver/constants"
	channelhealth "knative.dev/eventing-kafka/pkg/channel/distributed/receiver/health"
	receivertesting "knative.dev/eventing-kafka/pkg/channel/distributed/receiver/testing"
	commonclient "knative.dev/eventing-kafka/pkg/common/client"
	clienttesting "knative.dev/eventing-kafka/pkg/common/client/testing"
	configtesting "knative.dev/eventing-kafka/pkg/common/config/testing"
	"knative.dev/eventing-kafka/pkg/common/metrics"
	commontesting "knative.dev/eventing-kafka/pkg/common/testing"
	"knative.dev/pkg/logging"
	logtesting "knative.dev/pkg/logging/testing"
)

// Test The NewProducer Constructor
func TestNewProducer(t *testing.T) {

	// Test Data
	brokers := []string{configtesting.DefaultKafkaBroker}
	config := sarama.NewConfig()

	// Create A Mock Kafka SyncProducer
	mockSyncProducer := producertesting.NewMockSyncProducer()

	// Stub NewSyncProducerWrapper() For Testing And Restore After Test
	producertesting.StubNewSyncProducerFn(producertesting.ValidatingNewSyncProducerFn(t, brokers, config, mockSyncProducer))
	defer producertesting.RestoreNewSyncProducerFn()

	// Create And Validate A Test Producer
	createTestProducer(t, brokers, config, mockSyncProducer)

	// Verify The Mock SyncProducer State
	assert.False(t, mockSyncProducer.Closed())
}

// Test The ProduceKafkaMessage() Functionality For Event With PartitionKey
func TestProduceKafkaMessage(t *testing.T) {

	// Test Data
	brokers := []string{configtesting.DefaultKafkaBroker}
	config := sarama.NewConfig()
	channelReference := receivertesting.CreateChannelReference(receivertesting.ChannelName, receivertesting.ChannelNamespace)
	bindingMessage := receivertesting.CreateBindingMessage(cloudevents.VersionV1)

	// Create A Mock Kafka SyncProducer
	mockSyncProducer := producertesting.NewMockSyncProducer()

	// Stub NewSyncProducerWrapper() For Testing And Restore After Test
	producertesting.StubNewSyncProducerFn(producertesting.ValidatingNewSyncProducerFn(t, brokers, config, mockSyncProducer))
	defer producertesting.RestoreNewSyncProducerFn()

	// Create Producer To Test
	producer := createTestProducer(t, brokers, config, mockSyncProducer)

	// Perform The Test & Verify Results
	err := producer.ProduceKafkaMessage(context.Background(), channelReference, bindingMessage)
	assert.Nil(t, err)

	// Verify Message Was Produced Correctly
	producerMessage := mockSyncProducer.GetMessage()
	assert.NotNil(t, producerMessage)
	assert.Equal(t, receivertesting.TopicName, producerMessage.Topic)
	value, err := producerMessage.Value.Encode()
	assert.Nil(t, err)
	assert.Equal(t, receivertesting.EventDataJson, value)
	key, err := producerMessage.Key.Encode()
	assert.Nil(t, err)
	assert.Equal(t, receivertesting.PartitionKey, string(key))
	receivertesting.ValidateProducerMessageHeader(t, producerMessage.Headers, constants.KafkaHeaderKeyContentType, receivertesting.EventDataContentType)
	receivertesting.ValidateProducerMessageHeader(t, producerMessage.Headers, constants.CeKafkaHeaderKeySpecVersion, cloudevents.VersionV1)
	receivertesting.ValidateProducerMessageHeader(t, producerMessage.Headers, constants.CeKafkaHeaderKeyType, receivertesting.EventType)
	receivertesting.ValidateProducerMessageHeader(t, producerMessage.Headers, constants.CeKafkaHeaderKeyId, receivertesting.EventId)
	receivertesting.ValidateProducerMessageHeader(t, producerMessage.Headers, constants.CeKafkaHeaderKeySource, receivertesting.EventSource)
	receivertesting.ValidateProducerMessageHeader(t, producerMessage.Headers, constants.CeKafkaHeaderKeySubject, receivertesting.EventSubject)
	receivertesting.ValidateProducerMessageHeader(t, producerMessage.Headers, constants.CeKafkaHeaderKeyDataSchema, receivertesting.EventDataSchema)
	receivertesting.ValidateProducerMessageHeader(t, producerMessage.Headers, constants.CeKafkaHeaderKeyPartitionKey, receivertesting.PartitionKey)
}

// Test The Producer's ConfigChanged Functionality
func TestConfigChanged(t *testing.T) {

	logger := logtesting.TestLogger(t)
	ctx := logging.WithLogger(context.TODO(), logger)

	// Setup Test Environment Namespaces
	commontesting.SetTestEnvironment(t)

	// Test Data
	brokers := []string{configtesting.DefaultKafkaBroker}
	baseSaramaConfig, err := commonclient.NewConfigBuilder().WithDefaults().FromYaml(clienttesting.DefaultSaramaConfigYaml).Build(ctx)
	assert.Nil(t, err)

	// Define The TestCase Struct
	type TestCase struct {
		only              bool
		name              string
		newConfigMap      *corev1.ConfigMap
		expectNewProducer bool
	}

	// Create The TestCases
	testCases := []TestCase{
		{
			name:              "No Changes (Same Producer)",
			newConfigMap:      configtesting.NewKafkaConfigMap(),
			expectNewProducer: false,
		},
		{
			name:              "No EventingKafka Config (Same Producer)",
			newConfigMap:      configtesting.NewKafkaConfigMap(configtesting.WithoutEventingKafkaConfiguration),
			expectNewProducer: false,
		},
		{
			name:              "Admin Change (Same Producer)",
			newConfigMap:      configtesting.NewKafkaConfigMap(configtesting.WithModifiedSaramaAdmin),
			expectNewProducer: false,
		},
		{
			name:              "Net Change (New Producer)",
			newConfigMap:      configtesting.NewKafkaConfigMap(configtesting.WithModifiedSaramaNet),
			expectNewProducer: true,
		},
		{
			name:              "Metadata Change (New Producer)",
			newConfigMap:      configtesting.NewKafkaConfigMap(configtesting.WithModifiedSaramaMetadata),
			expectNewProducer: true,
		},
		{
			name:              "Consumer Change (Same Producer)",
			newConfigMap:      configtesting.NewKafkaConfigMap(configtesting.WithModifiedSaramaConsumer),
			expectNewProducer: false,
		},
		{
			name:              "Producer Change (New Producer)",
			newConfigMap:      configtesting.NewKafkaConfigMap(configtesting.WithModifiedSaramaProducer),
			expectNewProducer: true,
		},
	}

	// Filter To Those With "only" Flag (If Any Specified)
	filteredTestCases := make([]TestCase, 0)
	for _, testCase := range testCases {
		if testCase.only {
			filteredTestCases = append(filteredTestCases, testCase)
		}
	}
	if len(filteredTestCases) == 0 {
		filteredTestCases = testCases
	}

	// Make Sure To Restore The NewSyncProducer Wrapper After The Test
	defer producertesting.RestoreNewSyncProducerFn()

	// Run The Filtered TestCases
	for _, testCase := range filteredTestCases {
		t.Run(testCase.name, func(t *testing.T) {

			// Mock The SyncProducer & Stub The NewSyncProducerWrapper()
			mockSyncProducer := producertesting.NewMockSyncProducer()
			producertesting.StubNewSyncProducerFn(producertesting.NonValidatingNewSyncProducerFn(mockSyncProducer))

			// Create A Test Producer To Perform Tests Against
			producer := createTestProducer(t, brokers, baseSaramaConfig, mockSyncProducer)

			// Perform The Test
			newProducer := producer.ConfigChanged(ctx, testCase.newConfigMap)

			// Verify Expected State
			assert.Equal(t, testCase.expectNewProducer, newProducer != nil)
			assert.Equal(t, testCase.expectNewProducer, mockSyncProducer.Closed())
			if newProducer != nil {
				assert.Equal(t, producer.brokers, newProducer.brokers)
				assert.Equal(t, producer.statsReporter, newProducer.statsReporter)
				assert.Equal(t, producer.healthServer, newProducer.healthServer)
				assert.NotEqual(t, producer.configuration, newProducer.configuration)
			}
		})
	}
}

// Test The Producer's ConfigChanged Functionality
func TestSecretChanged(t *testing.T) {

	logger := logtesting.TestLogger(t)
	ctx := logging.WithLogger(context.TODO(), logger)

	// Setup Test Environment Namespaces
	commontesting.SetTestEnvironment(t)

	// Test Data
	brokers := []string{configtesting.DefaultKafkaBroker}
	auth := &commonclient.KafkaAuthConfig{
		SASL: &commonclient.KafkaSaslConfig{
			User:     configtesting.DefaultSecretUsername,
			Password: configtesting.DefaultSecretPassword,
			SaslType: configtesting.DefaultSecretSaslType,
		},
	}
	baseSaramaConfig, err := commonclient.NewConfigBuilder().WithDefaults().FromYaml(clienttesting.DefaultSaramaConfigYaml).WithAuth(auth).Build(ctx)
	assert.Nil(t, err)

	// Define The TestCase Struct
	type TestCase struct {
		only              bool
		name              string
		newSecret         *corev1.Secret
		expectNewProducer bool
	}

	// Create The TestCases
	testCases := []TestCase{
		{
			name:              "No Changes (Same Producer)",
			newSecret:         configtesting.NewKafkaSecret(),
			expectNewProducer: false,
		},
		{
			name:              "Password Change (New Producer)",
			newSecret:         configtesting.NewKafkaSecret(configtesting.WithModifiedPassword),
			expectNewProducer: true,
		},
		{
			name:              "Username Change (New Producer)",
			newSecret:         configtesting.NewKafkaSecret(configtesting.WithModifiedUsername),
			expectNewProducer: true,
		},
		{
			name:              "Empty Username Change (New Producer)",
			newSecret:         configtesting.NewKafkaSecret(configtesting.WithEmptyUsername),
			expectNewProducer: true,
		},
		{
			name:              "SaslType Change (New Producer)",
			newSecret:         configtesting.NewKafkaSecret(configtesting.WithModifiedSaslType),
			expectNewProducer: true,
		},
		{
			name:              "Namespace Change (Same Producer)",
			newSecret:         configtesting.NewKafkaSecret(configtesting.WithModifiedNamespace),
			expectNewProducer: false,
		},
		{
			name:              "No Auth Config In Secret (Same Producer)",
			newSecret:         configtesting.NewKafkaSecret(configtesting.WithMissingConfig),
			expectNewProducer: false,
		},
	}

	// Filter To Those With "only" Flag (If Any Specified)
	filteredTestCases := make([]TestCase, 0)
	for _, testCase := range testCases {
		if testCase.only {
			filteredTestCases = append(filteredTestCases, testCase)
		}
	}
	if len(filteredTestCases) == 0 {
		filteredTestCases = testCases
	}

	// Make Sure To Restore The NewSyncProducer Wrapper After The Test
	defer producertesting.RestoreNewSyncProducerFn()

	// Run The Filtered TestCases
	for _, testCase := range filteredTestCases {
		t.Run(testCase.name, func(t *testing.T) {

			// Mock The SyncProducer & Stub The NewSyncProducerWrapper()
			mockSyncProducer := producertesting.NewMockSyncProducer()
			producertesting.StubNewSyncProducerFn(producertesting.NonValidatingNewSyncProducerFn(mockSyncProducer))

			// Create A Test Producer To Perform Tests Against
			producer := createTestProducer(t, brokers, baseSaramaConfig, mockSyncProducer)

			// Perform The Test
			newProducer := producer.SecretChanged(ctx, testCase.newSecret)

			// Verify Expected State
			assert.Equal(t, testCase.expectNewProducer, newProducer != nil)
			assert.Equal(t, testCase.expectNewProducer, mockSyncProducer.Closed())
			if newProducer != nil {
				assert.Equal(t, producer.brokers, newProducer.brokers)
				assert.Equal(t, producer.statsReporter, newProducer.statsReporter)
				assert.Equal(t, producer.healthServer, newProducer.healthServer)
			}
		})
	}
}

// Test The Producer's Close() Functionality
func TestClose(t *testing.T) {

	// Test Data
	brokers := []string{configtesting.DefaultKafkaBroker}
	config := sarama.NewConfig()

	// Create A Mock Kafka SyncProducer
	mockSyncProducer := producertesting.NewMockSyncProducer()

	// Stub NewSyncProducerWrapper() For Testing And Restore After Test
	producertesting.StubNewSyncProducerFn(producertesting.ValidatingNewSyncProducerFn(t, brokers, config, mockSyncProducer))
	defer producertesting.RestoreNewSyncProducerFn()

	// Create A Test Producer
	producer := createTestProducer(t, brokers, config, mockSyncProducer)

	// Perform The Test
	producer.Close()

	// Verify The Results
	assert.False(t, producer.healthServer.ProducerReady())
	assert.True(t, mockSyncProducer.Closed())
}

// Utility Function For Creating A Producer With Specified Configuration
func createTestProducer(t *testing.T, brokers []string, config *sarama.Config, syncProducer sarama.SyncProducer) *Producer {

	// Create A Test Logger
	logger := logtesting.TestLogger(t).Desugar()

	// Create New Metrics Server & StatsReporter
	healthServer := channelhealth.NewChannelHealthServer("12345")
	statsReporter := metrics.NewStatsReporter(logger)

	// Create The Producer
	producer, err := NewProducer(logger, config, brokers, statsReporter, healthServer)

	// Verify Expected State
	assert.Nil(t, err)
	assert.NotNil(t, producer)
	assert.Equal(t, logger, producer.logger)
	assert.Equal(t, config, producer.configuration)
	assert.Equal(t, brokers, producer.brokers)
	assert.Equal(t, syncProducer, producer.kafkaProducer)
	assert.Equal(t, healthServer, producer.healthServer)
	assert.Equal(t, statsReporter, producer.statsReporter)
	assert.Equal(t, config.MetricRegistry, producer.metricsRegistry)
	assert.NotNil(t, producer.metricsStopChan)
	assert.NotNil(t, producer.metricsStoppedChan)
	assert.True(t, producer.healthServer.ProducerReady())
	assert.False(t, producer.healthServer.ChannelReady())

	// Return The Producer
	return producer
}
