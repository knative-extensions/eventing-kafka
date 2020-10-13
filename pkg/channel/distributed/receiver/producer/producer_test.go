package producer

import (
	"context"
	"encoding/json"
	"os"

	"github.com/Shopify/sarama"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/ghodss/yaml"
	gometrics "github.com/rcrowley/go-metrics"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	commonconfig "knative.dev/eventing-kafka/pkg/channel/distributed/common/config"
	commonconstants "knative.dev/eventing-kafka/pkg/channel/distributed/common/constants"
	"knative.dev/eventing-kafka/pkg/channel/distributed/common/metrics"
	"knative.dev/eventing-kafka/pkg/channel/distributed/receiver/constants"
	channelhealth "knative.dev/eventing-kafka/pkg/channel/distributed/receiver/health"
	receivertesting "knative.dev/eventing-kafka/pkg/channel/distributed/receiver/testing"
	logtesting "knative.dev/pkg/logging/testing"
	"knative.dev/pkg/system"

	"testing"
)

const (
	TestConfigAdmin = `
Admin:
  Timeout: 10000000000
`

	TestConfigNet = `
Net:
  TLS:
    Config:
      ClientAuth: 0
  SASL:
    Mechanism: PLAIN
    Version: 1
`
	TestConfigConsumer = `
Consumer:
  Offsets:
    AutoCommit:
        Interval: 5000000000
    Retention: 604800000000000
  Return:
    Errors: true
`

	TestConfigProducer = `
Producer:
  Idempotent: false
  RequiredAcks: -1
  Return:
    Successes: true
`

	TestConfigMeta = `
Metadata:
  RefreshFrequency: 300000000000`

	TestConfigBase = TestConfigAdmin + TestConfigNet + TestConfigMeta + TestConfigConsumer + TestConfigProducer

	TestConfigMetadataChange = TestConfigAdmin + TestConfigNet + `
Metadata:
  RefreshFrequency: 200000` + TestConfigConsumer + TestConfigProducer

	TestConfigProducerChange = TestConfigAdmin + TestConfigNet + TestConfigMeta + TestConfigProducer + `
Producer:
  MaxMessageBytes: 300` + TestConfigConsumer

	TestConfigConsumerChange = TestConfigAdmin + TestConfigNet + TestConfigMeta + TestConfigProducer + TestConfigConsumer + `
  Fetch:
    Min: 200
`

	TestConfigAdminChange = `
Admin:
  Retry:
    Max: 100` + TestConfigNet + TestConfigMeta + TestConfigProducer + TestConfigConsumer

	TestSaramaConfigYaml = TestConfigAdmin + `
Net:
  TLS:
    Config:
      ClientAuth: 0
  SASL:
    Mechanism: PLAIN
    Version: 1
    User: ` + receivertesting.KafkaUsername + `
    Password: ` + receivertesting.KafkaPassword + TestConfigMeta + TestConfigConsumer + `
ClientID: ` + receivertesting.ClientId + `
`
)

// Test The NewProducer Constructor
func TestNewProducer(t *testing.T) {

	// Create A Mock Kafka SyncProducer
	mockSyncProducer := receivertesting.NewMockSyncProducer()

	// Create A Test Producer
	producer := createTestProducer(t, mockSyncProducer)

	// Verify The Results
	assert.True(t, producer.healthServer.ProducerReady())
}

// Test The ProduceKafkaMessage() Functionality For Event With PartitionKey
func TestProduceKafkaMessage(t *testing.T) {

	// Create Test Data
	mockSyncProducer := receivertesting.NewMockSyncProducer()
	producer := createTestProducer(t, mockSyncProducer)
	channelReference := receivertesting.CreateChannelReference(receivertesting.ChannelName, receivertesting.ChannelNamespace)
	bindingMessage := receivertesting.CreateBindingMessage(cloudevents.VersionV1)

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

func getBaseConfigMap() *corev1.ConfigMap {
	return &corev1.ConfigMap{
		TypeMeta: v1.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: corev1.SchemeGroupVersion.String(),
		},
		ObjectMeta: v1.ObjectMeta{
			Name:      commonconfig.SettingsConfigMapName,
			Namespace: system.Namespace(),
		},
		Data: map[string]string{
			commonconfig.SaramaSettingsConfigKey: TestConfigBase,
		},
	}
}

// Test The Producer's ConfigChanged Functionality
func TestConfigChanged(t *testing.T) {
	// Stub The Kafka Producer Creation Wrapper With Test Version Returning Specified SyncProducer
	createSyncProducerWrapperPlaceholder := createSyncProducerWrapper
	createSyncProducerWrapper = func(config *sarama.Config, brokers []string) (sarama.SyncProducer, gometrics.Registry, error) {
		registry := gometrics.NewRegistry()
		return receivertesting.NewMockSyncProducer(), registry, nil
	}
	defer func() { createSyncProducerWrapper = createSyncProducerWrapperPlaceholder }()

	// Setup Environment
	assert.Nil(t, os.Setenv(system.NamespaceEnvKey, commonconstants.KnativeEventingNamespace))
	// Create Mocks
	mockSyncProducer := receivertesting.NewMockSyncProducer()
	producer := createTestProducer(t, mockSyncProducer)

	// Apply a change to the Producer config
	producer = runConfigChangedTest(t, producer, getBaseConfigMap(), TestConfigProducerChange, true)

	// Apply a metadata change
	producer = runConfigChangedTest(t, producer, getBaseConfigMap(), TestConfigMetadataChange, true)

	// Verify that Admin changes do not cause Reconfigure to be called
	producer = runConfigChangedTest(t, producer, getBaseConfigMap(), TestConfigAdminChange, false)
	// Verify that Consumer changes do not cause Reconfigure to be called
	producer = runConfigChangedTest(t, producer, getBaseConfigMap(), TestConfigConsumerChange, false)
	assert.NotNil(t, producer)
}

func runConfigChangedTest(t *testing.T, originalProducer *Producer, base *corev1.ConfigMap, changed string, expectedNewProducer bool) *Producer {

	// Change the Producer settings to the base config
	newProducer := originalProducer.ConfigChanged(base)
	if newProducer != nil {
		// Simulate what happens in main() when the producer changes
		originalProducer = newProducer
	}

	// Alter the configmap to use the changed settings
	newConfig := base
	newConfig.Data[commonconfig.SaramaSettingsConfigKey] = changed

	// Inform the Producer that the config has changed to the new settings
	newProducer = originalProducer.ConfigChanged(newConfig)

	// Verify that a new producer was created or not, as expected
	assert.Equal(t, expectedNewProducer, newProducer != nil)

	// Return either the new or original producer for use by the rest of the TestConfigChanged test
	if expectedNewProducer {
		return newProducer
	}
	return originalProducer
}

// Test The Producer's Close() Functionality
func TestClose(t *testing.T) {

	// Create A Mock Kafka SyncProducer
	mockSyncProducer := receivertesting.NewMockSyncProducer()

	// Create A Test Producer
	producer := createTestProducer(t, mockSyncProducer)

	// Perform The Test
	producer.Close()

	// Verify The Results
	assert.False(t, producer.healthServer.ProducerReady())
	assert.True(t, mockSyncProducer.Closed())
}

func getSaramaConfigFromYaml(t *testing.T, saramaYaml string) *sarama.Config {
	var config *sarama.Config
	jsonSettings, err := yaml.YAMLToJSON([]byte(saramaYaml))
	assert.Nil(t, err)
	assert.Nil(t, json.Unmarshal(jsonSettings, &config))
	return config
}

// Create A Producer With Specified KafkaProducer For Testing
func createTestProducer(t *testing.T, kafkaSyncProducer sarama.SyncProducer) *Producer {

	testConfig := getSaramaConfigFromYaml(t, TestSaramaConfigYaml)

	// Stub The Kafka Producer Creation Wrapper With Test Version Returning Specified SyncProducer
	createSyncProducerWrapperPlaceholder := createSyncProducerWrapper
	createSyncProducerWrapper = func(config *sarama.Config, brokers []string) (sarama.SyncProducer, gometrics.Registry, error) {
		assert.Equal(t, testConfig, config)
		assert.Equal(t, []string{receivertesting.KafkaBrokers}, brokers)
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
	producer, err := NewProducer(logger, testConfig, []string{receivertesting.KafkaBrokers}, statsReporter, healthServer)
	assert.Nil(t, err)
	assert.Equal(t, kafkaSyncProducer, producer.kafkaProducer)
	assert.Equal(t, healthServer, producer.healthServer)
	assert.Equal(t, statsReporter, producer.statsReporter)

	// Return The Producer
	return producer
}
