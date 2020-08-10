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
	"knative.dev/eventing-kafka/pkg/channel/constants"
	channelhealth "knative.dev/eventing-kafka/pkg/channel/health"
	"knative.dev/eventing-kafka/pkg/channel/test"
	commonconfig "knative.dev/eventing-kafka/pkg/common/config"
	"knative.dev/eventing-kafka/pkg/common/metrics"
	logtesting "knative.dev/pkg/logging/testing"
	"knative.dev/pkg/system"

	"testing"
)

const (
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

	TestConfigMeta = `
Metadata:
  RefreshFrequency: 300000000000`

	TestConfigBase = TestConfigNet + TestConfigMeta + TestConfigConsumer

	TestConfigMetadataChange = TestConfigNet + `
Metadata:
  RefreshFrequency: 200000` + TestConfigConsumer

	TestConfigProducerChange = TestConfigNet + TestConfigMeta + `
Producer:
  MaxMessageBytes: 300` + TestConfigConsumer

	TestConfigConsumerChange = TestConfigNet + TestConfigMeta + TestConfigConsumer + `
  Fetch:
    Min: 200
`

	TestConfigAdminChange = `
Admin:
  Retry:
    Max: 100` + TestConfigNet + TestConfigMeta + TestConfigConsumer

	TestSaramaConfigYaml = `
Net:
  TLS:
    Config:
      ClientAuth: 0
  SASL:
    Mechanism: PLAIN
    Version: 1
    User: ` + test.KafkaUsername + `
    Password: ` + test.KafkaPassword + TestConfigMeta + TestConfigConsumer + `
ClientID: ` + test.ClientId + `
`
)

// Test The NewProducer Constructor
func TestNewProducer(t *testing.T) {

	// Create A Mock Kafka SyncProducer
	mockSyncProducer := test.NewMockSyncProducer()

	// Create A Test Producer
	producer := createTestProducer(t, mockSyncProducer)

	// Verify The Results
	assert.True(t, producer.healthServer.ProducerReady())
}

// Test The ProduceKafkaMessage() Functionality For Event With PartitionKey
func TestProduceKafkaMessage(t *testing.T) {

	// Create Test Data
	mockSyncProducer := test.NewMockSyncProducer()
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

// Test The Producer's ConfigChanged Functionality
func TestConfigChanged(t *testing.T) {

	// Setup Environment
	assert.Nil(t, os.Setenv(system.NamespaceEnvKey, constants.KnativeEventingNamespace))
	// Create Mocks
	mockSyncProducer := test.NewMockSyncProducer()
	producer := createTestProducer(t, mockSyncProducer)

	// Create Test Data
	configMap := &corev1.ConfigMap{
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

	// Apply a change to the Producer config
	runConfigChangedTest(t, producer, configMap, TestConfigProducerChange, true)

	// Apply the base config again
	runConfigChangedTest(t, producer, configMap, TestConfigBase, true)

	// Verify that non-Producer changes do not cause Reconfigure to be called
	runConfigChangedTest(t, producer, configMap, TestConfigMetadataChange, false)
	runConfigChangedTest(t, producer, configMap, TestConfigAdminChange, false)
	runConfigChangedTest(t, producer, configMap, TestConfigConsumerChange, false)
}

func runConfigChangedTest(t *testing.T, component *Producer, base *corev1.ConfigMap, changed string, expectedNewProducer bool) {
	// Change the Producer settings to the base config
	component.ConfigChanged(base)

	// Alter the configmap to use the changed settings
	newConfig := base
	newConfig.Data[commonconfig.SaramaSettingsConfigKey] = changed

	// Inform the Producer that the config has changed to the new settings
	newProducer := component.ConfigChanged(newConfig)

	// Verify that a new producer was created or not, as expected
	if expectedNewProducer {
		assert.NotNil(t, newProducer)
	} else {
		assert.Nil(t, newProducer)
	}
}

// Test The Producer's Close() Functionality
func TestClose(t *testing.T) {

	// Create A Mock Kafka SyncProducer
	mockSyncProducer := test.NewMockSyncProducer()

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
		assert.Equal(t, []string{test.KafkaBrokers}, brokers)
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
	producer, err := NewProducer(logger, testConfig, []string{test.KafkaBrokers}, statsReporter, healthServer)
	assert.Nil(t, err)
	assert.Equal(t, kafkaSyncProducer, producer.kafkaProducer)
	assert.Equal(t, healthServer, producer.healthServer)
	assert.Equal(t, statsReporter, producer.statsReporter)

	// Return The Producer
	return producer
}
