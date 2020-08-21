package sarama

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/client-go/kubernetes/fake"
	commonconfig "knative.dev/eventing-kafka/pkg/common/config"
	commonconstants "knative.dev/eventing-kafka/pkg/common/constants"
	"knative.dev/eventing-kafka/pkg/common/kafka/constants"
	commontesting "knative.dev/eventing-kafka/pkg/common/testing"
	injectionclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/system"
)

const (
	// EKDefaultConfigYaml is intended to match what's in 200-eventing-kafka-configmap.yaml
	EKDefaultConfigYaml = `
channel:
  cpuLimit: 200m
  cpuRequest: 100m
  memoryLimit: 100Mi
  memoryRequest: 50Mi
  replicas: 1
dispatcher:
  cpuLimit: 500m
  cpuRequest: 300m
  memoryLimit: 128Mi
  memoryRequest: 50Mi
  replicas: 1
  retryInitialIntervalMillis: 500
  retryTimeMillis: 300000
  retryExponentialBackoff: true
kafka:
  topic:
    defaultNumPartitions: 4
    defaultReplicationFactor: 1
    defaultRetentionMillis: 604800000
  provider: azure
`

	EKDefaultSaramaConfig = `
Net:
  TLS:
    Config:
      ClientAuth: 0
  SASL:
    Mechanism: PLAIN
    Version: 1
Metadata:
  RefreshFrequency: 300000000000
Consumer:
  Offsets:
    AutoCommit:
        Interval: 5000000000
    Retention: 604800000000000
  Return:
    Errors: true
`

	TestEKConfigNoBackoff = `
dispatcher:
  cpuLimit: 500m
  cpuRequest: 300m
  memoryLimit: 128Mi
  memoryRequest: 50Mi
  replicas: ` + commontesting.DispatcherReplicas + `
  retryInitialIntervalMillis: ` + commontesting.DispatcherRetryInitial + `
  retryTimeMillis: ` + commontesting.DispatcherRetry + `
`
)

// Test Enabling Sarama Logging
func TestEnableSaramaLogging(t *testing.T) {

	// Restore Sarama Logger After Test
	saramaLoggerPlaceholder := sarama.Logger
	defer func() {
		sarama.Logger = saramaLoggerPlaceholder
	}()

	// Perform The Test
	EnableSaramaLogging()

	// Verify Results (Not Much Is Possible)
	sarama.Logger.Print("TestMessage")
}

// Test The NewSaramaConfig() Functionality
func TestNewSaramaConfig(t *testing.T) {
	config := NewSaramaConfig()
	assert.Equal(t, constants.ConfigKafkaVersion, config.Version)
	assert.True(t, config.Consumer.Return.Errors)
	assert.True(t, config.Producer.Return.Successes)
}

// Test The UpdateSaramaConfig() Functionality
func TestUpdateSaramaConfig(t *testing.T) {

	// Test Data
	clientId := "TestClientId"
	username := "TestUsername"
	password := "TestPassword"

	// Perform The Test
	config := sarama.NewConfig()
	UpdateSaramaConfig(config, clientId, username, password)

	// Verify The Results
	assert.Equal(t, clientId, config.ClientID)
	assert.Equal(t, username, config.Net.SASL.User)
	assert.Equal(t, password, config.Net.SASL.Password)
	assert.Equal(t, constants.ConfigKafkaVersion, config.Version)
	assert.True(t, config.Net.SASL.Enable)
	assert.True(t, config.Net.TLS.Enable)
	assert.Equal(t, &tls.Config{ClientAuth: tls.NoClientCert}, config.Net.TLS.Config)
}

// This test is specifically to validate that our default settings (used in 200-eventing-kafka-configmap.yaml)
// are valid.  If the defaults in the file change, change this test to match for verification purposes.
func TestLoadDefaultSaramaSettings(t *testing.T) {
	assert.Nil(t, os.Setenv(system.NamespaceEnvKey, commonconstants.KnativeEventingNamespace))
	configMap := commontesting.GetTestSaramaConfigMap(EKDefaultSaramaConfig, EKDefaultConfigYaml)
	fakeK8sClient := fake.NewSimpleClientset(configMap)
	ctx := context.WithValue(context.Background(), injectionclient.Key{}, fakeK8sClient)

	config, configuration, err := LoadSettings(ctx)
	assert.Nil(t, err)
	// Make sure all of our default Sarama settings were loaded properly
	assert.Equal(t, tls.ClientAuthType(0), config.Net.TLS.Config.ClientAuth)
	assert.Equal(t, sarama.SASLMechanism("PLAIN"), config.Net.SASL.Mechanism)
	assert.Equal(t, int16(1), config.Net.SASL.Version)
	assert.Equal(t, time.Duration(300000000000), config.Metadata.RefreshFrequency)
	assert.Equal(t, time.Duration(5000000000), config.Consumer.Offsets.AutoCommit.Interval)
	assert.Equal(t, time.Duration(604800000000000), config.Consumer.Offsets.Retention)
	assert.Equal(t, true, config.Consumer.Return.Errors)

	// Make sure all of our default eventing-kafka settings were loaded properly
	// Specifically checking the type (e.g. int64, int16, int) is important
	assert.Equal(t, resource.MustParse("200m"), configuration.Channel.CpuLimit)
	assert.Equal(t, resource.MustParse("100m"), configuration.Channel.CpuRequest)
	assert.Equal(t, resource.MustParse("100Mi"), configuration.Channel.MemoryLimit)
	assert.Equal(t, resource.MustParse("50Mi"), configuration.Channel.MemoryRequest)
	assert.Equal(t, 1, configuration.Channel.Replicas)
	assert.Equal(t, int32(4), configuration.Kafka.Topic.DefaultNumPartitions)
	assert.Equal(t, int16(1), configuration.Kafka.Topic.DefaultReplicationFactor)
	assert.Equal(t, int64(604800000), configuration.Kafka.Topic.DefaultRetentionMillis)
	assert.Equal(t, resource.MustParse("500m"), configuration.Dispatcher.CpuLimit)
	assert.Equal(t, resource.MustParse("300m"), configuration.Dispatcher.CpuRequest)
	assert.Equal(t, resource.MustParse("128Mi"), configuration.Dispatcher.MemoryLimit)
	assert.Equal(t, resource.MustParse("50Mi"), configuration.Dispatcher.MemoryRequest)
	assert.Equal(t, 1, configuration.Dispatcher.Replicas)
	assert.Equal(t, int64(500), configuration.Dispatcher.RetryInitialIntervalMillis)
	assert.Equal(t, int64(300000), configuration.Dispatcher.RetryTimeMillis)
	assert.Equal(t, true, *configuration.Dispatcher.RetryExponentialBackoff)
	assert.Equal(t, "azure", configuration.Kafka.Provider)
}

// Verify that the JSON fragment can be loaded into a sarama.Config struct
func TestMergeSaramaSettings(t *testing.T) {
	// Setup Environment
	assert.Nil(t, os.Setenv(system.NamespaceEnvKey, commonconstants.KnativeEventingNamespace))

	// Get a default Sarama config for verification that we don't overwrite settings when we merge
	defaultConfig := sarama.NewConfig()

	config := sarama.NewConfig()
	// Verify a few settings in different parts of two separate sarama.Config structures
	// Since it's a simple JSON merge we don't need to test every possible value.
	err := MergeSaramaSettings(config, commontesting.GetTestSaramaConfigMap(commontesting.OldSaramaConfig, commontesting.TestEKConfig))
	assert.Nil(t, err)
	assert.NotNil(t, config)
	assert.Equal(t, commontesting.OldClientId, config.ClientID)
	assert.Equal(t, commontesting.OldUsername, config.Net.SASL.User)
	assert.Equal(t, defaultConfig.Producer.Timeout, config.Producer.Timeout)
	assert.Equal(t, defaultConfig.Consumer.MaxProcessingTime, config.Consumer.MaxProcessingTime)

	err = MergeSaramaSettings(config, commontesting.GetTestSaramaConfigMap(commontesting.NewSaramaConfig, commontesting.TestEKConfig))
	assert.Nil(t, err)
	assert.NotNil(t, config)
	assert.Equal(t, commontesting.NewClientId, config.ClientID)
	assert.Equal(t, commontesting.NewUsername, config.Net.SASL.User)
	assert.Equal(t, defaultConfig.Producer.Timeout, config.Producer.Timeout)
	assert.Equal(t, defaultConfig.Consumer.MaxProcessingTime, config.Consumer.MaxProcessingTime)

	// Verify error when no Data section is provided
	configEmpty := commontesting.GetTestSaramaConfigMap(commontesting.NewSaramaConfig, commontesting.TestEKConfig)
	configEmpty.Data = nil
	err = MergeSaramaSettings(config, configEmpty)
	assert.NotNil(t, err)
}

// Verify that a sarama.Config struct can be serialized into a string without causing JSON errors
func TestMarshalSaramaJSON(t *testing.T) {
	config := sarama.NewConfig()
	jsonResult, err := MarshalSaramaJSON(config)
	assert.Nil(t, err)
	assert.NotEqual(t, "", jsonResult)

	// Verify that we can pull the JSON string back into a sarama.Config struct
	var newConfig sarama.Config
	err = json.Unmarshal([]byte(jsonResult), &newConfig)
	assert.Nil(t, err)

	// Check a few settings and make sure they're the same after the process
	assert.Equal(t, config.Admin, newConfig.Admin)
	assert.Equal(t, config.Net.TLS, newConfig.Net.TLS)
	assert.Equal(t, config.Net.SASL.User, newConfig.Net.SASL.User)
	assert.Equal(t, config.Net.SASL.Enable, newConfig.Net.SASL.Enable)
	assert.Equal(t, config.Metadata.RefreshFrequency, newConfig.Metadata.RefreshFrequency)
	assert.Equal(t, config.Producer.Compression, newConfig.Producer.Compression)
	assert.Equal(t, config.Consumer.Fetch, newConfig.Consumer.Fetch)
}

// Verify that comparisons of sarama config structs function as expected
func TestSaramaConfigEqual(t *testing.T) {
	config1 := sarama.NewConfig()
	config2 := sarama.NewConfig()

	// Change some of the values back and forth and verify that the comparison function is correctly evaluated
	assert.True(t, ConfigEqual(config1, config2))

	config1.Admin = sarama.Config{}.Admin // Zero out the entire Admin sub-struct
	assert.False(t, ConfigEqual(config1, config2))

	config2.Admin = sarama.Config{}.Admin // Zero out the entire Admin sub-struct
	assert.True(t, ConfigEqual(config1, config2))

	config1.Net.SASL.Version = 12345
	assert.False(t, ConfigEqual(config1, config2))

	config2.Net.SASL.Version = 12345
	assert.True(t, ConfigEqual(config1, config2))

	config1.Metadata.RefreshFrequency = 1234 * time.Second
	assert.False(t, ConfigEqual(config1, config2))

	config2.Metadata.RefreshFrequency = 1234 * time.Second
	assert.True(t, ConfigEqual(config1, config2))

	config1.Producer.Flush.Bytes = 12345678
	assert.False(t, ConfigEqual(config1, config2))

	config2.Producer.Flush.Bytes = 12345678
	assert.True(t, ConfigEqual(config1, config2))

	config1.RackID = "New Rack ID"
	assert.False(t, ConfigEqual(config1, config2))

	config2.RackID = "New Rack ID"
	assert.True(t, ConfigEqual(config1, config2))
}

func TestLoadEventingKafkaSettings(t *testing.T) {
	// Set up a configmap and verify that the sarama settings are loaded properly from it
	assert.Nil(t, os.Setenv(system.NamespaceEnvKey, commonconstants.KnativeEventingNamespace))
	configMap := commontesting.GetTestSaramaConfigMap(commontesting.OldSaramaConfig, commontesting.TestEKConfig)
	fakeK8sClient := fake.NewSimpleClientset(configMap)

	ctx := context.WithValue(context.Background(), injectionclient.Key{}, fakeK8sClient)

	saramaConfig, eventingKafkaConfig, err := LoadSettings(ctx)
	assert.Nil(t, err)
	verifyTestEKConfigSettings(t, saramaConfig, eventingKafkaConfig)
}

func TestLoadConfigFromMap(t *testing.T) {
	// Set up a configmap and verify that the sarama settings are loaded properly from it
	assert.Nil(t, os.Setenv(system.NamespaceEnvKey, commonconstants.KnativeEventingNamespace))
	configMap := commontesting.GetTestSaramaConfigMap(commontesting.OldSaramaConfig, commontesting.TestEKConfig)
	saramaConfig, eventingKafkaConfig, err := LoadConfigFromMap(configMap)
	assert.Nil(t, err)

	verifyTestEKConfigSettings(t, saramaConfig, eventingKafkaConfig)
}

func verifyTestEKConfigSettings(t *testing.T, saramaConfig *sarama.Config, eventingKafkaConfig *commonconfig.EventingKafkaConfig) {
	// Quick checks to make sure the loaded configs aren't complete junk
	assert.Equal(t, commontesting.OldClientId, saramaConfig.ClientID)
	assert.Equal(t, commontesting.OldUsername, saramaConfig.Net.SASL.User)
	assert.Equal(t, commontesting.DispatcherReplicas, strconv.Itoa(eventingKafkaConfig.Dispatcher.Replicas))
	assert.Equal(t, commontesting.DispatcherRetryInitial, strconv.FormatInt(eventingKafkaConfig.Dispatcher.RetryInitialIntervalMillis, 10))
	assert.Equal(t, commontesting.DispatcherRetry, strconv.FormatInt(eventingKafkaConfig.Dispatcher.RetryTimeMillis, 10))

	// Verify in particular that the "RetryExponentialBackoff" field is set properly (bool pointer)
	assert.NotNil(t, eventingKafkaConfig.Dispatcher.RetryExponentialBackoff)
	assert.True(t, *eventingKafkaConfig.Dispatcher.RetryExponentialBackoff)
}

func TestLoadEventingKafkaSettings_NoBackoff(t *testing.T) {
	// Set up a configmap and verify that the sarama settings are loaded properly from it
	assert.Nil(t, os.Setenv(system.NamespaceEnvKey, commonconstants.KnativeEventingNamespace))
	configMap := commontesting.GetTestSaramaConfigMap(commontesting.OldSaramaConfig, TestEKConfigNoBackoff)
	fakeK8sClient := fake.NewSimpleClientset(configMap)

	ctx := context.WithValue(context.Background(), injectionclient.Key{}, fakeK8sClient)

	_, configuration, err := LoadSettings(ctx)
	assert.Nil(t, err)
	// Verify that the "RetryExponentialBackoff" field is set properly (bool pointer)
	assert.Nil(t, configuration.Dispatcher.RetryExponentialBackoff)
}
