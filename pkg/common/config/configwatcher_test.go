package config

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
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	internaltesting "knative.dev/eventing-kafka/pkg/common/internal/testing"
	"knative.dev/eventing-kafka/pkg/common/kafka/constants"
	injectionclient "knative.dev/pkg/client/injection/kube/client"
	logtesting "knative.dev/pkg/logging/testing"
	"knative.dev/pkg/system"
)

// The watcher handler sets this variable to indicate that it was called
var watchedConfigMap *corev1.ConfigMap

const (
	OldClientId                = "TestOldClientId"
	NewClientId                = "TestNewClientId"
	OldUsername                = "TestOldUsername"
	NewUsername                = "TestNewUsername"
	TestDispatcherReplicas     = "3"
	TestDispatcherRetryInitial = "5000"
	TestDispatcherRetry        = "500000"

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
	EKDefaultConfigYaml = `
channel:
  cpuLimit: 200m
  cpuRequest: 100m
  memoryLimit: 100Mi
  memoryRequest: 50Mi
  replicas: 1
default:
  numPartitions: 4
  replicationFactor: 1
  retentionMillis: 604800000
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
  brokers: "eventhub.servicebus.windows.net:9093"
  offset:
    commitAsync: false
    commitDurationMillis: 30000
    commitMessageCount: 50
  password: TODO
  provider: azure
  secret: kafka-credentials
  username: $Connection-String
metrics:
  domain: eventing-kafka
  port: 8081
serviceAccount: eventing-kafka-channel-controller
`

	OldSaramaConfig = `
Net:
  TLS:
    Enable: true
  SASL:
    Enable: true
    Mechanism: PLAIN
    Version: 1
    User: ` + OldUsername + `
    Password: TestPassword
Metadata:
  RefreshFrequency: 300000000000
ClientID: ` + OldClientId + `
`
	NewSaramaConfig = `
Net:
  TLS:
    Enable: true
  SASL:
    Enable: true
    Mechanism: PLAIN
    Version: 1
    User: ` + NewUsername + `
    Password: TestPassword
Metadata:
  RefreshFrequency: 300000000000
ClientID: ` + NewClientId + `
`
	TestEKConfig = `
dispatcher:
  cpuLimit: 500m
  cpuRequest: 300m
  memoryLimit: 128Mi
  memoryRequest: 50Mi
  replicas: ` + TestDispatcherReplicas + `
  retryInitialIntervalMillis: ` + TestDispatcherRetryInitial + `
  retryTimeMillis: ` + TestDispatcherRetry + `
  retryExponentialBackoff: true
`
)

// Returns A ConfigMap Containing The Desired Sarama Config JSON Fragment
func getTestSaramaConfigMap(saramaConfig string, ekConfig string) *corev1.ConfigMap {
	return internaltesting.GetTestSaramaConfigMap(saramaConfig, ekConfig)
}

// Test The InitializeObservability() Functionality
func TestInitializeConfigWatcher(t *testing.T) {

	// Test Data
	ctx := context.TODO()

	// Obtain a Test Logger (Required By be InitializeConfigWatcher function)
	logger := logtesting.TestLogger(t)

	// Setup Environment
	assert.Nil(t, os.Setenv(system.NamespaceEnvKey, constants.KnativeEventingNamespace))

	// Create A Test Observability ConfigMap For The InitializeObservability() Call To Watch
	configMap := getTestSaramaConfigMap(OldSaramaConfig, TestEKConfig)

	// Create The Fake K8S Client And Add It To The ConfigMap
	fakeK8sClient := fake.NewSimpleClientset(configMap)

	// Add The Fake K8S Client To The Context (Required By InitializeObservability)
	ctx = context.WithValue(ctx, injectionclient.Key{}, fakeK8sClient)

	// Perform The Test (Initialize The Observability Watcher)
	err := InitializeConfigWatcher(logger, ctx, configWatcherHandler)
	assert.Nil(t, err)

	testConfigMap, err := fakeK8sClient.CoreV1().ConfigMaps(system.Namespace()).Get(SettingsConfigMapName, v1.GetOptions{})
	assert.Nil(t, err)
	assert.Equal(t, testConfigMap.Data["sarama"], OldSaramaConfig)

	// Change the config map and verify the handler is called
	testConfigMap.Data["sarama"] = NewSaramaConfig

	// The configWatcherHandler should change this to a valid ConfigMap
	watchedConfigMap = nil

	testConfigMap, err = fakeK8sClient.CoreV1().ConfigMaps(system.Namespace()).Update(testConfigMap)
	assert.Nil(t, err)
	assert.Equal(t, testConfigMap.Data["sarama"], NewSaramaConfig)

	// Wait for the configWatcherHandler to be called (happens pretty quickly; loop usually only runs once)
	for try := 0; watchedConfigMap == nil && try < 100; try++ {
		time.Sleep(5 * time.Millisecond)
	}
	assert.NotNil(t, watchedConfigMap)
	assert.Equal(t, watchedConfigMap.Data["sarama"], NewSaramaConfig)
}

// Handler function for the ConfigMap watcher
func configWatcherHandler(configMap *corev1.ConfigMap) {
	// Set this package variable to indicate that the test watcher was called
	watchedConfigMap = configMap
}

func TestLoadSettingsConfigMap(t *testing.T) {
	// Not much to this function; just set up a configmap and make sure it gets loaded
	assert.Nil(t, os.Setenv(system.NamespaceEnvKey, constants.KnativeEventingNamespace))
	configMap := getTestSaramaConfigMap(OldSaramaConfig, TestEKConfig)
	fakeK8sClient := fake.NewSimpleClientset(configMap)

	getConfigMap, err := LoadSettingsConfigMap(fakeK8sClient)
	assert.Nil(t, err)
	assert.Equal(t, configMap.Data[SaramaSettingsConfigKey], getConfigMap.Data[SaramaSettingsConfigKey])
}

func TestLoadEventingKafkaSettings(t *testing.T) {
	// Set up a configmap and verify that the sarama settings are loaded properly from it
	assert.Nil(t, os.Setenv(system.NamespaceEnvKey, constants.KnativeEventingNamespace))
	configMap := getTestSaramaConfigMap(OldSaramaConfig, TestEKConfig)
	fakeK8sClient := fake.NewSimpleClientset(configMap)

	ctx := context.WithValue(context.Background(), injectionclient.Key{}, fakeK8sClient)

	config, ekConfig, err := LoadEventingKafkaSettings(ctx)
	assert.Nil(t, err)
	// Quick checks to make sure the loaded configs aren't complete junk
	assert.Equal(t, OldClientId, config.ClientID)
	assert.Equal(t, OldUsername, config.Net.SASL.User)
	assert.Equal(t, TestDispatcherReplicas, strconv.Itoa(ekConfig.Dispatcher.Replicas))
	assert.Equal(t, TestDispatcherRetryInitial, strconv.FormatInt(ekConfig.Dispatcher.RetryInitialIntervalMillis, 10))
	assert.Equal(t, TestDispatcherRetry, strconv.FormatInt(ekConfig.Dispatcher.RetryTimeMillis, 10))
}

// This test is specifically to validate that our default settings (used in 200-eventing-kafka-configmap.yaml)
// are valid.  If the defaults in the file change, change this test to match for verification purposes.
func TestLoadDefaultSaramaSettings(t *testing.T) {
	assert.Nil(t, os.Setenv(system.NamespaceEnvKey, constants.KnativeEventingNamespace))
	configMap := getTestSaramaConfigMap(EKDefaultSaramaConfig, EKDefaultConfigYaml)
	fakeK8sClient := fake.NewSimpleClientset(configMap)
	ctx := context.WithValue(context.Background(), injectionclient.Key{}, fakeK8sClient)

	config, ekConfig, err := LoadEventingKafkaSettings(ctx)
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
	assert.Equal(t, "200m", ekConfig.Channel.CpuLimit)
	assert.Equal(t, "100m", ekConfig.Channel.CpuRequest)
	assert.Equal(t, "100Mi", ekConfig.Channel.MemoryLimit)
	assert.Equal(t, "50Mi", ekConfig.Channel.MemoryRequest)
	assert.Equal(t, 1, ekConfig.Channel.Replicas)
	assert.Equal(t, 4, ekConfig.Default.NumPartitions)
	assert.Equal(t, 1, ekConfig.Default.ReplicationFactor)
	assert.Equal(t, int64(604800000), ekConfig.Default.RetentionMillis)
	assert.Equal(t, "500m", ekConfig.Dispatcher.CpuLimit)
	assert.Equal(t, "300m", ekConfig.Dispatcher.CpuRequest)
	assert.Equal(t, "128Mi", ekConfig.Dispatcher.MemoryLimit)
	assert.Equal(t, "50Mi", ekConfig.Dispatcher.MemoryRequest)
	assert.Equal(t, 1, ekConfig.Dispatcher.Replicas)
	assert.Equal(t, int64(500), ekConfig.Dispatcher.RetryInitialIntervalMillis)
	assert.Equal(t, int64(300000), ekConfig.Dispatcher.RetryTimeMillis)
	assert.Equal(t, true, ekConfig.Dispatcher.RetryExponentialBackoff)
	assert.Equal(t, "eventhub.servicebus.windows.net:9093", ekConfig.Kafka.Brokers)
	assert.Equal(t, false, ekConfig.Kafka.Offset.CommitAsync)
	assert.Equal(t, int64(30000), ekConfig.Kafka.Offset.CommitDurationMillis)
	assert.Equal(t, int64(50), ekConfig.Kafka.Offset.CommitMessageCount)
	assert.Equal(t, "TODO", ekConfig.Kafka.Password)
	assert.Equal(t, "azure", ekConfig.Kafka.Provider)
	assert.Equal(t, "kafka-credentials", ekConfig.Kafka.Secret)
	assert.Equal(t, "$Connection-String", ekConfig.Kafka.Username)
	assert.Equal(t, "eventing-kafka", ekConfig.Metrics.Domain)
	assert.Equal(t, 8081, ekConfig.Metrics.Port)
	assert.Equal(t, "eventing-kafka-channel-controller", ekConfig.ServiceAccount)
}

// Verify that the JSON fragment can be loaded into a sarama.Config struct
func TestMergeSaramaSettings(t *testing.T) {
	// Setup Environment
	assert.Nil(t, os.Setenv(system.NamespaceEnvKey, constants.KnativeEventingNamespace))

	// Get a default Sarama config for verification that we don't overwrite settings when we merge
	defaultConfig := sarama.NewConfig()

	config := sarama.NewConfig()
	// Verify a few settings in different parts of two separate sarama.Config structures
	// Since it's a simple JSON merge we don't need to test every possible value.
	err := MergeSaramaSettings(config, getTestSaramaConfigMap(OldSaramaConfig, TestEKConfig))
	assert.Nil(t, err)
	assert.NotNil(t, config)
	assert.Equal(t, OldClientId, config.ClientID)
	assert.Equal(t, OldUsername, config.Net.SASL.User)
	assert.Equal(t, defaultConfig.Producer.Timeout, config.Producer.Timeout)
	assert.Equal(t, defaultConfig.Consumer.MaxProcessingTime, config.Consumer.MaxProcessingTime)

	err = MergeSaramaSettings(config, getTestSaramaConfigMap(NewSaramaConfig, TestEKConfig))
	assert.Nil(t, err)
	assert.NotNil(t, config)
	assert.Equal(t, NewClientId, config.ClientID)
	assert.Equal(t, NewUsername, config.Net.SASL.User)
	assert.Equal(t, defaultConfig.Producer.Timeout, config.Producer.Timeout)
	assert.Equal(t, defaultConfig.Consumer.MaxProcessingTime, config.Consumer.MaxProcessingTime)

	// Verify error when no Data section is provided
	configEmpty := getTestSaramaConfigMap(NewSaramaConfig, TestEKConfig)
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
	assert.True(t, SaramaConfigEqual(config1, config2))

	config1.Admin = sarama.Config{}.Admin // Zero out the entire Admin sub-struct
	assert.False(t, SaramaConfigEqual(config1, config2))

	config2.Admin = sarama.Config{}.Admin // Zero out the entire Admin sub-struct
	assert.True(t, SaramaConfigEqual(config1, config2))

	config1.Net.SASL.Version = 12345
	assert.False(t, SaramaConfigEqual(config1, config2))

	config2.Net.SASL.Version = 12345
	assert.True(t, SaramaConfigEqual(config1, config2))

	config1.Metadata.RefreshFrequency = 1234 * time.Second
	assert.False(t, SaramaConfigEqual(config1, config2))

	config2.Metadata.RefreshFrequency = 1234 * time.Second
	assert.True(t, SaramaConfigEqual(config1, config2))

	config1.Producer.Flush.Bytes = 12345678
	assert.False(t, SaramaConfigEqual(config1, config2))

	config2.Producer.Flush.Bytes = 12345678
	assert.True(t, SaramaConfigEqual(config1, config2))

	config1.RackID = "New Rack ID"
	assert.False(t, SaramaConfigEqual(config1, config2))

	config2.RackID = "New Rack ID"
	assert.True(t, SaramaConfigEqual(config1, config2))
}
