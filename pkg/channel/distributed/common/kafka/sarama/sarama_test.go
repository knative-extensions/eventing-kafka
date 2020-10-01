package sarama

import (
	"context"
	"crypto/tls"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/client-go/kubernetes/fake"
	commonconfig "knative.dev/eventing-kafka/pkg/channel/distributed/common/config"
	commonconstants "knative.dev/eventing-kafka/pkg/channel/distributed/common/constants"
	commontesting "knative.dev/eventing-kafka/pkg/channel/distributed/common/testing"
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
  adminType: azure
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

	EKDefaultSaramaConfigWithRootCert = `
Net:
  TLS:
    Enable: true
    Config:
      RootPEMs: # Array of Root Certificate PEM Files As Strings (Mind indentation and use '|-' Syntax To Avoid Terminating \n)
      - |-
        -----BEGIN CERTIFICATE-----
        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
        NAehp9bMeco=
        -----END CERTIFICATE-----
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
	EKDefaultSaramaConfigWithInsecureSkipVerify = `
Net:
  TLS:
    Enable: true
    Config:
      InsecureSkipVerify: true
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
	assert.Equal(t, sarama.V0_8_2_0, config.Version)
	assert.Nil(t, config.Net.TLS.Config)
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
	assert.Equal(t, "azure", configuration.Kafka.AdminType)
}

// Verify that the JSON fragment can be loaded into a sarama.Config struct
func TestMergeSaramaSettings(t *testing.T) {
	// Setup Environment
	assert.Nil(t, os.Setenv(system.NamespaceEnvKey, commonconstants.KnativeEventingNamespace))

	// Get a default Sarama config for verification that we don't overwrite settings when we merge
	defaultConfig := sarama.NewConfig()

	// Verify a few settings in different parts of two separate sarama.Config structures
	// Since it's a simple JSON merge we don't need to test every possible value.
	config, err := MergeSaramaSettings(nil, commontesting.GetTestSaramaConfigMap(commontesting.OldSaramaConfig, commontesting.TestEKConfig))
	assert.Nil(t, err)
	assert.NotNil(t, config)
	assert.Equal(t, commontesting.OldUsername, config.Net.SASL.User)
	assert.Equal(t, defaultConfig.Producer.Timeout, config.Producer.Timeout)
	assert.Equal(t, defaultConfig.Consumer.MaxProcessingTime, config.Consumer.MaxProcessingTime)

	config, err = MergeSaramaSettings(config, commontesting.GetTestSaramaConfigMap(commontesting.NewSaramaConfig, commontesting.TestEKConfig))
	assert.Nil(t, err)
	assert.NotNil(t, config)
	assert.Equal(t, sarama.V2_3_0_0, config.Version)
	assert.Equal(t, commontesting.NewUsername, config.Net.SASL.User)
	assert.Equal(t, defaultConfig.Producer.Timeout, config.Producer.Timeout)
	assert.Equal(t, defaultConfig.Consumer.MaxProcessingTime, config.Consumer.MaxProcessingTime)

	// Verify error when no Data section is provided
	configEmpty := commontesting.GetTestSaramaConfigMap(commontesting.NewSaramaConfig, commontesting.TestEKConfig)
	configEmpty.Data = nil
	config, err = MergeSaramaSettings(config, configEmpty)
	assert.NotNil(t, err)

	// Verify error when an invalid Version is provided
	configMap := commontesting.GetTestSaramaConfigMap(commontesting.NewSaramaConfig, commontesting.TestEKConfig)
	regexVersion := regexp.MustCompile(`Version:\s*\d*\.[\d.]*`) // Must have at least one period or it will match the "Version: 1" in Net.SASL
	configMap.Data[commontesting.SaramaSettingsConfigKey] =
		regexVersion.ReplaceAllString(configMap.Data[commontesting.SaramaSettingsConfigKey], "Version: INVALID")
	config, err = MergeSaramaSettings(config, configMap)
	assert.NotNil(t, err)

	// Verify error when an invalid RootPEMs is provided
	configMap = commontesting.GetTestSaramaConfigMap(EKDefaultSaramaConfigWithRootCert, commontesting.TestEKConfig)
	regexPEMs := regexp.MustCompile(`-----BEGIN CERTIFICATE-----`)
	configMap.Data[commontesting.SaramaSettingsConfigKey] =
		regexPEMs.ReplaceAllString(configMap.Data[commontesting.SaramaSettingsConfigKey], "INVALID CERT DATA")
	config, err = MergeSaramaSettings(config, configMap)
	assert.NotNil(t, err)

	// Verify that the RootPEMs section is merged properly
	configMap = commontesting.GetTestSaramaConfigMap(EKDefaultSaramaConfigWithRootCert, commontesting.TestEKConfig)
	config, err = MergeSaramaSettings(config, configMap)
	assert.Nil(t, err)
	assert.NotNil(t, config.Net.TLS.Config.RootCAs)

	// Verify that the InsecureSkipVerify flag can be set properly
	configMap = commontesting.GetTestSaramaConfigMap(EKDefaultSaramaConfigWithInsecureSkipVerify, commontesting.TestEKConfig)
	config, err = MergeSaramaSettings(config, configMap)
	assert.Nil(t, err)
	assert.True(t, config.Net.TLS.Config.InsecureSkipVerify)
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

	// Change a boolean flag in the TLS.Config struct (which is not Sarama-specific) and make sure the compare function
	// works with those sub-structs as well.
	config1.Net.TLS.Config = &tls.Config{}
	config2.Net.TLS.Config = &tls.Config{}

	config1.Net.TLS.Config.InsecureSkipVerify = true
	config2.Net.TLS.Config.InsecureSkipVerify = false
	assert.False(t, ConfigEqual(config1, config2))
	config2.Net.TLS.Config.InsecureSkipVerify = true
	assert.True(t, ConfigEqual(config1, config2))
	config1.Net.TLS.Config.InsecureSkipVerify = false
	assert.False(t, ConfigEqual(config1, config2))
	config2.Net.TLS.Config.InsecureSkipVerify = false
	assert.True(t, ConfigEqual(config1, config2))

	// Test config with TLS struct
	config1, err := MergeSaramaSettings(nil, commontesting.GetTestSaramaConfigMap(EKDefaultSaramaConfigWithRootCert, commontesting.TestEKConfig))
	assert.Nil(t, err)
	config2, err = MergeSaramaSettings(nil, commontesting.GetTestSaramaConfigMap(EKDefaultSaramaConfigWithRootCert, commontesting.TestEKConfig))
	assert.Nil(t, err)
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

func TestLoadSettings(t *testing.T) {
	// Set up a configmap and verify that the sarama and eventing-kafka settings are loaded properly from it
	ctx := getTestSaramaContext(t, commontesting.OldSaramaConfig, commontesting.TestEKConfig)
	saramaConfig, eventingKafkaConfig, err := LoadSettings(ctx)
	assert.Nil(t, err)
	verifyTestEKConfigSettings(t, saramaConfig, eventingKafkaConfig)

	// Verify that a context with no configmap returns an error
	ctx = context.WithValue(context.Background(), injectionclient.Key{}, fake.NewSimpleClientset())
	saramaConfig, eventingKafkaConfig, err = LoadSettings(ctx)
	assert.Nil(t, saramaConfig)
	assert.Nil(t, eventingKafkaConfig)
	assert.NotNil(t, err)

	// Verify that a configmap with no data section returns an error
	configMap := commontesting.GetTestSaramaConfigMap("", "")
	configMap.Data = nil
	ctx = context.WithValue(context.Background(), injectionclient.Key{}, fake.NewSimpleClientset(configMap))
	saramaConfig, eventingKafkaConfig, err = LoadSettings(ctx)
	assert.Nil(t, saramaConfig)
	assert.Nil(t, eventingKafkaConfig)
	assert.NotNil(t, err)

	// Verify that a configmap with invalid YAML returns an error
	configMap = commontesting.GetTestSaramaConfigMap(commontesting.OldSaramaConfig, "")
	configMap.Data[commontesting.EventingKafkaSettingsConfigKey] = "\tinvalidYaml"
	ctx = context.WithValue(context.Background(), injectionclient.Key{}, fake.NewSimpleClientset(configMap))
	saramaConfig, eventingKafkaConfig, err = LoadSettings(ctx)
	assert.Nil(t, saramaConfig)
	assert.Nil(t, eventingKafkaConfig)
	assert.NotNil(t, err)
}

func TestLoadEventingKafkaSettings_NoBackoff(t *testing.T) {
	// Set up a configmap and verify that the eventing-kafka settings are loaded properly from it
	ctx := getTestSaramaContext(t, commontesting.OldSaramaConfig, TestEKConfigNoBackoff)
	_, configuration, err := LoadSettings(ctx)
	assert.Nil(t, err)
	// Verify that the "RetryExponentialBackoff" field is set properly (bool pointer)
	assert.Nil(t, configuration.Dispatcher.RetryExponentialBackoff)
}

func verifyTestEKConfigSettings(t *testing.T, saramaConfig *sarama.Config, eventingKafkaConfig *commonconfig.EventingKafkaConfig) {
	// Quick checks to make sure the loaded configs aren't complete junk
	assert.Equal(t, commontesting.OldUsername, saramaConfig.Net.SASL.User)
	assert.Equal(t, commontesting.OldPassword, saramaConfig.Net.SASL.Password)
	assert.Equal(t, commontesting.DispatcherReplicas, strconv.Itoa(eventingKafkaConfig.Dispatcher.Replicas))
	assert.Equal(t, commontesting.DispatcherRetryInitial, strconv.FormatInt(eventingKafkaConfig.Dispatcher.RetryInitialIntervalMillis, 10))
	assert.Equal(t, commontesting.DispatcherRetry, strconv.FormatInt(eventingKafkaConfig.Dispatcher.RetryTimeMillis, 10))

	// Verify in particular that the "RetryExponentialBackoff" field is set properly (bool pointer)
	assert.NotNil(t, eventingKafkaConfig.Dispatcher.RetryExponentialBackoff)
	assert.True(t, *eventingKafkaConfig.Dispatcher.RetryExponentialBackoff)
}

func getTestSaramaContext(t *testing.T, saramaConfig string, eventingKafkaConfig string) context.Context {
	// Set up a configmap and return a context containing that configmap (for tests)
	assert.Nil(t, os.Setenv(system.NamespaceEnvKey, commonconstants.KnativeEventingNamespace))
	configMap := commontesting.GetTestSaramaConfigMap(saramaConfig, eventingKafkaConfig)
	fakeK8sClient := fake.NewSimpleClientset(configMap)
	ctx := context.WithValue(context.Background(), injectionclient.Key{}, fakeK8sClient)
	assert.NotNil(t, ctx)
	return ctx
}

// Test The ExtractRoots() Functionality
func TestExtractRootCerts(t *testing.T) {

	// The Sarama Config YAML String To Test
	beforeSaramaConfigYaml := EKDefaultSaramaConfigWithRootCert

	// Perform The Test (Extract The RootCert)
	afterSaramaConfigYaml, certPool, err := extractRootCerts(beforeSaramaConfigYaml)

	// Verify The RootCert Was Extracted Successfully & Returned In CertPool
	assert.NotNil(t, afterSaramaConfigYaml)
	assert.NotEqual(t, beforeSaramaConfigYaml, afterSaramaConfigYaml)
	assert.False(t, strings.Contains(afterSaramaConfigYaml, "RootPEMs"))
	assert.False(t, strings.Contains(afterSaramaConfigYaml, "-----BEGIN CERTIFICATE-----"))
	assert.False(t, strings.Contains(afterSaramaConfigYaml, "-----END CERTIFICATE-----"))
	assert.NotNil(t, certPool)
	assert.Nil(t, err)
	subjects := certPool.Subjects()
	assert.NotNil(t, subjects)
	assert.Len(t, subjects, 1)

	// Attempt To Extract Again (Now That There Arent' Any RootPEMs)
	finalSaramaConfigYaml, certPool, err := extractRootCerts(afterSaramaConfigYaml)

	// Verify The YAML String Is Unchanged And CertPool Is Nil
	assert.NotNil(t, finalSaramaConfigYaml)
	assert.Equal(t, afterSaramaConfigYaml, finalSaramaConfigYaml)
	assert.Nil(t, certPool)
	assert.Nil(t, err)
}
