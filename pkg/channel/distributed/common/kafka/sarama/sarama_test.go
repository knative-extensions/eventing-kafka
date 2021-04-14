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

package sarama

import (
	"context"
	"crypto/tls"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/api/resource"
	commonconfig "knative.dev/eventing-kafka/pkg/common/config"
	"knative.dev/eventing-kafka/pkg/common/configmaploader"
	fakeConfigmapLoader "knative.dev/eventing-kafka/pkg/common/configmaploader/fake"
	"knative.dev/eventing-kafka/pkg/common/constants"
	commonconstants "knative.dev/eventing-kafka/pkg/common/constants"
	commontesting "knative.dev/eventing-kafka/pkg/common/testing"
)

const (
	// EKDefaultConfigYaml is intended to match what's in 200-eventing-kafka-configmap.yaml
	EKDefaultConfigYaml = `
receiver:
  cpuRequest: 100m
  memoryRequest: 50Mi
  replicas: 1
dispatcher:
  cpuRequest: 100m
  memoryRequest: 50Mi
  replicas: 1
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
)

// Test Enabling Sarama Logging
func TestEnableSaramaLogging(t *testing.T) {

	// Restore Sarama Logger After Test
	saramaLoggerPlaceholder := sarama.Logger
	defer func() {
		sarama.Logger = saramaLoggerPlaceholder
	}()

	// Perform The Test
	EnableSaramaLogging(true)

	// Verify Results (Not Much Is Possible)
	sarama.Logger.Print("TestMessage - Should See")

	EnableSaramaLogging(false)

	// Verify Results Visually
	sarama.Logger.Print("TestMessage - Should Be Hidden")
}

// This test is specifically to validate that our default settings (used in 200-eventing-kafka-configmap.yaml)
// are valid.  If the defaults in the file change, change this test to match for verification purposes.
func TestLoadDefaultSaramaSettings(t *testing.T) {
	commontesting.SetTestEnvironment(t)
	configMap := commontesting.GetTestSaramaConfigMap(EKDefaultSaramaConfig, EKDefaultConfigYaml)
	configmapLoader := fakeConfigmapLoader.NewFakeConfigmapLoader()
	configmapLoader.Register(commonconstants.SettingsConfigMapMountPath, configMap.Data)
	ctx := context.WithValue(context.Background(), configmaploader.Key{}, configmapLoader.Load)

	config, configuration, err := LoadSettings(ctx, "myClient", nil)
	assert.Nil(t, err)
	// Make sure all of our default Sarama settings were loaded properly
	assert.Equal(t, tls.ClientAuthType(0), config.Net.TLS.Config.ClientAuth)
	assert.Equal(t, sarama.SASLMechanism("PLAIN"), config.Net.SASL.Mechanism)
	assert.Equal(t, int16(1), config.Net.SASL.Version)
	assert.Equal(t, time.Duration(300000000000), config.Metadata.RefreshFrequency)
	assert.Equal(t, time.Duration(5000000000), config.Consumer.Offsets.AutoCommit.Interval)
	assert.Equal(t, time.Duration(604800000000000), config.Consumer.Offsets.Retention)
	assert.Equal(t, true, config.Consumer.Return.Errors)
	assert.Equal(t, "myClient", config.ClientID)

	// Make sure all of our default eventing-kafka settings were loaded properly
	// Specifically checking the type (e.g. int64, int16, int) is important
	assert.Equal(t, resource.Quantity{}, configuration.Receiver.CpuLimit)
	assert.Equal(t, resource.MustParse("100m"), configuration.Receiver.CpuRequest)
	assert.Equal(t, resource.Quantity{}, configuration.Receiver.MemoryLimit)
	assert.Equal(t, resource.MustParse("50Mi"), configuration.Receiver.MemoryRequest)
	assert.Equal(t, 1, configuration.Receiver.Replicas)
	assert.Equal(t, int32(4), configuration.Kafka.Topic.DefaultNumPartitions)
	assert.Equal(t, int16(1), configuration.Kafka.Topic.DefaultReplicationFactor)
	assert.Equal(t, int64(604800000), configuration.Kafka.Topic.DefaultRetentionMillis)
	assert.Equal(t, resource.Quantity{}, configuration.Dispatcher.CpuLimit)
	assert.Equal(t, resource.MustParse("100m"), configuration.Dispatcher.CpuRequest)
	assert.Equal(t, resource.Quantity{}, configuration.Dispatcher.MemoryLimit)
	assert.Equal(t, resource.MustParse("50Mi"), configuration.Dispatcher.MemoryRequest)
	assert.Equal(t, 1, configuration.Dispatcher.Replicas)
	assert.Equal(t, "azure", configuration.Kafka.AdminType)
}

func TestLoadEventingKafkaSettings(t *testing.T) {
	commontesting.SetTestEnvironment(t)

	{
		// Set up a configmap and verify that the sarama settings are loaded properly from it
		configMap := commontesting.GetTestSaramaConfigMap(commontesting.OldSaramaConfig, commontesting.TestEKConfig)
		ctx := CreateContextWithConfigmapLoader(configMap.Data)
		saramaConfig, eventingKafkaConfig, err := LoadSettings(ctx, "", nil)
		assert.Nil(t, err)
		verifyTestEKConfigSettings(t, saramaConfig, eventingKafkaConfig)
	}

	{
		// Test the LoadEventingKafkaSettings function by itself
		configMap := commontesting.GetTestSaramaConfigMap(commontesting.OldSaramaConfig, commontesting.TestEKConfig)
		eventingKafkaConfig, err := LoadEventingKafkaSettings(configMap.Data)
		assert.Nil(t, err)
		assert.Equal(t, commontesting.DispatcherReplicas, fmt.Sprint(eventingKafkaConfig.Dispatcher.Replicas))
	}

	{
		// Verify that invalid YAML returns an error
		configMap := commontesting.GetTestSaramaConfigMap(commontesting.OldSaramaConfig, commontesting.TestEKConfig)
		configMap.Data[constants.EventingKafkaSettingsConfigKey] = "\tinvalidYAML"
		eventingKafkaConfig, err := LoadEventingKafkaSettings(configMap.Data)
		assert.Nil(t, eventingKafkaConfig)
		assert.NotNil(t, err)
	}

	{
		// Verify that a configmap with no data section returns an error
		eventingKafkaConfig, err := LoadEventingKafkaSettings(nil)
		assert.Nil(t, eventingKafkaConfig)
		assert.NotNil(t, err)
	}
}

func TestLoadSettings(t *testing.T) {
	{
		// Set up a configmap and verify that the sarama and eventing-kafka settings are loaded properly from it
		configMap := commontesting.GetTestSaramaConfigMap(commontesting.OldSaramaConfig, commontesting.TestEKConfig)
		ctx := CreateContextWithConfigmapLoader(configMap.Data)
		saramaConfig, eventingKafkaConfig, err := LoadSettings(ctx, "", nil)
		assert.Nil(t, err)
		verifyTestEKConfigSettings(t, saramaConfig, eventingKafkaConfig)
	}

	{
		// Verify that nil configmap data returns an error
		ctx := CreateContextWithConfigmapLoader(nil)
		saramaConfig, eventingKafkaConfig, err := LoadSettings(ctx, "", nil)
		assert.Nil(t, saramaConfig)
		assert.Nil(t, eventingKafkaConfig)
		assert.NotNil(t, err)
	}

	{
		// Verify that empty configmap data does not return an error
		ctx := CreateContextWithConfigmapLoader(map[string]string{})
		_, _, err := LoadSettings(ctx, "", nil)
		assert.Nil(t, err)
	}

	{
		// Verify that a configmap with invalid YAML returns an error
		configMap := commontesting.GetTestSaramaConfigMap(commontesting.OldSaramaConfig, "")
		configMap.Data[constants.EventingKafkaSettingsConfigKey] = "\tinvalidYaml"
		ctx := CreateContextWithConfigmapLoader(configMap.Data)
		saramaConfig, eventingKafkaConfig, err := LoadSettings(ctx, "", nil)
		assert.Nil(t, saramaConfig)
		assert.Nil(t, eventingKafkaConfig)
		assert.NotNil(t, err)
	}
}

func TestAuthFromSarama(t *testing.T) {

	// Define The TestCase Struct
	type TestCase struct {
		name             string
		existingUser     string
		existingPassword string
		existingSaslType sarama.SASLMechanism
		expectNil        bool
		expectedUser     string
		expectedPassword string
		expectedSaslType string
	}

	// Create The TestCases
	testCases := []TestCase{
		{
			name:             "Empty User",
			existingUser:     "",
			existingPassword: "testPassword",
			existingSaslType: sarama.SASLTypeOAuth,
			expectNil:        true,
		},
		{
			name:             "Existing User",
			existingUser:     "testUser",
			existingPassword: "testPassword",
			existingSaslType: sarama.SASLTypePlaintext,
			expectedUser:     "testUser",
			expectedPassword: "testPassword",
			expectedSaslType: sarama.SASLTypePlaintext,
		},
	}

	// Run The TestCases
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			existingConfig := sarama.NewConfig()
			existingConfig.Net.SASL.User = testCase.existingUser
			existingConfig.Net.SASL.Password = testCase.existingPassword
			existingConfig.Net.SASL.Mechanism = testCase.existingSaslType
			config := AuthFromSarama(existingConfig)
			if testCase.expectNil {
				assert.Nil(t, config)
			} else {
				assert.Equal(t, testCase.expectedUser, config.SASL.User)
				assert.Equal(t, testCase.expectedPassword, config.SASL.Password)
				assert.Equal(t, testCase.expectedSaslType, config.SASL.SaslType)
			}
		})
	}
}

func TestStringifyHeaders(t *testing.T) {

	// Define The TestCase Struct
	type TestCase struct {
		name     string
		headers  []sarama.RecordHeader
		expected map[string][]string
	}

	// Create The TestCases
	testCases := []TestCase{
		{
			name:     "Nil Slice",
			headers:  nil,
			expected: map[string][]string{},
		},
		{
			name:     "Empty Slice",
			headers:  []sarama.RecordHeader{},
			expected: map[string][]string{},
		},
		{
			name: "One Header",
			headers: []sarama.RecordHeader{
				{
					Key:   []byte("one-key"),
					Value: []byte("one-value"),
				},
			},
			expected: map[string][]string{
				"one-key": {"one-value"},
			},
		},
		{
			name: "Two Different Headers",
			headers: []sarama.RecordHeader{
				{
					Key:   []byte("one-key"),
					Value: []byte("one-value"),
				},
				{
					Key:   []byte("two-key"),
					Value: []byte("two-value"),
				},
			},
			expected: map[string][]string{
				"one-key": {"one-value"},
				"two-key": {"two-value"},
			},
		},
		{
			name: "Two Identical Headers",
			headers: []sarama.RecordHeader{
				{
					Key:   []byte("one-key"),
					Value: []byte("one-value"),
				},
				{
					Key:   []byte("one-key"),
					Value: []byte("two-value"),
				},
			},
			expected: map[string][]string{
				"one-key": {"one-value", "two-value"},
			},
		},
	}

	// Run The TestCases
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {

			// Perform The Test
			stringHeaders := StringifyHeaders(testCase.headers)

			// Perform The Test With Pointers (should have same output)
			ptrs := make([]*sarama.RecordHeader, len(testCase.headers))
			for index, header := range testCase.headers {
				ptrs[index] = &sarama.RecordHeader{Key: header.Key, Value: header.Value}
			}
			stringHeadersFromPtrs := StringifyHeaderPtrs(ptrs)

			// Verify Expected State
			assert.Equal(t, testCase.expected, stringHeaders)
			assert.Equal(t, testCase.expected, stringHeadersFromPtrs)
		})
	}
}

func CreateContextWithConfigmapLoader(data map[string]string) context.Context {
	configmapLoader := fakeConfigmapLoader.NewFakeConfigmapLoader()
	configmapLoader.Register(commonconstants.SettingsConfigMapMountPath, data)
	return context.WithValue(context.Background(), configmaploader.Key{}, configmapLoader.Load)
}

func verifyTestEKConfigSettings(t *testing.T, saramaConfig *sarama.Config, eventingKafkaConfig *commonconfig.EventingKafkaConfig) {
	// Quick checks to make sure the loaded configs aren't complete junk
	assert.Equal(t, commontesting.OldUsername, saramaConfig.Net.SASL.User)
	assert.Equal(t, commontesting.OldPassword, saramaConfig.Net.SASL.Password)
	assert.Equal(t, commontesting.DispatcherReplicas, strconv.Itoa(eventingKafkaConfig.Dispatcher.Replicas))

	// Verify that the default CloudEvent settings are set (i.e. not zero)
	assert.Equal(t, constants.DefaultMaxIdleConns, eventingKafkaConfig.CloudEvents.MaxIdleConns)
	assert.Equal(t, constants.DefaultMaxIdleConnsPerHost, eventingKafkaConfig.CloudEvents.MaxIdleConnsPerHost)
}
