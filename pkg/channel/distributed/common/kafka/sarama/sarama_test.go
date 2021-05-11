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
	"strconv"
	"testing"
	"time"

	"knative.dev/eventing-kafka/pkg/common/client"
	"knative.dev/pkg/system"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/api/resource"
	commonconfig "knative.dev/eventing-kafka/pkg/common/config"
	"knative.dev/eventing-kafka/pkg/common/constants"
	commontesting "knative.dev/eventing-kafka/pkg/common/testing"
)

const (
	// EKDefaultConfigYaml is intended to match what's in 300-eventing-kafka-configmap.yaml
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
  brokers: REPLACE_WITH_CLUSTER_URL
  enableSaramaLogging: false
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

	config, configuration, err := LoadSettings(context.TODO(), "myClient", configMap.Data, nil)
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
	assert.Equal(t, "REPLACE_WITH_CLUSTER_URL", configuration.Kafka.Brokers)
}

func TestLoadEventingKafkaSettings(t *testing.T) {

	commontesting.SetTestEnvironment(t)

	// Define The TestCase Struct
	type TestCase struct {
		name            string
		ekConfig        map[string]string
		expectErr       bool
		expectNilConfig bool
		expectConfig    *commonconfig.EventingKafkaConfig
	}

	receiverReplicas, _ := strconv.Atoi(commontesting.ReceiverReplicas)
	dispatcherReplicas, _ := strconv.Atoi(commontesting.DispatcherReplicas)

	// testEKConfig represents the expected EventingKafkaConfig struct when using the TestEKConfig string
	testEKConfig := &commonconfig.EventingKafkaConfig{
		Receiver: commonconfig.EKReceiverConfig{
			EKKubernetesConfig: commonconfig.EKKubernetesConfig{
				CpuLimit:      resource.MustParse(commontesting.ReceiverCpuLimit),
				CpuRequest:    resource.MustParse(commontesting.ReceiverCpuRequest),
				MemoryLimit:   resource.MustParse(commontesting.ReceiverMemoryLimit),
				MemoryRequest: resource.MustParse(commontesting.ReceiverMemoryRequest),
				Replicas:      receiverReplicas,
			},
		},
		Dispatcher: commonconfig.EKDispatcherConfig{
			EKKubernetesConfig: commonconfig.EKKubernetesConfig{
				CpuLimit:      resource.MustParse(commontesting.DispatcherCpuLimit),
				CpuRequest:    resource.MustParse(commontesting.DispatcherCpuRequest),
				MemoryLimit:   resource.MustParse(commontesting.DispatcherMemoryLimit),
				MemoryRequest: resource.MustParse(commontesting.DispatcherMemoryRequest),
				Replicas:      dispatcherReplicas,
			},
		},
		Kafka: commonconfig.EKKafkaConfig{
			Brokers: commontesting.BrokerString,
		},
	}

	// Create The TestCases
	testCases := []TestCase{
		{
			name:     "Basic",
			ekConfig: map[string]string{constants.EventingKafkaSettingsConfigKey: commontesting.TestEKConfig},
		},
		{
			name:      "Invalid YAML",
			ekConfig:  map[string]string{constants.EventingKafkaSettingsConfigKey: "\tinvalidYAML"},
			expectErr: true,
		},
		{
			name:            "Empty Config (Defaults Only)",
			ekConfig:        map[string]string{constants.EventingKafkaSettingsConfigKey: ""},
			expectNilConfig: true,
		},
		{
			name: "Defaults Overridden",
			ekConfig: map[string]string{constants.EventingKafkaSettingsConfigKey: `
cloudevents:
  maxIdleConns: 1234
  maxIdleConnsPerHost: 123
kafka:
  authSecretName: new-secret-name
  authSecretNamespace: new-secret-namespace`},
			expectConfig: &commonconfig.EventingKafkaConfig{
				CloudEvents: commonconfig.EKCloudEventConfig{
					MaxIdleConns:        1234,
					MaxIdleConnsPerHost: 123,
				},
				Kafka: commonconfig.EKKafkaConfig{
					AuthSecretName:      "new-secret-name",
					AuthSecretNamespace: "new-secret-namespace",
				},
			},
		},
		{
			name:      "Nil Config",
			ekConfig:  nil,
			expectErr: true,
		},
	}

	// Run The TestCases
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {

			// Perform The Test (note: LoadSettings calls LoadEventingKafkaSettings)
			eventingKafkaConfig, err := LoadEventingKafkaSettings(testCase.ekConfig)

			// Verify Expected State
			if testCase.expectErr {
				assert.NotNil(t, err)
			} else if testCase.expectNilConfig {
				assert.Nil(t, err)
				assert.Nil(t, eventingKafkaConfig)
			} else {
				assert.Nil(t, err)
				assert.NotNil(t, eventingKafkaConfig)

				if testCase.expectConfig != nil {
					// Verify that defaultable-values are not overridden if provided
					assert.Equal(t, testCase.expectConfig.CloudEvents.MaxIdleConns, eventingKafkaConfig.CloudEvents.MaxIdleConns)
					assert.Equal(t, testCase.expectConfig.CloudEvents.MaxIdleConnsPerHost, eventingKafkaConfig.CloudEvents.MaxIdleConnsPerHost)
					assert.Equal(t, testCase.expectConfig.Kafka.AuthSecretNamespace, eventingKafkaConfig.Kafka.AuthSecretNamespace)
					assert.Equal(t, testCase.expectConfig.Kafka.AuthSecretName, eventingKafkaConfig.Kafka.AuthSecretName)
				} else {
					// Verify that the defaults are set (i.e. not zero)
					assert.Equal(t, constants.DefaultMaxIdleConns, eventingKafkaConfig.CloudEvents.MaxIdleConns)
					assert.Equal(t, constants.DefaultMaxIdleConnsPerHost, eventingKafkaConfig.CloudEvents.MaxIdleConnsPerHost)
					assert.Equal(t, system.Namespace(), eventingKafkaConfig.Kafka.AuthSecretNamespace)
					assert.Equal(t, DefaultAuthSecretName, eventingKafkaConfig.Kafka.AuthSecretName)

					// Verify other settings in the eventing-kafka config
					assert.Equal(t, testEKConfig.Receiver.CpuLimit, eventingKafkaConfig.Receiver.CpuLimit)
					assert.Equal(t, testEKConfig.Receiver.CpuRequest, eventingKafkaConfig.Receiver.CpuRequest)
					assert.Equal(t, testEKConfig.Receiver.MemoryLimit, eventingKafkaConfig.Receiver.MemoryLimit)
					assert.Equal(t, testEKConfig.Receiver.MemoryRequest, eventingKafkaConfig.Receiver.MemoryRequest)
					assert.Equal(t, testEKConfig.Receiver.Replicas, eventingKafkaConfig.Receiver.Replicas)
					assert.Equal(t, testEKConfig.Dispatcher.CpuLimit, eventingKafkaConfig.Dispatcher.CpuLimit)
					assert.Equal(t, testEKConfig.Dispatcher.CpuRequest, eventingKafkaConfig.Dispatcher.CpuRequest)
					assert.Equal(t, testEKConfig.Dispatcher.MemoryLimit, eventingKafkaConfig.Dispatcher.MemoryLimit)
					assert.Equal(t, testEKConfig.Dispatcher.MemoryRequest, eventingKafkaConfig.Dispatcher.MemoryRequest)
					assert.Equal(t, testEKConfig.Dispatcher.Replicas, eventingKafkaConfig.Dispatcher.Replicas)
					assert.Equal(t, testEKConfig.Kafka.Brokers, eventingKafkaConfig.Kafka.Brokers)
				}
			}
		})
	}
}

func TestLoadSettings(t *testing.T) {
	commontesting.SetTestEnvironment(t)

	// Define The TestCase Struct
	type TestCase struct {
		name           string
		config         map[string]string
		authConfig     *client.KafkaAuthConfig
		expectErr      bool
		expectDefaults bool
	}

	// Create The TestCases
	testCases := []TestCase{
		{
			name:   "Basic",
			config: map[string]string{constants.SaramaSettingsConfigKey: commontesting.OldSaramaConfig},
		},
		{
			name:      "Invalid Sarama YAML",
			config:    map[string]string{constants.SaramaSettingsConfigKey: "\tinvalidYAML"},
			expectErr: true,
		},
		{
			name:      "Invalid EKConfig YAML",
			config:    map[string]string{constants.EventingKafkaSettingsConfigKey: "\tinvalidYAML"},
			expectErr: true,
		},
		{
			name:       "Auth Config With Empty User",
			config:     map[string]string{constants.SaramaSettingsConfigKey: commontesting.OldSaramaConfig},
			authConfig: &client.KafkaAuthConfig{SASL: &client.KafkaSaslConfig{User: ""}},
		},
		{
			name:           "Empty Config (Defaults Only)",
			config:         map[string]string{constants.SaramaSettingsConfigKey: ""},
			expectDefaults: true,
		},
		{
			name:      "Nil Config",
			config:    nil,
			expectErr: true,
		},
	}

	// Run The TestCases
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {

			// Perform The Test (note that the eventingKafkaConfig is tested separately in TestLoadEventingKafkaSettings)
			saramaConfig, eventingKafkaConfig, err := LoadSettings(context.TODO(), "", testCase.config, testCase.authConfig)

			// Verify Expected State
			assert.Nil(t, eventingKafkaConfig)
			if testCase.expectErr {
				assert.NotNil(t, err)
			} else {
				assert.Nil(t, err)
				assert.NotNil(t, saramaConfig)
				// Verify a few Sarama settings; this isn't testing the ConfigBuilder itself so it doesn't need to be detailed
				if testCase.expectDefaults {
					assert.Equal(t, "", saramaConfig.Net.SASL.User)
					assert.Equal(t, "", saramaConfig.Net.SASL.Password)
				} else {
					assert.Equal(t, commontesting.OldUsername, saramaConfig.Net.SASL.User)
					assert.Equal(t, commontesting.OldPassword, saramaConfig.Net.SASL.Password)
				}
			}
		})
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
