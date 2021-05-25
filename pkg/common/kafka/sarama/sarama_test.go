/*
Copyright 2021 The Knative Authors

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
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/client-go/kubernetes/fake"
	injectionclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/system"

	"knative.dev/eventing-kafka/pkg/common/client"
	commonconfig "knative.dev/eventing-kafka/pkg/common/config"
	"knative.dev/eventing-kafka/pkg/common/constants"
	commontesting "knative.dev/eventing-kafka/pkg/common/testing"
)

const (
	// EKDefaultConfigYaml is intended to match what's in 300-eventing-kafka-configmap.yaml
	EKDefaultConfigYaml = `
channel:
  adminType: azure
  receiver:
    cpuRequest: 100m
    memoryRequest: 50Mi
  dispatcher:
    cpuRequest: 100m
    memoryRequest: 50Mi
kafka:
  brokers: REPLACE_WITH_CLUSTER_URL
  enableSaramaLogging: false
  topic:
    defaultNumPartitions: 4
    defaultReplicationFactor: 1
    defaultRetentionMillis: 604800000
`
	EKDefaultSaramaConfig = `
enableLogging: false
config: |
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
	// EKDefaultConfigYamlOld is intended to match what used to be in 300-eventing-kafka-configmap.yaml
	// (still supported/tested for backwards compatibility)
	EKDefaultConfigYamlOld = `
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
	EKDefaultSaramaConfigOld = `
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

// mockGetAuth returns a function that satisfies the GetAuth prototype, returning the provided values
func mockGetAuth(authConfig *client.KafkaAuthConfig, err error) GetAuth {
	return func(_ context.Context, _ string, _ string) (*client.KafkaAuthConfig, error) {
		return authConfig, err
	}
}

// This test is specifically to validate that our default settings (used in 200-eventing-kafka-configmap.yaml)
// are valid.  If the defaults in the file change, change this test to match for verification purposes.
func TestLoadDefaultSaramaSettings(t *testing.T) {

	// Define The TestCase Struct
	type TestCase struct {
		name string
		data map[string]string
	}

	// Required for GetTestSaramaConfigMap
	commontesting.SetTestEnvironment(t)

	// Backward-compatibility test needs a missing version key
	noVersionData := commontesting.GetTestSaramaConfigMap("", EKDefaultSaramaConfigOld, EKDefaultConfigYamlOld).Data
	delete(noVersionData, constants.VersionConfigKey)

	// Create The TestCases
	testCases := []TestCase{
		{
			name: "Current Config (" + constants.CurrentConfigVersion + ")",
			data: commontesting.GetTestSaramaConfigMap(constants.CurrentConfigVersion, EKDefaultSaramaConfig, EKDefaultConfigYaml).Data,
		},
		{
			name: "Upgrade Config (no version)",
			data: noVersionData,
		},
	}

	// Run The TestCases
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			settings, err := LoadSettings(context.TODO(), "myClient", testCase.data, mockGetAuth(nil, nil))
			assert.Nil(t, err)
			verifySaramaSettings(t, settings)
			verifyEventingKafkaSettings(t, settings)
		})
	}
}

func verifySaramaSettings(t *testing.T, settings *commonconfig.EventingKafkaConfig) {
	// Make sure all of our default Sarama settings were loaded properly
	assert.Equal(t, tls.ClientAuthType(0), settings.Sarama.Config.Net.TLS.Config.ClientAuth)
	assert.Equal(t, sarama.SASLMechanism("PLAIN"), settings.Sarama.Config.Net.SASL.Mechanism)
	assert.Equal(t, int16(1), settings.Sarama.Config.Net.SASL.Version)
	assert.Equal(t, time.Duration(300000000000), settings.Sarama.Config.Metadata.RefreshFrequency)
	assert.Equal(t, time.Duration(5000000000), settings.Sarama.Config.Consumer.Offsets.AutoCommit.Interval)
	assert.Equal(t, time.Duration(604800000000000), settings.Sarama.Config.Consumer.Offsets.Retention)
	assert.Equal(t, true, settings.Sarama.Config.Consumer.Return.Errors)
	assert.Equal(t, "myClient", settings.Sarama.Config.ClientID)
}

func verifyEventingKafkaSettings(t *testing.T, settings *commonconfig.EventingKafkaConfig) {
	// Make sure all of our default eventing-kafka settings were loaded properly
	// Specifically checking the type (e.g. int64, int16, int) is important
	assert.Equal(t, resource.Quantity{}, settings.Channel.Receiver.CpuLimit)
	assert.Equal(t, resource.MustParse("100m"), settings.Channel.Receiver.CpuRequest)
	assert.Equal(t, resource.Quantity{}, settings.Channel.Receiver.MemoryLimit)
	assert.Equal(t, resource.MustParse("50Mi"), settings.Channel.Receiver.MemoryRequest)
	assert.Equal(t, 1, settings.Channel.Receiver.Replicas)
	assert.Equal(t, int32(4), settings.Kafka.Topic.DefaultNumPartitions)
	assert.Equal(t, int16(1), settings.Kafka.Topic.DefaultReplicationFactor)
	assert.Equal(t, int64(604800000), settings.Kafka.Topic.DefaultRetentionMillis)
	assert.Equal(t, resource.Quantity{}, settings.Channel.Dispatcher.CpuLimit)
	assert.Equal(t, resource.MustParse("100m"), settings.Channel.Dispatcher.CpuRequest)
	assert.Equal(t, resource.Quantity{}, settings.Channel.Dispatcher.MemoryLimit)
	assert.Equal(t, resource.MustParse("50Mi"), settings.Channel.Dispatcher.MemoryRequest)
	assert.Equal(t, 1, settings.Channel.Dispatcher.Replicas)
	assert.Equal(t, "azure", settings.Channel.AdminType)
	assert.Equal(t, "REPLACE_WITH_CLUSTER_URL", settings.Kafka.Brokers)
}

func Test_upgradeConfig(t *testing.T) {
	// Define The TestCase Struct
	type TestCase struct {
		name      string
		data      map[string]string
		expectNew bool
	}

	// Required for GetTestSaramaConfigMap
	commontesting.SetTestEnvironment(t)

	// Backward-compatibility test needs a missing version key
	noVersionData := commontesting.GetTestSaramaConfigMap("", EKDefaultSaramaConfigOld, EKDefaultConfigYamlOld).Data
	delete(noVersionData, constants.VersionConfigKey)

	// Create The TestCases
	testCases := []TestCase{
		{
			name: "Nil map",
			data: nil,
		},
		{
			name: "Missing eventing-kafka config",
			data: map[string]string{constants.SaramaSettingsConfigKey: "exists"},
		},
		{
			name: "Invalid eventing-kafka config",
			data: map[string]string{
				constants.EventingKafkaSettingsConfigKey: "\tInvalid",
				constants.SaramaSettingsConfigKey:        "exists",
			},
		},
		{
			name:      "Upgrade Config (no version)",
			data:      noVersionData,
			expectNew: true,
		},
		{
			name: "Upgrade Not Necessary (current version)",
			data: commontesting.GetTestSaramaConfigMap(constants.CurrentConfigVersion, EKDefaultSaramaConfig, EKDefaultConfigYaml).Data,
		},
	}

	// Run The TestCases
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			newConfig := upgradeConfig(testCase.data)
			if testCase.expectNew {
				assert.NotNil(t, newConfig)
				verifyEventingKafkaSettings(t, newConfig)
			} else {
				assert.Nil(t, newConfig)
			}
		})
	}
}

func TestLoadAuthConfig(t *testing.T) {
	// Setup Test Environment Namespaces
	commontesting.SetTestEnvironment(t)

	authSecret := commontesting.GetTestSaramaSecret(
		commontesting.SecretName,
		commontesting.OldAuthUsername,
		commontesting.OldAuthPassword,
		commontesting.OldAuthNamespace,
		commontesting.OldAuthSaslType)

	authSecretNoUser := commontesting.GetTestSaramaSecret(
		commontesting.SecretName,
		"",
		commontesting.OldAuthPassword,
		commontesting.OldAuthNamespace,
		commontesting.OldAuthSaslType)

	// Define The TestCase Struct
	type TestCase struct {
		name          string
		secret        *corev1.Secret
		secretName    string
		wantErr       bool
		wantNilConfig bool
	}

	// Create The TestCases
	testCases := []TestCase{
		{
			name:       "Valid secret, valid request",
			secret:     authSecret,
			secretName: commontesting.SecretName,
		},
		{
			name:       "Valid secret, not found",
			secret:     authSecret,
			secretName: "invalid-secret",
			wantErr:    true,
		},
		{
			name:          "Valid secret, empty user",
			secret:        authSecretNoUser,
			secretName:    commontesting.SecretName,
			wantNilConfig: true,
		},
	}

	// Run The TestCases
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := context.WithValue(context.TODO(), injectionclient.Key{}, fake.NewSimpleClientset(testCase.secret))
			kafkaAuth, err := LoadAuthConfig(ctx, testCase.secretName, system.Namespace())
			if !testCase.wantErr {
				assert.Nil(t, err)
				if testCase.wantNilConfig {
					assert.Nil(t, kafkaAuth)
				} else {
					assert.NotNil(t, kafkaAuth)
					assert.Equal(t, commontesting.OldAuthUsername, kafkaAuth.SASL.User)
					assert.Equal(t, commontesting.OldAuthPassword, kafkaAuth.SASL.Password)
					assert.Equal(t, commontesting.OldAuthSaslType, kafkaAuth.SASL.SaslType)
				}
			} else {
				assert.NotNil(t, err)
			}
		})
	}
}

func TestLoadEventingKafkaSettings(t *testing.T) {

	commontesting.SetTestEnvironment(t)

	// Define The TestCase Struct
	type TestCase struct {
		name         string
		ekConfig     map[string]string
		expectErr    bool
		expectConfig *commonconfig.EventingKafkaConfig
		expectEmpty  bool
	}

	receiverReplicas, _ := strconv.Atoi(commontesting.ReceiverReplicas)
	dispatcherReplicas, _ := strconv.Atoi(commontesting.DispatcherReplicas)

	// testEKConfig represents the expected EventingKafkaConfig struct when using the TestEKConfig string
	testEKConfig := &commonconfig.EventingKafkaConfig{
		Channel: commonconfig.EKChannelConfig{
			Dispatcher: commonconfig.EKDispatcherConfig{
				EKKubernetesConfig: commonconfig.EKKubernetesConfig{
					CpuLimit:      resource.MustParse(commontesting.DispatcherCpuLimit),
					CpuRequest:    resource.MustParse(commontesting.DispatcherCpuRequest),
					MemoryLimit:   resource.MustParse(commontesting.DispatcherMemoryLimit),
					MemoryRequest: resource.MustParse(commontesting.DispatcherMemoryRequest),
					Replicas:      dispatcherReplicas,
				},
			},
			Receiver: commonconfig.EKReceiverConfig{
				EKKubernetesConfig: commonconfig.EKKubernetesConfig{
					CpuLimit:      resource.MustParse(commontesting.ReceiverCpuLimit),
					CpuRequest:    resource.MustParse(commontesting.ReceiverCpuRequest),
					MemoryLimit:   resource.MustParse(commontesting.ReceiverMemoryLimit),
					MemoryRequest: resource.MustParse(commontesting.ReceiverMemoryRequest),
					Replicas:      receiverReplicas,
				},
			},
		},
		Kafka: commonconfig.EKKafkaConfig{
			Brokers: commontesting.BrokerString,
		},
	}

	// Create The TestCases
	testCases := []TestCase{
		{
			name: "Basic",
			ekConfig: map[string]string{
				constants.VersionConfigKey:               constants.CurrentConfigVersion,
				constants.EventingKafkaSettingsConfigKey: commontesting.TestEKConfig,
				constants.SaramaSettingsConfigKey:        commontesting.OldSaramaConfig},
		},
		{
			name: "Empty Sarama Config (creates default sarama.Config)",
			ekConfig: map[string]string{
				constants.VersionConfigKey:               constants.CurrentConfigVersion,
				constants.EventingKafkaSettingsConfigKey: commontesting.TestEKConfig},
		},
		{
			name:      "Invalid YAML",
			ekConfig:  map[string]string{constants.EventingKafkaSettingsConfigKey: "\tinvalidYAML"},
			expectErr: true,
		},
		{
			name: "Empty Config (Defaults Only)",
			ekConfig: map[string]string{
				constants.VersionConfigKey:               constants.CurrentConfigVersion,
				constants.EventingKafkaSettingsConfigKey: "",
				constants.SaramaSettingsConfigKey:        commontesting.OldSaramaConfig},
			expectEmpty: true,
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

			// Perform The Test (note: LoadSettings calls loadEventingKafkaSettings)
			eventingKafkaConfig, err := LoadSettings(context.TODO(), "", testCase.ekConfig, mockGetAuth(nil, nil))

			// Verify Expected State
			if testCase.expectErr {
				assert.NotNil(t, err)
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

					if !testCase.expectEmpty {
						// Verify other settings in the eventing-kafka config
						assert.Equal(t, testEKConfig.Channel.Receiver.CpuLimit, eventingKafkaConfig.Channel.Receiver.CpuLimit)
						assert.Equal(t, testEKConfig.Channel.Receiver.CpuRequest, eventingKafkaConfig.Channel.Receiver.CpuRequest)
						assert.Equal(t, testEKConfig.Channel.Receiver.MemoryLimit, eventingKafkaConfig.Channel.Receiver.MemoryLimit)
						assert.Equal(t, testEKConfig.Channel.Receiver.MemoryRequest, eventingKafkaConfig.Channel.Receiver.MemoryRequest)
						assert.Equal(t, testEKConfig.Channel.Receiver.Replicas, eventingKafkaConfig.Channel.Receiver.Replicas)
						assert.Equal(t, testEKConfig.Channel.Dispatcher.CpuLimit, eventingKafkaConfig.Channel.Dispatcher.CpuLimit)
						assert.Equal(t, testEKConfig.Channel.Dispatcher.CpuRequest, eventingKafkaConfig.Channel.Dispatcher.CpuRequest)
						assert.Equal(t, testEKConfig.Channel.Dispatcher.MemoryLimit, eventingKafkaConfig.Channel.Dispatcher.MemoryLimit)
						assert.Equal(t, testEKConfig.Channel.Dispatcher.MemoryRequest, eventingKafkaConfig.Channel.Dispatcher.MemoryRequest)
						assert.Equal(t, testEKConfig.Channel.Dispatcher.Replicas, eventingKafkaConfig.Channel.Dispatcher.Replicas)
						assert.Equal(t, testEKConfig.Kafka.Brokers, eventingKafkaConfig.Kafka.Brokers)
					}
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
		authErr        error
		expectErr      bool
		expectDefaults bool
	}

	// Create The TestCases
	testCases := []TestCase{
		{
			name: "Basic",
			config: map[string]string{
				constants.VersionConfigKey:               constants.CurrentConfigVersion,
				constants.SaramaSettingsConfigKey:        commontesting.OldSaramaConfig,
				constants.EventingKafkaSettingsConfigKey: commontesting.TestEKConfig,
			},
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
			name: "Auth Config With Empty User",
			config: map[string]string{
				constants.VersionConfigKey:               constants.CurrentConfigVersion,
				constants.SaramaSettingsConfigKey:        commontesting.OldSaramaConfig,
				constants.EventingKafkaSettingsConfigKey: commontesting.TestEKConfig,
			},
			authConfig:     &client.KafkaAuthConfig{SASL: &client.KafkaSaslConfig{User: ""}},
			expectDefaults: true, // The empty auth will remove the user/password from the OldSaramaConfig
		},
		{
			name: "Auth Error",
			config: map[string]string{
				constants.VersionConfigKey:               constants.CurrentConfigVersion,
				constants.SaramaSettingsConfigKey:        commontesting.OldSaramaConfig,
				constants.EventingKafkaSettingsConfigKey: commontesting.TestEKConfig,
			},
			authErr:   fmt.Errorf("test error"),
			expectErr: true,
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
			settings, err := LoadSettings(context.TODO(), "", testCase.config, mockGetAuth(testCase.authConfig, testCase.authErr))

			// Verify Expected State
			if testCase.expectErr {
				assert.NotNil(t, err)
			} else {
				assert.Nil(t, err)
				assert.NotNil(t, settings)
				// Verify a few Sarama settings; this isn't testing the ConfigBuilder itself so it doesn't need to be detailed
				if testCase.expectDefaults {
					assert.Equal(t, "", settings.Sarama.Config.Net.SASL.User)
					assert.Equal(t, "", settings.Sarama.Config.Net.SASL.Password)
				} else {
					assert.Equal(t, commontesting.OldUsername, settings.Sarama.Config.Net.SASL.User)
					assert.Equal(t, commontesting.OldPassword, settings.Sarama.Config.Net.SASL.Password)
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
