/*
Copyright 2019 The Knative Authors

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

package utils

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	clientcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	injectionclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/system"
	_ "knative.dev/pkg/system/testing"

	"knative.dev/eventing-kafka/pkg/common/client"
	"knative.dev/eventing-kafka/pkg/common/config"
	configtesting "knative.dev/eventing-kafka/pkg/common/config/testing"
	"knative.dev/eventing-kafka/pkg/common/constants"
	kafkasarama "knative.dev/eventing-kafka/pkg/common/kafka/sarama"
)

type KubernetesAPI struct {
	Client kubernetes.Interface
}

const (
	secretName      = "kafka-auth"
	secretNamespace = "my-namespace"
)

func TestGenerateTopicNameWithDot(t *testing.T) {
	expected := "knative-messaging-kafka.channel-namespace.channel-name"
	actual := TopicName(".", "channel-namespace", "channel-name")
	if expected != actual {
		t.Errorf("Expected '%s'. Actual '%s'", expected, actual)
	}
}

func TestGenerateTopicNameWithHyphen(t *testing.T) {
	expected := "knative-messaging-kafka-channel-namespace-channel-name"
	actual := TopicName("-", "channel-namespace", "channel-name")
	if expected != actual {
		t.Errorf("Expected '%s'. Actual '%s'", expected, actual)
	}
}

func TestGetKafkaConfig_BackwardsCompatibility(t *testing.T) {

	api := &KubernetesAPI{
		Client: fake.NewSimpleClientset(),
	}

	testCases := map[string]struct {
		corev1Input clientcorev1.CoreV1Interface
		secretName  string
		secretNS    string
		cacert      string
		usercert    string
		userkey     string
		user        string
		password    string
		tlsEnabled  string

		expected *client.KafkaAuthConfig
	}{
		"empty secret": {
			corev1Input: api.Client.CoreV1(),
			secretName:  secretName,
			secretNS:    secretNamespace,
			expected:    nil, // The Sarama builder expects a nil KafkaAuthConfig if no authentication is desired
		},
		"wrong secret name": {
			corev1Input: api.Client.CoreV1(),
			secretName:  "wrong",
			secretNS:    secretNamespace,
			cacert:      "cacert",
			usercert:    "usercert",
			userkey:     "userkey",
			user:        "user",
			password:    "password",
			expected:    nil,
		},
		"tls and sasl secret": {
			corev1Input: api.Client.CoreV1(),
			secretName:  secretName,
			secretNS:    secretNamespace,
			cacert:      "cacert",
			usercert:    "usercert",
			userkey:     "userkey",
			user:        "user",
			password:    "password",
			expected: &client.KafkaAuthConfig{
				TLS: &client.KafkaTlsConfig{
					Cacert:   "cacert",
					Usercert: "usercert",
					Userkey:  "userkey",
				},
				SASL: &client.KafkaSaslConfig{
					User:     "user",
					Password: "password",
					SaslType: configtesting.DefaultSecretSaslType,
				},
			},
		},
		"tls secret": {
			corev1Input: api.Client.CoreV1(),
			secretName:  secretName,
			secretNS:    secretNamespace,
			cacert:      "cacert",
			usercert:    "usercert",
			userkey:     "userkey",
			user:        "",
			password:    "",
			expected: &client.KafkaAuthConfig{
				TLS: &client.KafkaTlsConfig{
					Cacert:   "cacert",
					Usercert: "usercert",
					Userkey:  "userkey",
				},
			},
		},
		"sasl secret": {
			corev1Input: api.Client.CoreV1(),
			secretName:  secretName,
			secretNS:    secretNamespace,
			user:        "user",
			password:    "password",
			expected: &client.KafkaAuthConfig{
				TLS: nil,
				SASL: &client.KafkaSaslConfig{
					User:     "user",
					Password: "password",
					SaslType: configtesting.DefaultSecretSaslType,
				},
			},
		},
		"tls enabled but no secret": {
			corev1Input: api.Client.CoreV1(),
			secretName:  secretName,
			secretNS:    secretNamespace,
			tlsEnabled:  "true",
			expected: &client.KafkaAuthConfig{
				TLS: &client.KafkaTlsConfig{},
			},
		},
	}

	for n, tc := range testCases {
		t.Run(n, func(t *testing.T) {

			// set up the namespace and service account to copy secret into.
			kafkAuthSecretNS := tc.corev1Input.Namespaces()
			namespace := &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name: secretNamespace,
				}}

			_, nsCreateError := kafkAuthSecretNS.Create(context.Background(), namespace, metav1.CreateOptions{})
			assert.Nil(t, nsCreateError, "error creating namespace")

			// set up secret
			kafkaAuthSecret := tc.corev1Input.Secrets(secretNamespace)
			secret := createSecret(tc.cacert, tc.usercert, tc.userkey, tc.user, tc.password, tc.tlsEnabled)
			_, secretCreateError := kafkaAuthSecret.Create(context.Background(), secret, metav1.CreateOptions{})
			assert.Nil(t, secretCreateError, "error creating secret resources")

			ctx := context.WithValue(context.Background(), injectionclient.Key{}, fake.NewSimpleClientset(secret, namespace))
			configMap := map[string]string{
				"bootstrapServers":    "kafkabroker.kafka:9092",
				"authSecretName":      tc.secretName,
				"authSecretNamespace": tc.secretNS}
			kafkaConfig, err := GetKafkaConfig(ctx, "test-client-id", configMap, kafkasarama.LoadAuthConfig)
			assert.Nil(t, err)
			if tc.expected == nil {
				assert.Nil(t, kafkaConfig.EventingKafka.Auth)
			} else {
				assert.Nil(t, err)
				assert.Equal(t, tc.expected.TLS, kafkaConfig.EventingKafka.Auth.TLS)
				assert.Equal(t, tc.expected.SASL, kafkaConfig.EventingKafka.Auth.SASL)
			}

			// clean up after test
			assert.Nil(t, tc.corev1Input.Secrets(secretNamespace).Delete(context.Background(), secretName, metav1.DeleteOptions{}))
			assert.Nil(t, tc.corev1Input.Namespaces().Delete(context.Background(), secretNamespace, metav1.DeleteOptions{}))
		})
	}
}

func TestGetKafkaConfig(t *testing.T) {

	// Create an empty Sarama config for comparisons involving a missing "sarama" field in the configmap
	saramaEmptyConfig, _ := client.NewConfigBuilder().
		WithDefaults().
		FromYaml("").
		WithAuth(nil).
		WithClientId("test-client-id").
		Build(context.TODO())

	// Comparing Sarama structs requires ignoring some obstinate (and irrelevant to our concerns) fields
	saramaIgnoreOpts := []cmp.Option{
		cmpopts.IgnoreUnexported(saramaEmptyConfig.Version, x509.CertPool{}, tls.Config{}),
		cmpopts.IgnoreFields(saramaEmptyConfig.Consumer.Group.Rebalance, "Strategy"),
		cmpopts.IgnoreFields(*saramaEmptyConfig, "MetricRegistry"),
		cmpopts.IgnoreFields(saramaEmptyConfig.Producer, "Partitioner"),
	}

	// The loadEventingKafkaSettings function defaults the number of replicas to one
	defaultK8SConfig := config.EKKubernetesConfig{Replicas: 1}

	defaultCloudEvents := config.EKCloudEventConfig{
		MaxIdleConns:        constants.DefaultMaxIdleConns,
		MaxIdleConnsPerHost: constants.DefaultMaxIdleConnsPerHost,
	}

	kafkaConfig := func(brokers string, name string, namespace string) config.EKKafkaConfig {
		return config.EKKafkaConfig{
			Brokers:             brokers,
			AuthSecretName:      name,
			AuthSecretNamespace: namespace,
			Topic: config.EKKafkaTopicConfig{
				DefaultNumPartitions:     kafkasarama.DefaultNumPartitions,
				DefaultReplicationFactor: kafkasarama.DefaultReplicationFactor,
				DefaultRetentionMillis:   kafkasarama.DefaultRetentionMillis,
			},
		}
	}

	testCases := []struct {
		name     string
		data     map[string]string
		getError string
		expected *KafkaConfig
	}{
		{
			name:     "invalid config path",
			data:     nil,
			getError: "missing configuration",
		},
		{
			name:     "configmap with no data",
			data:     map[string]string{},
			getError: "missing configuration",
		},
		{
			name:     "configmap with no bootstrapServers key (backwards-compatibility)",
			data:     map[string]string{"key": "value"},
			getError: "missing or empty brokers in configuration",
		},
		{
			name:     "configmap with empty bootstrapServers value (backwards-compatibility)",
			data:     map[string]string{"bootstrapServers": ""},
			getError: "missing or empty brokers in configuration",
		},
		{
			name: "single bootstrapServers (backwards-compatibility)",
			data: map[string]string{"bootstrapServers": "kafkabroker.kafka:9092"},
			expected: &KafkaConfig{
				Brokers: []string{"kafkabroker.kafka:9092"},
				EventingKafka: &config.EventingKafkaConfig{
					CloudEvents: defaultCloudEvents,
					Kafka:       kafkaConfig("kafkabroker.kafka:9092", kafkasarama.DefaultAuthSecretName, system.Namespace()),
					Sarama: config.EKSaramaConfig{
						Config: saramaEmptyConfig,
					},
				},
			},
		},
		{
			name: "single bootstrapServers and auth secret name and namespace (backwards-compatibility)",
			data: map[string]string{"bootstrapServers": "kafkabroker.kafka:9092", "authSecretName": "kafka-auth-secret", "authSecretNamespace": "default"},
			expected: &KafkaConfig{
				Brokers: []string{"kafkabroker.kafka:9092"},
				EventingKafka: &config.EventingKafkaConfig{
					CloudEvents: defaultCloudEvents,
					Kafka:       kafkaConfig("kafkabroker.kafka:9092", "kafka-auth-secret", "default"),
					Sarama: config.EKSaramaConfig{
						Config: saramaEmptyConfig,
					},
				},
			},
		},
		{
			name: "multiple bootstrapServers (backwards-compatibility)",
			data: map[string]string{"bootstrapServers": "kafkabroker1.kafka:9092,kafkabroker2.kafka:9092"},
			expected: &KafkaConfig{
				Brokers: []string{"kafkabroker1.kafka:9092", "kafkabroker2.kafka:9092"},
				EventingKafka: &config.EventingKafkaConfig{
					CloudEvents: defaultCloudEvents,
					Kafka:       kafkaConfig("kafkabroker1.kafka:9092,kafkabroker2.kafka:9092", kafkasarama.DefaultAuthSecretName, system.Namespace()),
					Sarama: config.EKSaramaConfig{
						Config: saramaEmptyConfig,
					},
				},
			},
		},
		{
			name: "elevated max idle connections (backwards-compatibility)",
			data: map[string]string{"bootstrapServers": "kafkabroker.kafka:9092", "maxIdleConns": "9000"},
			expected: &KafkaConfig{
				Brokers: []string{"kafkabroker.kafka:9092"},
				EventingKafka: &config.EventingKafkaConfig{
					CloudEvents: config.EKCloudEventConfig{
						MaxIdleConns:        9000,
						MaxIdleConnsPerHost: 100,
					},
					Kafka: kafkaConfig("kafkabroker.kafka:9092", kafkasarama.DefaultAuthSecretName, system.Namespace()),
					Sarama: config.EKSaramaConfig{
						Config: saramaEmptyConfig,
					},
				},
			},
		},
		{
			name: "elevated max idle connections per host (backwards-compatibility)",
			data: map[string]string{"bootstrapServers": "kafkabroker.kafka:9092", "maxIdleConnsPerHost": "900"},
			expected: &KafkaConfig{
				Brokers: []string{"kafkabroker.kafka:9092"},
				EventingKafka: &config.EventingKafkaConfig{
					CloudEvents: config.EKCloudEventConfig{
						MaxIdleConns:        1000,
						MaxIdleConnsPerHost: 900,
					},
					Kafka: kafkaConfig("kafkabroker.kafka:9092", kafkasarama.DefaultAuthSecretName, system.Namespace()),
					Sarama: config.EKSaramaConfig{
						Config: saramaEmptyConfig,
					},
				},
			},
		},
		{
			name: "elevated max idle values (backwards-compatibility)",
			data: map[string]string{"bootstrapServers": "kafkabroker.kafka:9092", "maxIdleConns": "9000", "maxIdleConnsPerHost": "600"},
			expected: &KafkaConfig{
				Brokers: []string{"kafkabroker.kafka:9092"},
				EventingKafka: &config.EventingKafkaConfig{
					CloudEvents: config.EKCloudEventConfig{
						MaxIdleConns:        9000,
						MaxIdleConnsPerHost: 600,
					},
					Kafka: kafkaConfig("kafkabroker.kafka:9092", kafkasarama.DefaultAuthSecretName, system.Namespace()),
					Sarama: config.EKSaramaConfig{
						Config: saramaEmptyConfig,
					},
				},
			},
		},
		{
			name:     "invalid strings in configmap",
			data:     map[string]string{"bootstrapServers": "kafkabroker.kafka:9092", "maxIdleConns": "invalid-int"},
			getError: `failed to parse "maxIdleConns": strconv.Atoi: parsing "invalid-int": invalid syntax`,
		},
		{
			name:     "empty string in brokers",
			data:     map[string]string{"bootstrapServers": "kafkabroker.kafka:9092,"},
			getError: "empty brokers value in configuration",
		},
		// Tests for the versioned/consolidated configmap are light, as the LoadSettings function has its own unit tests
		{
			name: "versioned, multiple brokers, empty sarama field",
			data: map[string]string{
				constants.VersionConfigKey:               constants.CurrentConfigVersion,
				constants.EventingKafkaSettingsConfigKey: "kafka:\n  brokers: kafkabroker1.kafka:9092,kafkabroker2.kafka:9092",
			},
			expected: &KafkaConfig{
				Brokers: []string{"kafkabroker1.kafka:9092", "kafkabroker2.kafka:9092"},
				EventingKafka: &config.EventingKafkaConfig{
					Channel: config.EKChannelConfig{
						Dispatcher: config.EKDispatcherConfig{EKKubernetesConfig: defaultK8SConfig},
						// The consolidated channel doesn't use the Receiver config, but there are default values in it
						// (namely "Replicas") that make the zero-value of the Receiver struct invalid for comparisons
						Receiver: config.EKReceiverConfig{EKKubernetesConfig: defaultK8SConfig},
					},
					CloudEvents: defaultCloudEvents,
					Kafka: config.EKKafkaConfig{
						Brokers:             "kafkabroker1.kafka:9092,kafkabroker2.kafka:9092",
						AuthSecretName:      kafkasarama.DefaultAuthSecretName,
						AuthSecretNamespace: "knative-testing",
					},
					Sarama: config.EKSaramaConfig{
						Config: saramaEmptyConfig,
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Logf("Running %s", t.Name())

			got, err := GetKafkaConfig(context.TODO(), "test-client-id", tc.data,
				func(context.Context, string, string) *client.KafkaAuthConfig { return nil })

			if tc.getError != "" {
				assert.NotNil(t, err)
				assert.Equal(t, tc.getError, err.Error())
			} else {
				assert.Nil(t, err)
				if diff := cmp.Diff(tc.expected, got, saramaIgnoreOpts...); diff != "" {
					t.Errorf("unexpected Config (-want, +got) = %v", diff)
				}
			}

		})
	}
}

func TestFindContainer(t *testing.T) {
	testCases := []struct {
		name          string
		deployment    *appsv1.Deployment
		containerName string
		expected      *corev1.Container
	}{
		{
			name: "no containers in deployment",
			deployment: &appsv1.Deployment{Spec: appsv1.DeploymentSpec{
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{},
					},
				},
			}},
			containerName: "foo",
			expected:      nil,
		},
		{
			name: "no container found",
			deployment: &appsv1.Deployment{Spec: appsv1.DeploymentSpec{
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{
							Name:  "foo",
							Image: "example.registry.com/foo",
						}},
					},
				},
			}},
			containerName: "bar",
			expected:      nil,
		},
		{
			name: "container found",
			deployment: &appsv1.Deployment{Spec: appsv1.DeploymentSpec{
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{
							Name:  "bar",
							Image: "example.registry.com/bar",
						}, {
							Name:  "foo",
							Image: "example.registry.com/foo",
						}},
					},
				},
			}},
			containerName: "foo",
			expected: &corev1.Container{
				Name:  "foo",
				Image: "example.registry.com/foo",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Logf("Running %s", t.Name())
			assert.Equal(t, tc.expected, FindContainer(tc.deployment, tc.containerName))
		})
	}

}

func createSecret(cacert string, usercert string, userkey string, user string, password string, tlsEnabled string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: secretNamespace,
			Name:      secretName,
		},
		Data: map[string][]byte{
			config.TlsCacert:    []byte(cacert),
			config.TlsUsercert:  []byte(usercert),
			config.TlsUserkey:   []byte(userkey),
			config.TlsEnabled:   []byte(tlsEnabled),
			config.SaslUser:     []byte(user),
			config.SaslPassword: []byte(password),
		},
	}
}
