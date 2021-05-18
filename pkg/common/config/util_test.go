package config

import (
	"context"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/fake"
	injectionclient "knative.dev/pkg/client/injection/kube/client"
	logtesting "knative.dev/pkg/logging/testing"

	kafkav1beta1 "knative.dev/eventing-kafka/pkg/apis/messaging/v1beta1"
	commontesting "knative.dev/eventing-kafka/pkg/common/testing"
)

const (
	numPartitions            = int32(123)
	defaultNumPartitions     = int32(987)
	replicationFactor        = int16(22)
	defaultReplicationFactor = int16(33)
	defaultRetentionMillis   = int64(55555)
)

func TestConfigmapDataCheckSum(t *testing.T) {
	cases := []struct {
		name          string
		configmapData map[string]string
		expected      string
	}{{
		name:          "nil configmap data",
		configmapData: nil,
		expected:      "",
	}, {
		name:          "empty configmap data",
		configmapData: map[string]string{},
		expected:      "3c7e1501", // precomputed manually
	}, {
		name:          "with configmap data",
		configmapData: map[string]string{"foo": "bar"},
		expected:      "f39c9878", // precomputed manually
	}}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			retrieved := ConfigmapDataCheckSum(tc.configmapData)
			if diff := cmp.Diff(tc.expected, retrieved); diff != "" {
				t.Errorf("unexpected Config (-want, +got) = %v", diff)
			}
		})
	}
}

func TestGetAuthConfigFromKubernetes(t *testing.T) {
	// Setup Test Environment Namespaces
	commontesting.SetTestEnvironment(t)

	// Test Data
	oldAuthSecret := getSaramaTestSecret(t,
		commontesting.SecretName,
		commontesting.OldAuthUsername,
		commontesting.OldAuthPassword,
		commontesting.OldAuthNamespace,
		commontesting.OldAuthSaslType)

	// Define The TestCase Struct
	type TestCase struct {
		name    string
		secret  *corev1.Secret
		askName string
		wantErr bool
	}

	// Create The TestCases
	testCases := []TestCase{
		{
			name:    "Valid secret, valid request",
			secret:  oldAuthSecret,
			askName: commontesting.SecretName,
			wantErr: false,
		},
		{
			name:    "Valid secret, not found",
			secret:  oldAuthSecret,
			askName: "invalid-secret-name",
			wantErr: true,
		},
	}

	// Run The TestCases
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := context.WithValue(context.TODO(), injectionclient.Key{}, fake.NewSimpleClientset(testCase.secret))
			kafkaAuth, err := GetAuthConfigFromKubernetes(ctx, testCase.askName, commontesting.SystemNamespace)
			if !testCase.wantErr {
				assert.Nil(t, err)
				assert.NotNil(t, kafkaAuth)
				assert.Equal(t, commontesting.OldAuthUsername, kafkaAuth.SASL.User)
				assert.Equal(t, commontesting.OldAuthPassword, kafkaAuth.SASL.Password)
				assert.Equal(t, commontesting.OldAuthSaslType, kafkaAuth.SASL.SaslType)
			} else {
				assert.NotNil(t, err)
			}
		})
	}
}

func TestGetConfigFromSecret_Valid(t *testing.T) {
	secret := getSaramaTestSecret(t,
		commontesting.SecretName,
		commontesting.OldAuthUsername,
		commontesting.OldAuthPassword,
		commontesting.OldAuthNamespace,
		"")
	kafkaAuth := GetAuthConfigFromSecret(secret)
	assert.Equal(t, commontesting.OldAuthUsername, kafkaAuth.SASL.User)
	assert.Equal(t, commontesting.OldAuthPassword, kafkaAuth.SASL.Password)
	assert.Equal(t, sarama.SASLTypePlaintext, kafkaAuth.SASL.SaslType)
}

func TestGetConfigFromSecret_Invalid(t *testing.T) {
	authCfg := GetAuthConfigFromSecret(nil)
	assert.Nil(t, authCfg)
}

func getSaramaTestSecret(t *testing.T, name string,
	username string, password string, namespace string, saslType string) *corev1.Secret {
	commontesting.SetTestEnvironment(t)
	return commontesting.GetTestSaramaSecret(name, username, password, namespace, saslType)
}

// Test The NumPartitions Accessor
func TestNumPartitions(t *testing.T) {

	// Test Logger
	logger := logtesting.TestLogger(t)

	// Test Data
	configuration := &EventingKafkaConfig{Kafka: EKKafkaConfig{Topic: EKKafkaTopicConfig{DefaultNumPartitions: defaultNumPartitions}}}

	// Test The Default Failover Use Case
	channel := &kafkav1beta1.KafkaChannel{}
	actualNumPartitions := NumPartitions(channel, configuration, logger)
	assert.Equal(t, defaultNumPartitions, actualNumPartitions)

	// Test The Valid NumPartitions Use Case
	channel = &kafkav1beta1.KafkaChannel{Spec: kafkav1beta1.KafkaChannelSpec{NumPartitions: numPartitions}}
	actualNumPartitions = NumPartitions(channel, configuration, logger)
	assert.Equal(t, numPartitions, actualNumPartitions)
}

// Test The ReplicationFactor Accessor
func TestReplicationFactor(t *testing.T) {

	// Test Logger
	logger := logtesting.TestLogger(t)

	// Test Data
	configuration := &EventingKafkaConfig{Kafka: EKKafkaConfig{Topic: EKKafkaTopicConfig{DefaultReplicationFactor: defaultReplicationFactor}}}

	// Test The Default Failover Use Case
	channel := &kafkav1beta1.KafkaChannel{}
	actualReplicationFactor := ReplicationFactor(channel, configuration, logger)
	assert.Equal(t, defaultReplicationFactor, actualReplicationFactor)

	// Test The Valid ReplicationFactor Use Case
	channel = &kafkav1beta1.KafkaChannel{Spec: kafkav1beta1.KafkaChannelSpec{ReplicationFactor: replicationFactor}}
	actualReplicationFactor = ReplicationFactor(channel, configuration, logger)
	assert.Equal(t, replicationFactor, actualReplicationFactor)
}

// Test The RetentionMillis Accessor
func TestRetentionMillis(t *testing.T) {

	// Test Logger
	logger := logtesting.TestLogger(t)

	// Test Data
	configuration := &EventingKafkaConfig{Kafka: EKKafkaConfig{Topic: EKKafkaTopicConfig{DefaultRetentionMillis: defaultRetentionMillis}}}

	// Test The Default Failover Use Case
	channel := &kafkav1beta1.KafkaChannel{}
	actualRetentionMillis := RetentionMillis(channel, configuration, logger)
	assert.Equal(t, defaultRetentionMillis, actualRetentionMillis)

	// TODO - No RetentionMillis In eventing-contrib KafkaChannel
	//// Test The Valid RetentionMillis Use Case
	//channel = &kafkav1beta1.KafkaChannel{Spec: kafkav1beta1.KafkaChannelSpec{RetentionMillis: retentionMillis}}
	//actualRetentionMillis = RetentionMillis(channel, environment, logger)
	//assert.Equal(t, retentionMillis, actualRetentionMillis)
}
