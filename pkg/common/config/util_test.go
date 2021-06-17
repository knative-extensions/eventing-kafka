package config

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"knative.dev/eventing-kafka/pkg/common/constants"
	"knative.dev/pkg/system"

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
	retentionMillis			 = int64(123456)
	defaultRetentionMillis	 = int64(604800000)
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
		name      string
		secret    *corev1.Secret
		askName   string
		expectNil bool
	}

	// Create The TestCases
	testCases := []TestCase{
		{
			name:    "Valid secret, valid request",
			secret:  oldAuthSecret,
			askName: commontesting.SecretName,
		},
		{
			name:    "Valid secret, backwards-compatibility",
			secret:  oldAuthSecret,
			askName: commontesting.SecretName,
		},
		{
			name:      "Valid secret, not found",
			secret:    oldAuthSecret,
			askName:   "invalid-secret-name",
			expectNil: true,
		},
	}

	// Run The TestCases
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := context.WithValue(context.TODO(), injectionclient.Key{}, fake.NewSimpleClientset(testCase.secret))
			kafkaAuth := GetAuthConfigFromKubernetes(ctx, testCase.askName, commontesting.SystemNamespace)
			if testCase.expectNil {
				assert.Nil(t, kafkaAuth)
			} else {
				assert.NotNil(t, kafkaAuth)
				assert.NotNil(t, kafkaAuth.SASL)
				assert.Equal(t, commontesting.OldAuthUsername, kafkaAuth.SASL.User)
				assert.Equal(t, commontesting.OldAuthPassword, kafkaAuth.SASL.Password)
				assert.Equal(t, commontesting.OldAuthSaslType, kafkaAuth.SASL.SaslType)
			}
		})
	}
}

func TestGetAuthConfigFromSecret(t *testing.T) {
	// Setup Test Environment Namespaces
	commontesting.SetTestEnvironment(t)

	// Define The TestCase Struct
	type TestCase struct {
		name      string
		data      map[string][]byte
		expectNil bool
	}

	// Asserts that, if there is a key in the data, that its mapped value is the same as the given value
	assertKey := func(t *testing.T, data map[string][]byte, key string, value string) {
		mapValue, ok := data[key]
		if !ok {
			return
		}
		assert.Equal(t, string(mapValue), value)
	}

	// Create The TestCases
	testCases := []TestCase{
		{
			name: "Valid secret",
			data: map[string][]byte{
				constants.KafkaSecretKeyUsername: []byte(commontesting.OldAuthUsername),
				constants.KafkaSecretKeyPassword: []byte(commontesting.OldAuthPassword),
				constants.KafkaSecretKeySaslType: []byte(commontesting.OldAuthSaslType),
			},
		},
		{
			name: "Valid secret, backwards-compatibility, TLS and SASL",
			data: map[string][]byte{
				TlsCacert:    []byte("test-cacert"),
				TlsUsercert:  []byte("test-usercert"),
				TlsUserkey:   []byte("test-userkey"),
				TlsEnabled:   []byte("true"),
				SaslUser:     []byte("test-sasluser"),
				SaslPassword: []byte("test-password"),
			},
		},
		{
			name: "Valid secret, backwards-compatibility, TLS/SASL without CA cert",
			data: map[string][]byte{
				TlsEnabled:   []byte("true"),
				SaslUser:     []byte("test-sasluser"),
				SaslPassword: []byte("test-password"),
			},
		},
		{
			name: "Valid secret, backwards-compatibility, SASL, Invalid TLS",
			data: map[string][]byte{
				TlsEnabled:   []byte("invalid-bool"),
				SaslUser:     []byte("test-sasluser"),
				SaslPassword: []byte("test-password"),
			},
		},
		{
			name:      "Valid secret, backwards-compatibility, nil data",
			data:      nil,
			expectNil: true,
		},
	}

	// Run The TestCases
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			secret := &corev1.Secret{
				TypeMeta: metav1.TypeMeta{
					Kind:       "Secret",
					APIVersion: corev1.SchemeGroupVersion.String(),
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:      commontesting.SecretName,
					Namespace: system.Namespace(),
				},
				Data: testCase.data,
			}
			kafkaAuth := GetAuthConfigFromSecret(secret)
			if testCase.expectNil {
				assert.Nil(t, kafkaAuth)
			} else {
				assert.NotNil(t, kafkaAuth)
				assert.NotNil(t, kafkaAuth.SASL)
				assertKey(t, testCase.data, constants.KafkaSecretKeyUsername, kafkaAuth.SASL.User)
				assertKey(t, testCase.data, constants.KafkaSecretKeyPassword, kafkaAuth.SASL.Password)
				assertKey(t, testCase.data, constants.KafkaSecretKeySaslType, kafkaAuth.SASL.SaslType)
				assertKey(t, testCase.data, SaslUser, kafkaAuth.SASL.User)
				assertKey(t, testCase.data, SaslPassword, kafkaAuth.SASL.Password)
				if kafkaAuth.TLS != nil {
					assertKey(t, testCase.data, TlsCacert, kafkaAuth.TLS.Cacert)
					assertKey(t, testCase.data, TlsUsercert, kafkaAuth.TLS.Usercert)
					assertKey(t, testCase.data, TlsUserkey, kafkaAuth.TLS.Userkey)
				}
			}
		})
	}
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

	// Test The Valid RetentionMillis Use Case
	channel = &kafkav1beta1.KafkaChannel{Spec: kafkav1beta1.KafkaChannelSpec{RetentionMillis: retentionMillis}}
	actualRetentionMillis = RetentionMillis(channel, configuration, logger)
	assert.Equal(t, retentionMillis, actualRetentionMillis)
}