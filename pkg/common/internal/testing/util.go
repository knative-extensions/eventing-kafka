package testing

import (
	"context"
	"os"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	commonconfig "knative.dev/eventing-kafka/pkg/common/config"
	injectionclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/system"
)

// Test Constants
const (
	KnativeEventingNamespace = "knative-eventing"

	// These constants are used here to make sure that the CreateConsumerGroup() call doesn't have problems,
	// but they aren't non-testing defaults since most settings are now in 200-eventing-kafka-configmap.yaml
	ConfigAdminTimeout                      = "10000000000"
	ConfigNetKeepAlive                      = "30000000000"
	ConfigMetadataRefreshFrequency          = "300000000000"
	ConfigConsumerOffsetsAutoCommitInterval = "5000000000"
	ConfigConsumerOffsetsRetention          = "604800000000000"
	ConfigProducerIdempotent                = "false"
	ConfigProducerRequiredAcks              = "-1"

	SaramaDefaultConfigYaml = `
Admin:
  Timeout: ` + ConfigAdminTimeout + `
Net:
  KeepAlive: ` + ConfigNetKeepAlive + `
Metadata:
  RefreshFrequency: ` + ConfigMetadataRefreshFrequency + `
Consumer:
  Offsets:
    AutoCommit:
      Interval: ` + ConfigConsumerOffsetsAutoCommitInterval + `
    Retention: ` + ConfigConsumerOffsetsRetention + `
  Return:
    Errors: true
Producer:
  Idempotent: ` + ConfigProducerIdempotent + `
  RequiredAcks: ` + ConfigProducerRequiredAcks + `
  Return:
    Successes: true
`
)

// Returns A ConfigMap Containing The Desired Sarama Config JSON Fragment
func getTestSaramaConfigMap(saramaConfig string, ekConfig string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		TypeMeta: v1.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: corev1.SchemeGroupVersion.String(),
		},
		ObjectMeta: v1.ObjectMeta{
			Name:      commonconfig.SettingsConfigMapName,
			Namespace: system.Namespace(),
		},
		Data: map[string]string{
			commonconfig.SaramaSettingsConfigKey:        saramaConfig,
			commonconfig.EventingKafkaSettingsConfigKey: ekConfig,
		},
	}
}

func GetDefaultSaramaConfig(t *testing.T) *sarama.Config {
	assert.Nil(t, os.Setenv(system.NamespaceEnvKey, KnativeEventingNamespace))
	configMap := getTestSaramaConfigMap(SaramaDefaultConfigYaml, "")
	fakeK8sClient := fake.NewSimpleClientset(configMap)

	ctx := context.WithValue(context.Background(), injectionclient.Key{}, fakeK8sClient)

	config, _, err := commonconfig.LoadEventingKafkaSettings(ctx)
	assert.Nil(t, err)
	return config
}
