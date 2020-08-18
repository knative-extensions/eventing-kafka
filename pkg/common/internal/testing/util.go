package testing

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/ghodss/yaml"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	commonutil "knative.dev/eventing-kafka/pkg/common/kafka/util"
)

// Test Constants
const (
	KnativeEventingNamespace       = "knative-eventing"
	SettingsConfigMapName          = "config-eventing-kafka"
	SaramaSettingsConfigKey        = "sarama"
	EventingKafkaSettingsConfigKey = "eventing-kafka"

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

// Returns A ConfigMap Containing The Desired Sarama Config JSON Fragment, Name And Namespace
func GetTestSaramaConfigMapNamespaced(name, namespace, saramaConfig, configuration string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		TypeMeta: v1.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: corev1.SchemeGroupVersion.String(),
		},
		ObjectMeta: v1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Data: map[string]string{
			SaramaSettingsConfigKey:        saramaConfig,
			EventingKafkaSettingsConfigKey: configuration,
		},
	}
}

// Returns A ConfigMap Containing The Desired Sarama Config JSON Fragment
func GetTestSaramaConfigMap(saramaConfig string, configuration string) *corev1.ConfigMap {
	return GetTestSaramaConfigMapNamespaced(SettingsConfigMapName, KnativeEventingNamespace, saramaConfig, configuration)
}

func GetDefaultSaramaConfig(t *testing.T) *sarama.Config {
	config := commonutil.NewSaramaConfig()
	assert.NotNil(t, config)
	jsonSettings, err := yaml.YAMLToJSON([]byte(SaramaDefaultConfigYaml))
	assert.Nil(t, err)
	assert.Nil(t, json.Unmarshal(jsonSettings, &config))
	return config
}

// Retries an HTTP GET request a specified number of times before giving up
func RetryGet(url string, pause time.Duration, retryCount int) (*http.Response, error) {
	var resp *http.Response
	var err error

	// Retry up to "retryCount" number of attempts, waiting for "pause" duration between tries.
	for tryCounter := 0; tryCounter < retryCount; tryCounter++ {
		if resp, err = http.Get(url); err == nil {
			// GET request succeeded; return immediately
			return resp, err
		}
		time.Sleep(pause)
	}
	// Request failed too many times; return the error for caller to process
	return resp, err
}
