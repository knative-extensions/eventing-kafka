package testing

import (
	"net/http"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/ghodss/yaml"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Returns A ConfigMap Containing The Desired Sarama Config YAML
func GetTestSaramaConfigMap(saramaConfig string, configuration string) *corev1.ConfigMap {
	return GetTestSaramaConfigMapNamespaced(SettingsConfigMapName, KnativeEventingNamespace, saramaConfig, configuration)
}

// Returns A ConfigMap Containing The Desired Sarama Config YAML, Name And Namespace
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

func GetDefaultSaramaConfig(t *testing.T, initialConfig *sarama.Config) *sarama.Config {
	assert.NotNil(t, initialConfig)
	assert.Nil(t, yaml.Unmarshal([]byte(SaramaDefaultConfigYaml), initialConfig))
	return initialConfig
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
