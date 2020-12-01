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

package testing

import (
	"net/http"
	"os"
	"testing"
	"time"

	"knative.dev/eventing-kafka/pkg/channel/distributed/common/env"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/system"

	"knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/constants"

	"github.com/Shopify/sarama"
	"github.com/ghodss/yaml"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Returns A ConfigMap Containing The Desired Sarama Config YAML
func GetTestSaramaConfigMap(saramaConfig string, configuration string) *corev1.ConfigMap {
	return GetTestSaramaConfigMapNamespaced(SettingsConfigMapName, system.Namespace(), saramaConfig, configuration)
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

// Obtain A Default Sarama Config With Custom Values For Testing
func GetDefaultSaramaConfig(t *testing.T) *sarama.Config {
	config := sarama.NewConfig()
	config.Version = constants.ConfigKafkaVersionDefault
	config.Consumer.Return.Errors = true
	config.Producer.Return.Successes = true
	assert.NotNil(t, config)
	assert.Nil(t, yaml.Unmarshal([]byte(SaramaDefaultConfigYaml), config))
	return config
}

// Retries an HTTP GET request a specified number of times before giving up.
// The retry is triggered if the GET response is either and error or the "retryAgain" value.  Passing in -1
// will retry only on errors (as -1 is not a possible HTTP response).
func RetryGet(url string, pause time.Duration, retryCount int, retryAgain int) (*http.Response, error) {
	var resp *http.Response
	var err error

	// Retry up to "retryCount" number of attempts, waiting for "pause" duration between tries.
	for tryCounter := 0; tryCounter < retryCount; tryCounter++ {
		if resp, err = http.Get(url); err == nil {
			// GET request succeeded; return immediately unless this is the "retryAgain" value (e.g. a 404)
			if retryAgain != resp.StatusCode {
				return resp, err
			}
		}
		time.Sleep(pause)
	}
	// Request failed too many times; return the error for caller to process
	return resp, err
}

// Sets the environment variables that are necessary for common components
func SetTestEnvironment(t *testing.T) {
	// The system.Namespace() call panics if the SYSTEM_NAMESPACE variable isn't set, so
	// this sets an example namespace value explicitly for testing purposes
	assert.Nil(t, os.Setenv(system.NamespaceEnvKey, SystemNamespace))
	// The logging.ConfigMapName() has a default if it isn't present, so this just uses that
	// function directly to ensure that the CONFIG_LOGGING_NAME variable is set
	assert.Nil(t, os.Setenv(env.KnativeLoggingConfigMapNameEnvVarKey, logging.ConfigMapName()))
}
