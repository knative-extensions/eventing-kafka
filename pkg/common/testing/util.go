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
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"knative.dev/eventing-kafka/pkg/channel/distributed/common/env"
	kafkaconstants "knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/constants"
	"knative.dev/eventing-kafka/pkg/common/constants"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/system"
)

// GetTestSaramaConfigMap Returns A ConfigMap Containing The Desired Sarama Config YAML
func GetTestSaramaConfigMap(version string, saramaConfig string, configuration string) *corev1.ConfigMap {
	return GetTestSaramaConfigMapNamespaced(version, constants.SettingsConfigMapName, system.Namespace(), saramaConfig, configuration)
}

// GetTestSaramaSecret Returns A Secret Containing The Desired Fields
func GetTestSaramaSecret(name string, username string, password string, namespace string, saslType string) *corev1.Secret {
	return &corev1.Secret{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Secret",
			APIVersion: corev1.SchemeGroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: system.Namespace(),
		},
		Data: map[string][]byte{
			kafkaconstants.KafkaSecretKeyUsername:  []byte(username),
			kafkaconstants.KafkaSecretKeyPassword:  []byte(password),
			kafkaconstants.KafkaSecretKeyNamespace: []byte(namespace),
			kafkaconstants.KafkaSecretKeySaslType:  []byte(saslType),
		},
	}
}

// GetTestSaramaConfigMapNamespaced Returns A ConfigMap Containing The Desired Sarama Config YAML, Name And Namespace
func GetTestSaramaConfigMapNamespaced(version, name, namespace, saramaConfig, configuration string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: corev1.SchemeGroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Data: map[string]string{
			constants.VersionConfigKey:               version,
			constants.SaramaSettingsConfigKey:        saramaConfig,
			constants.EventingKafkaSettingsConfigKey: configuration,
		},
	}
}

// SetTestEnvironment Sets the environment variables that are necessary for common components
func SetTestEnvironment(t *testing.T) {
	// The system.Namespace() call panics if the SYSTEM_NAMESPACE variable isn't set, so
	// this sets an example namespace value explicitly for testing purposes
	assert.Nil(t, os.Setenv(system.NamespaceEnvKey, SystemNamespace))
	// The logging.ConfigMapName() has a default if it isn't present, so this just uses that
	// function directly to ensure that the CONFIG_LOGGING_NAME variable is set
	assert.Nil(t, os.Setenv(env.KnativeLoggingConfigMapNameEnvVarKey, logging.ConfigMapName()))
}
