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

package testing

import (
	"regexp"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kafkaconstants "knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/constants"
	"knative.dev/eventing-kafka/pkg/common/constants"
	"knative.dev/pkg/system"
)

// Constants
const (
	DefaultSaramaConfigYaml = `
Version: 2.0.0
Admin:
  Timeout: 10000000000
Net:
  KeepAlive: 30000000000
  MaxOpenRequests: 1
  TLS:
    Enable: true
  SASL:
    Enable: true
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
Producer:
  Idempotent: true
  RequiredAcks: -1
  Return:
    Successes: true
`
	DefaultEventingKafkaConfigYaml = `
receiver:
  cpuRequest: 100m
  memoryRequest: 64Mi
  replicas: 1
dispatcher:
  cpuRequest: 300m
  memoryRequest: 64Mi
  replicas: 1
kafka:
  enableSaramaLogging: false
  brokers: ` + DefaultKafkaBroker + `
  topic:
    defaultNumPartitions: 4
    defaultReplicationFactor: 1
    defaultRetentionMillis: 604800000
    adminType: kafka
`
	DefaultKafkaBroker     = "TestBroker"
	DefaultSecretUsername  = "TestUsername"
	DefaultSecretPassword  = "TestPassword"
	DefaultSecretSaslType  = "PLAIN"
	DefaultSecretNamespace = "TestNamespace"
)

// KafkaConfigMapOption Enables Customization Of An Eventing-Kafka ConfigMap
type KafkaConfigMapOption func(configMap *corev1.ConfigMap)

// Create A New Eventing-Kafka ConfigMap For Testing
func NewKafkaConfigMap(options ...KafkaConfigMapOption) *corev1.ConfigMap {

	// Create A Base Kafka ConfigMap With Default Sarama & EventingKafka Configuration
	configMap := &corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: corev1.SchemeGroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      constants.SettingsConfigMapName,
			Namespace: system.Namespace(),
		},
		Data: map[string]string{
			constants.SaramaSettingsConfigKey:        DefaultSaramaConfigYaml,
			constants.EventingKafkaSettingsConfigKey: DefaultEventingKafkaConfigYaml,
		},
	}

	// Apply The Specified Eventing-Kafka ConfigMap Options
	for _, option := range options {
		option(configMap)
	}

	// Return The Custom Eventing-Kafka ConfigMap
	return configMap
}

// Modify The Default "Admin" Section Of The Sarama Config YAML
func WithModifiedSaramaAdmin(configMap *corev1.ConfigMap) {
	timeoutRegExp := regexp.MustCompile(`Timeout: 10000000000`)
	currentSaramaString := configMap.Data[constants.SaramaSettingsConfigKey]
	updatedSaramaString := timeoutRegExp.ReplaceAllString(currentSaramaString, "Timeout: 20000000000")
	configMap.Data[constants.SaramaSettingsConfigKey] = updatedSaramaString
}

// Modify The Default "Net" Section Of The Sarama Config YAML
func WithModifiedSaramaNet(configMap *corev1.ConfigMap) {
	maxOpenRequestsRegExp := regexp.MustCompile(`MaxOpenRequests: 1`)
	currentSaramaString := configMap.Data[constants.SaramaSettingsConfigKey]
	updatedSaramaString := maxOpenRequestsRegExp.ReplaceAllString(currentSaramaString, "MaxOpenRequests: 2")
	configMap.Data[constants.SaramaSettingsConfigKey] = updatedSaramaString
}

// Modify The Default "Net" Section Of The Sarama Config YAML
func WithModifiedSaramaMetadata(configMap *corev1.ConfigMap) {
	refreshFrequencyRegExp := regexp.MustCompile(`RefreshFrequency: 300000000000`)
	currentSaramaString := configMap.Data[constants.SaramaSettingsConfigKey]
	updatedSaramaString := refreshFrequencyRegExp.ReplaceAllString(currentSaramaString, "RefreshFrequency: 400000000000")
	configMap.Data[constants.SaramaSettingsConfigKey] = updatedSaramaString
}

// Modify The Default "Consumer" Section Of The Sarama Config YAML
func WithModifiedSaramaConsumer(configMap *corev1.ConfigMap) {
	intervalRegExp := regexp.MustCompile(`Interval: 5000000000`)
	currentSaramaString := configMap.Data[constants.SaramaSettingsConfigKey]
	updatedSaramaString := intervalRegExp.ReplaceAllString(currentSaramaString, "Interval: 6000000000")
	configMap.Data[constants.SaramaSettingsConfigKey] = updatedSaramaString
}

// Modify The Default "Producer" Section Of The Sarama Config YAML
func WithModifiedSaramaProducer(configMap *corev1.ConfigMap) {
	requiredAcksRegExp := regexp.MustCompile(`RequiredAcks: -1`)
	currentSaramaString := configMap.Data[constants.SaramaSettingsConfigKey]
	updatedSaramaString := requiredAcksRegExp.ReplaceAllString(currentSaramaString, "RequiredAcks: 0")
	configMap.Data[constants.SaramaSettingsConfigKey] = updatedSaramaString
}

// Remove The Entire Eventing-Kafka Configuration
func WithoutEventingKafkaConfiguration(configMap *corev1.ConfigMap) {
	delete(configMap.Data, constants.EventingKafkaSettingsConfigKey)
}

// KafkaConfigMapOption Enables Customization Of An Eventing-Kafka Secret
type KafkaSecretOption func(secret *corev1.Secret)

// Create A New Eventing-Kafka Secret For Testing
func NewKafkaSecret(options ...KafkaSecretOption) *corev1.Secret {

	// Create A Base Kafka Secret With Default Auth Configuration
	secret := &corev1.Secret{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Secret",
			APIVersion: corev1.SchemeGroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      constants.SettingsSecretName,
			Namespace: system.Namespace(),
		},
		Data: map[string][]byte{
			kafkaconstants.KafkaSecretKeyPassword:  []byte(DefaultSecretPassword),
			kafkaconstants.KafkaSecretKeySaslType:  []byte(DefaultSecretSaslType),
			kafkaconstants.KafkaSecretKeyUsername:  []byte(DefaultSecretUsername),
			kafkaconstants.KafkaSecretKeyNamespace: []byte(DefaultSecretNamespace),
		},
	}

	// Apply The Specified Eventing-Kafka ConfigMap Options
	for _, option := range options {
		option(secret)
	}

	// Return The Custom Eventing-Kafka Secret
	return secret
}

// Modify The Default Password Section Of The Secret Data
func WithModifiedPassword(secret *corev1.Secret) {
	secret.Data[kafkaconstants.KafkaSecretKeyPassword] = []byte("TestModifiedPassword")
}

// Modify The Default Username Section Of The Secret Data
func WithModifiedUsername(secret *corev1.Secret) {
	secret.Data[kafkaconstants.KafkaSecretKeyUsername] = []byte("TestModifiedUsername")
}

// Empty The Default Username Section Of The Secret Data
func WithEmptyUsername(secret *corev1.Secret) {
	secret.Data[kafkaconstants.KafkaSecretKeyUsername] = []byte("")
}

// Modify The Default SaslType Section Of The Secret Data
func WithModifiedSaslType(secret *corev1.Secret) {
	secret.Data[kafkaconstants.KafkaSecretKeySaslType] = []byte("TestModifiedSaslType")
}

// Modify The Default Namespace Section Of The Secret Data
func WithModifiedNamespace(secret *corev1.Secret) {
	secret.Data[kafkaconstants.KafkaSecretKeyNamespace] = []byte("TestModifiedNamespace")
}

// Remove the Data From The Secret
func WithMissingConfig(secret *corev1.Secret) {
	secret.Data = nil
}
