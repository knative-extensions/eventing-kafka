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
	"github.com/Shopify/sarama"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"knative.dev/pkg/system"

	commonconstants "knative.dev/eventing-kafka/pkg/common/constants"
	commontesting "knative.dev/eventing-kafka/pkg/common/testing"
)

// Constants
const (
	DefaultKafkaBroker     = "TestBroker"
	DefaultSecretUsername  = "TestUsername"
	DefaultSecretPassword  = "TestPassword"
	DefaultSecretSaslType  = sarama.SASLTypePlaintext
	DefaultSecretNamespace = "TestNamespace"

	ModifiedSecretUsername  = "TestModifiedUsername"
	ModifiedSecretPassword  = "TestModifiedPassword"
	ModifiedSecretSaslType  = sarama.SASLTypeSCRAMSHA256
	ModifiedSecretNamespace = "TestModifiedNamespace"
)

// KafkaSecretOption Enables Customization Of An Eventing-Kafka Secret
type KafkaSecretOption func(secret *corev1.Secret)

// NewKafkaSecret Creates A New Eventing-Kafka Secret For Testing
func NewKafkaSecret(options ...KafkaSecretOption) *corev1.Secret {

	// Create A Base Kafka Secret With Default Auth Configuration
	secret := &corev1.Secret{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Secret",
			APIVersion: corev1.SchemeGroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      commontesting.SecretName,
			Namespace: system.Namespace(),
		},
		Data: map[string][]byte{
			commonconstants.KafkaSecretKeyPassword:  []byte(DefaultSecretPassword),
			commonconstants.KafkaSecretKeySaslType:  []byte(DefaultSecretSaslType),
			commonconstants.KafkaSecretKeyUsername:  []byte(DefaultSecretUsername),
			commonconstants.KafkaSecretKeyNamespace: []byte(DefaultSecretNamespace),
		},
	}

	// Apply The Specified Eventing-Kafka ConfigMap Options
	for _, option := range options {
		option(secret)
	}

	// Return The Custom Eventing-Kafka Secret
	return secret
}

// WithModifiedPassword Modifies The Default Password Section Of The Secret Data
func WithModifiedPassword(secret *corev1.Secret) {
	secret.Data[commonconstants.KafkaSecretKeyPassword] = []byte(ModifiedSecretPassword)
}

// WithModifiedUsername Modifies The Default Username Section Of The Secret Data
func WithModifiedUsername(secret *corev1.Secret) {
	secret.Data[commonconstants.KafkaSecretKeyUsername] = []byte(ModifiedSecretUsername)
}

// WithEmptyUsername Empties The Default Username Section Of The Secret Data
func WithEmptyUsername(secret *corev1.Secret) {
	secret.Data[commonconstants.KafkaSecretKeyUsername] = []byte("")
}

// WithModifiedSaslType Modifies The Default SaslType Section Of The Secret Data
func WithModifiedSaslType(secret *corev1.Secret) {
	secret.Data[commonconstants.KafkaSecretKeySaslType] = []byte(ModifiedSecretSaslType)
}

// WithModifiedNamespace Modifies The Default Namespace Section Of The Secret Data
func WithModifiedNamespace(secret *corev1.Secret) {
	secret.Data[commonconstants.KafkaSecretKeyNamespace] = []byte(ModifiedSecretNamespace)
}

// WithMissingConfig Removes the Data From The Secret
func WithMissingConfig(secret *corev1.Secret) {
	secret.Data = nil
}
