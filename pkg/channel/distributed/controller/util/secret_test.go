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

package util

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	clientconstants "knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/constants"
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/constants"
	logtesting "knative.dev/pkg/logging/testing"
)

// Test The SecretLogger() Functionality
func TestSecretLogger(t *testing.T) {

	// Test Logger
	logger := logtesting.TestLogger(t).Desugar()

	// Test Data
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "TestSecretName", Namespace: "TestSecretNamespace"},
	}

	// Perform The Test
	secretLogger := SecretLogger(logger, secret)
	assert.NotNil(t, secretLogger)
	assert.NotEqual(t, logger, secretLogger)
	secretLogger.Info("Testing Secret Logger")
}

// Test The NewSecretOwnerReference() Functionality
func TestNewSecretOwnerReference(t *testing.T) {

	// Test Data
	const secretName = "TestSecretName"
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: secretName},
	}

	// Perform The Test
	controllerRef := NewSecretOwnerReference(secret)

	// Validate Results
	assert.NotNil(t, controllerRef)
	assert.Equal(t, corev1.SchemeGroupVersion.String(), controllerRef.APIVersion)
	assert.Equal(t, constants.SecretKind, controllerRef.Kind)
	assert.Equal(t, secret.ObjectMeta.Name, controllerRef.Name)
	assert.True(t, *controllerRef.BlockOwnerDeletion)
	assert.True(t, *controllerRef.Controller)
}

// Test The GetKafkaSecrets() Functionality
func TestGetKafkaSecret(t *testing.T) {

	// Test Data
	k8sNamespace1 := "TestK8SNamespace1"
	k8sNamespace2 := "TestK8SNamespace2"
	k8sNamespace3 := "TestK8SNamespace3"

	kafkaSecretName1 := "TestKafkaSecretName1"
	kafkaSecretName2 := "TestKafkaSecretName2"
	kafkaSecretName3 := "TestKafkaSecretName3"

	kafkaSecret1 := createKafkaSecret(kafkaSecretName1, k8sNamespace1)
	kafkaSecret2 := createKafkaSecret(kafkaSecretName2, k8sNamespace2)
	kafkaSecret3 := createKafkaSecret(kafkaSecretName3, k8sNamespace2)

	// Get The Test K8S Client
	k8sClient := fake.NewSimpleClientset(kafkaSecret1, kafkaSecret2, kafkaSecret3)

	// Perform The Success Test
	result, err := GetKafkaSecret(context.Background(), k8sClient, k8sNamespace1)
	assert.Nil(t, err)
	assert.Equal(t, kafkaSecret1, result)

	// Perform The Multiple Secrets Error Test
	result, err = GetKafkaSecret(context.Background(), k8sClient, k8sNamespace2)
	assert.NotNil(t, err)
	assert.Nil(t, result)

	// Perform The No Secrets Error Test
	result, err = GetKafkaSecret(context.Background(), k8sClient, k8sNamespace3)
	assert.Nil(t, err)
	assert.Nil(t, result)
}

// Create K8S Kafka Secret With Specified Config
func createKafkaSecret(name string, namespace string) *corev1.Secret {
	return &corev1.Secret{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Secret",
			APIVersion: corev1.SchemeGroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				clientconstants.KafkaSecretLabel: "true",
			},
		},
	}
}
