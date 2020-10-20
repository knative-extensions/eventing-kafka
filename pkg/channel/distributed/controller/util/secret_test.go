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
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
