package util

import (
	"knative.dev/eventing-kafka/pkg/controller/constants"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	logtesting "knative.dev/pkg/logging/testing"
	"testing"
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
