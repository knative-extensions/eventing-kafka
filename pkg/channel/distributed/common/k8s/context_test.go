package k8s

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"knative.dev/eventing-kafka/pkg/channel/distributed/common/constants"
	"knative.dev/eventing-kafka/pkg/channel/distributed/common/env"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/system"
)

// Test The LoggingContext() Functionality
func TestLoggingContext(t *testing.T) {

	// Test Data
	ctx := context.TODO()
	component := "TestComponent"

	// Setup Environment
	assert.Nil(t, os.Setenv(system.NamespaceEnvKey, constants.KnativeEventingNamespace))
	assert.Nil(t, os.Setenv(env.KnativeLoggingConfigMapNameEnvVarKey, logging.ConfigMapName()))

	// Create A Test Logging ConfigMap
	loggingConfigMap := &corev1.ConfigMap{
		TypeMeta: v1.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: corev1.SchemeGroupVersion.String(),
		},
		ObjectMeta: v1.ObjectMeta{
			Name:      logging.ConfigMapName(),
			Namespace: system.Namespace(),
		},
	}

	// Create The Fake K8S Client
	fakeK8sClient := fake.NewSimpleClientset(loggingConfigMap)

	// Temporarily Swap The K8S Client Wrapper For Testing
	k8sClientWrapperRef := K8sClientWrapper
	K8sClientWrapper = func(masterUrlArg string, kubeconfigPathArg string) kubernetes.Interface {
		assert.Empty(t, masterUrlArg)
		assert.Empty(t, kubeconfigPathArg)
		return fakeK8sClient
	}
	defer func() { K8sClientWrapper = k8sClientWrapperRef }()

	// Perform The Test (Initialize The Logging Context)
	resultContext := LoggingContext(ctx, component, "", "")

	// Verify The Results
	assert.NotNil(t, resultContext)
	assert.Equal(t, fakeK8sClient, kubeclient.Get(resultContext))
	assert.NotNil(t, logging.FromContext(resultContext))

	// Log Something And Wait (Visual Test ; )
	logging.FromContext(ctx).Info("Test Logger")
	time.Sleep(1 * time.Second)
}
