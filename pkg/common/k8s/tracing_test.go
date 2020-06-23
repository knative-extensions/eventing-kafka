package k8s

import (
	"context"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"knative.dev/eventing-kafka/pkg/common/kafka/constants"
	injectionclient "knative.dev/pkg/client/injection/kube/client"
	logtesting "knative.dev/pkg/logging/testing"
	"knative.dev/pkg/system"
	tracingconfig "knative.dev/pkg/tracing/config"
	"os"
	"testing"
)

// Test The InitializeTracing() Functionality
func TestInitializeTracing(t *testing.T) {

	// Test Data
	ctx := context.TODO()
	service := "TestService"

	// Obtain a Test Logger (Required By Tracing Function)
	logger := logtesting.TestLogger(t)

	// Setup Environment
	assert.Nil(t, os.Setenv(system.NamespaceEnvKey, constants.KnativeEventingNamespace))

	// Create A Test Tracing ConfigMap For The SetupDynamicPublishing() Call To Watch
	tracingConfigMap := &corev1.ConfigMap{
		TypeMeta: v1.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: corev1.SchemeGroupVersion.String(),
		},
		ObjectMeta: v1.ObjectMeta{
			Name:      tracingconfig.ConfigName,
			Namespace: system.Namespace(),
		},
	}

	// Create The Fake K8S Client And Add It To The ConfigMap
	fakeK8sClient := fake.NewSimpleClientset(tracingConfigMap)

	// Add The Fake K8S Client To The Context (Required By InitializeTracing)
	ctx = context.WithValue(ctx, injectionclient.Key{}, fakeK8sClient)

	// Perform The Test (Initialize The Tracing Watcher)
	InitializeTracing(logger, ctx, service)

	// If the InitializeTracing Succeeds, it will not fatally exit
	// (Not the best test of failure conditions but it does run through the SetupDynamicPublishing() call at least
	//  and verify that the happy-path doesn't error out)
}
