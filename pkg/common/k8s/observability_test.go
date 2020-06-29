package k8s

import (
	"context"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"knative.dev/eventing-kafka/pkg/common/env"
	"knative.dev/eventing-kafka/pkg/common/kafka/constants"
	"knative.dev/eventing-kafka/pkg/controller/test"
	injectionclient "knative.dev/pkg/client/injection/kube/client"
	logtesting "knative.dev/pkg/logging/testing"
	"knative.dev/pkg/metrics"
	"knative.dev/pkg/system"
	"os"
	"testing"
)

// Test The InitializeObservability() Functionality
func TestInitializeObservability(t *testing.T) {

	// Test Data
	ctx := context.TODO()

	// Obtain a Test Logger (Required By Observability Function)
	logger := logtesting.TestLogger(t)

	// Setup Environment
	assert.Nil(t, os.Setenv(system.NamespaceEnvKey, constants.KnativeEventingNamespace))
	assert.Nil(t, os.Setenv(env.MetricsDomainEnvVarKey, test.MetricsDomain))

	// Create A Test Observability ConfigMap For The InitializeObservability() Call To Watch
	tracingConfigMap := &corev1.ConfigMap{
		TypeMeta: v1.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: corev1.SchemeGroupVersion.String(),
		},
		ObjectMeta: v1.ObjectMeta{
			Name:      metrics.ConfigMapName(),
			Namespace: system.Namespace(),
		},
		Data: map[string]string {
			"metrics.backend-destination": "prometheus",
			"profiling.enable": "true",
		},
	}

	// Create The Fake K8S Client And Add It To The ConfigMap
	fakeK8sClient := fake.NewSimpleClientset(tracingConfigMap)

	// Add The Fake K8S Client To The Context (Required By InitializeObservability)
	ctx = context.WithValue(ctx, injectionclient.Key{}, fakeK8sClient)

	// Perform The Test (Initialize The Observability Watcher)
	InitializeObservability(logger, ctx, test.MetricsDomain)

	// If the InitializeObservability Succeeds, it will not fatally exit
	// (Not the best test of failure conditions but it does run through the SetupDynamicPublishing() call at least
	//  and verify that the happy-path doesn't error out)
}
