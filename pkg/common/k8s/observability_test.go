package k8s

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"knative.dev/eventing-kafka/pkg/common/env"
	"knative.dev/eventing-kafka/pkg/common/kafka/constants"
	injectionclient "knative.dev/pkg/client/injection/kube/client"
	logtesting "knative.dev/pkg/logging/testing"
	"knative.dev/pkg/metrics"
	"knative.dev/pkg/system"
)

// Test The InitializeObservability() Functionality
func TestInitializeObservability(t *testing.T) {

	// Test Data
	ctx := context.TODO()
	metricsPort := 9876
	metricsDomain := "eventing-kafka"

	// Obtain a Test Logger (Required By Observability Function)
	logger := logtesting.TestLogger(t)

	// Setup Environment
	assert.Nil(t, os.Setenv(system.NamespaceEnvKey, constants.KnativeEventingNamespace))
	assert.Nil(t, os.Setenv(env.MetricsDomainEnvVarKey, metricsDomain))

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
		Data: map[string]string{
			"metrics.backend-destination": "prometheus",
			"profiling.enable":            "true",
		},
	}

	// Create The Fake K8S Client And Add It To The ConfigMap
	fakeK8sClient := fake.NewSimpleClientset(tracingConfigMap)

	// Add The Fake K8S Client To The Context (Required By InitializeObservability)
	ctx = context.WithValue(ctx, injectionclient.Key{}, fakeK8sClient)

	// Perform The Test (Initialize The Observability Watcher)
	InitializeObservability(logger, ctx, metricsDomain, metricsPort)

	// Verify that the profiling endpoint exists and responds to requests
	assertGet(t, "http://localhost:8008/debug/pprof", 200)
	// Verify that the metrics endpoint exists and responds to requests
	assertGet(t, fmt.Sprintf("http://localhost:%v/metrics", metricsPort), 200)
}

func assertGet(t *testing.T, url string, expected int) {
	var resp *http.Response
	var err error

	// Allow a few tries to give the observability servers a second or two to start up if necessary
	for bailOut := 0; bailOut < 20; bailOut++ {
		if resp, err = http.Get(url); err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	assert.Nil(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, expected, resp.StatusCode)
	err = resp.Body.Close()
	assert.Nil(t, err)
}
