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

package config

import (
	"context"
	"errors"
	"net/http"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opencensus.io/stats/view"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"knative.dev/eventing-kafka/pkg/channel/distributed/common/env"
	commontesting "knative.dev/eventing-kafka/pkg/common/testing"
	injectionclient "knative.dev/pkg/client/injection/kube/client"
	configmap "knative.dev/pkg/configmap/informer"
	logtesting "knative.dev/pkg/logging/testing"
	"knative.dev/pkg/metrics"
	"knative.dev/pkg/system"
)

// Test The InitializeObservability() Functionality
func TestInitializeObservability(t *testing.T) {
	// Test Data
	ctx := context.TODO()
	metricsPort := 9877
	metricsDomain := "eventing-kafka"

	// Obtain a Test Logger (Required By Observability Function)
	logger := logtesting.TestLogger(t)

	// Setup Environment
	commontesting.SetTestEnvironment(t)
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

	// Set up some mocks so that we don't need to actually start local servers to run through
	// the core InitializeObservability functionality

	ListenAndServeWrapperRef := ListenAndServeWrapper
	defer func() { ListenAndServeWrapper = ListenAndServeWrapperRef }()

	StartWatcherWrapperRef := StartWatcherWrapper
	defer func() { StartWatcherWrapper = StartWatcherWrapperRef }()

	UpdateExporterWrapperRef := UpdateExporterWrapper
	defer func() { UpdateExporterWrapper = UpdateExporterWrapperRef }()

	calledStartWatcher := false
	calledUpdateExporter := false

	ListenAndServeWrapper = func(srv *http.Server) func() error {
		return func() error {
			return nil
		}
	}

	UpdateExporterWrapper = func(ctx context.Context, ops metrics.ExporterOptions, logger *zap.SugaredLogger) error {
		calledUpdateExporter = true
		return nil
	}

	StartWatcherWrapper = func(cmw *configmap.InformedWatcher, done <-chan struct{}) error {
		calledStartWatcher = true
		cmw.OnChange(tracingConfigMap)
		return nil
	}

	// Perform The Test (Initialize The Observability Watcher)
	err := InitializeObservability(ctx, logger, metricsDomain, metricsPort, system.Namespace())
	assert.Nil(t, err)

	assert.True(t, calledStartWatcher)
	assert.True(t, calledUpdateExporter)

	// Unregister the views as registering them twice causes a panic
	view.Unregister(metrics.NewMemStatsAll().DefaultViews()...)
	StartWatcherWrapper = func(cmw *configmap.InformedWatcher, done <-chan struct{}) error { return errors.New("failure") }

	// Test error conditions from the watcher
	err = InitializeObservability(ctx, logger, metricsDomain, metricsPort, system.Namespace())
	assert.NotNil(t, err)
}
