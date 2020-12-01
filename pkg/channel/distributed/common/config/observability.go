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
	nethttp "net/http"
	"strings"

	"knative.dev/pkg/configmap"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/injection/sharedmain"
	"knative.dev/pkg/metrics"
	"knative.dev/pkg/profiling"
)

var ListenAndServeWrapper = func(srv *nethttp.Server) func() error { return srv.ListenAndServe }
var StartWatcherWrapper = func(cmw *configmap.InformedWatcher, done <-chan struct{}) error { return cmw.Start(done) }
var UpdateExporterWrapper = metrics.UpdateExporter

//
// Initialize The Specified Context With A Profiling Server (ConfigMap Watcher And HTTP Endpoint)
// Much Of This Function Is Taken From The knative.dev sharedmain Package
//
func InitializeObservability(ctx context.Context, logger *zap.SugaredLogger, metricsDomain string, metricsPort int, namespace string) error {

	// Initialize the profiling server
	// Taken from knative.dev/pkg/injection/sharedmain/main.go::MainWithConfig
	profilingHandler := profiling.NewHandler(logger, false)
	profilingServer := profiling.NewServer(profilingHandler)
	eg, egCtx := errgroup.WithContext(ctx)
	eg.Go(ListenAndServeWrapper(profilingServer))
	go func() {
		// This will block until either a signal arrives or one of the grouped functions
		// returns an error.
		<-egCtx.Done()

		err := profilingServer.Shutdown(context.Background())
		if err != nil {
			logger.Error("Error while shutting down profiling server", zap.Error(err))
		}
		if err = eg.Wait(); err != nil && err != nethttp.ErrServerClosed {
			logger.Error("Error while running server", zap.Error(err))
		}
	}()

	// Since these functions are designed to be called by the main() function, the default KNative package
	// behavior here is a fatal exit if the stats or watch cannot be set up.

	// Initialize the memory stats, which will be added to the eventing metrics exporter every 30 seconds
	metrics.MemStatsOrDie(ctx)

	// Create A Watcher On The Observability ConfigMap & Dynamically Update Observability Configuration
	cmw := sharedmain.SetupConfigMapWatchOrDie(ctx, logger)

	// Start The Observability ConfigMap Watcher
	// Taken from knative.dev/pkg/injection/sharedmain/main.go::WatchObservabilityConfigOrDie
	// and knative.dev/pkg/metrics/exporter.go::ConfigMapWatcher
	if _, err := kubeclient.Get(ctx).CoreV1().ConfigMaps(namespace).Get(ctx, metrics.ConfigMapName(),
		metav1.GetOptions{}); err == nil {
		cmw.Watch(metrics.ConfigMapName(),
			func(configMap *corev1.ConfigMap) {
				err := UpdateExporterWrapper(ctx, metrics.ExporterOptions{
					Domain:         metrics.Domain(),
					Component:      strings.ReplaceAll(metricsDomain, "-", "_"),
					ConfigMap:      configMap.Data,
					PrometheusPort: metricsPort,
					Secrets:        sharedmain.SecretFetcher(ctx),
				}, logger)
				if err != nil {
					logger.Error("Error during UpdateExporter", zap.Error(err))
				}
			},
			profilingHandler.UpdateFromConfigMap)
	} else if !apierrors.IsNotFound(err) {
		logger.Error("Error reading ConfigMap "+metrics.ConfigMapName(), zap.Error(err))
		return err
	}

	if err := StartWatcherWrapper(cmw, ctx.Done()); err != nil {
		logger.Error("Failed to start observability configuration manager", zap.Error(err))
		return err
	}

	return nil
}
