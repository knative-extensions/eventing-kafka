package config

import (
	"context"
	nethttp "net/http"
	"strings"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/injection/sharedmain"
	"knative.dev/pkg/metrics"
	"knative.dev/pkg/profiling"
	"knative.dev/pkg/system"
)

//
// Initialize The Specified Context With A Profiling Server (ConfigMap Watcher And HTTP Endpoint)
// Much Of This Function Is Taken From The knative.dev sharedmain Package
//
func InitializeObservability(logger *zap.SugaredLogger, ctx context.Context, metricsDomain string, metricsPort int) error {

	// Initialize the profiling server
	// Taken from knative.dev/pkg/injection/sharedmain/main.go::MainWithConfig
	profilingHandler := profiling.NewHandler(logger, false)
	profilingServer := profiling.NewServer(profilingHandler)
	eg, egCtx := errgroup.WithContext(ctx)
	eg.Go(profilingServer.ListenAndServe)
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
	sharedmain.MemStatsOrDie(ctx)

	// Create A Watcher On The Observability ConfigMap & Dynamically Update Observability Configuration
	cmw := sharedmain.SetupConfigMapWatchOrDie(ctx, logger)

	// Start The Observability ConfigMap Watcher
	// Taken from knative.dev/pkg/injection/sharedmain/main.go::WatchObservabilityConfigOrDie
	// and knative.dev/pkg/metrics/exporter.go::ConfigMapWatcher
	if _, err := kubeclient.Get(ctx).CoreV1().ConfigMaps(system.Namespace()).Get(metrics.ConfigMapName(),
		metav1.GetOptions{}); err == nil {
		cmw.Watch(metrics.ConfigMapName(),
			func(configMap *corev1.ConfigMap) {
				err := metrics.UpdateExporter(metrics.ExporterOptions{
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

	if err := cmw.Start(ctx.Done()); err != nil {
		logger.Error("Failed to start observability configuration manager", zap.Error(err))
		return err
	}

	return nil
}
