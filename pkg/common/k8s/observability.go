package k8s

import (
	"context"
	"go.opencensus.io/stats/view"
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
	nethttp "net/http"
	"strings"
	"time"
)

//
// Initialize The Specified Context With A Profiling Server (ConfigMap Watcher And HTTP Endpoint)
// Much Of This Function Is Taken From The knative.dev sharedmain Package
//
func InitializeObservability(logger *zap.SugaredLogger, ctx context.Context, metricsDomain string, metricsPort int) {
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

	msp := metrics.NewMemStatsAll()
	msp.Start(ctx, 30*time.Second)
	if err := view.Register(msp.DefaultViews()...); err != nil {
		logger.Fatalf("Error exporting go memstats view: %v", err)
	}

	// Create A Watcher On The Observability ConfigMap & Dynamically Update Observability Configuration
	cmw := sharedmain.SetupConfigMapWatchOrDie(ctx, logger)

	// Start The Observability ConfigMap Watcher
	if _, err := kubeclient.Get(ctx).CoreV1().ConfigMaps(system.Namespace()).Get(metrics.ConfigMapName(),
		metav1.GetOptions{}); err == nil {
		cmw.Watch(metrics.ConfigMapName(),
			func(configMap *corev1.ConfigMap) {
				err := metrics.UpdateExporter(metrics.ExporterOptions{
					Domain:    metrics.Domain(),
					Component: strings.ReplaceAll(metricsDomain, "-", "_"),
					ConfigMap: configMap.Data,
					PrometheusPort: metricsPort,
					Secrets:   sharedmain.SecretFetcher(ctx),
				}, logger)
				if err != nil {
					logger.Error("Error during UpdateExporter", zap.Error(err))
				}
			},
			profilingHandler.UpdateFromConfigMap)
	} else if !apierrors.IsNotFound(err) {
		logger.Fatalw("Error reading ConfigMap "+metrics.ConfigMapName(), zap.Error(err))
	}

	if err := cmw.Start(ctx.Done()); err != nil {
		logger.Fatalw("Failed to start observability configuration manager", zap.Error(err))
	}
}
