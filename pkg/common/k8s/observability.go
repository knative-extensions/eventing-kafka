package k8s

import (
	"context"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"knative.dev/pkg/injection/sharedmain"
	"knative.dev/pkg/profiling"
	nethttp "net/http"
)

//
// Initialize The Specified Context With A Profiling Server (ConfigMap Watcher And HTTP Endpoint)
// Much Of This Function Is Taken From sharedmain.Main()
//
func InitializeObservability(logger *zap.SugaredLogger, ctx context.Context, metricsDomain string) {
	logger.Info("EDV:  InitializeObservability, metricsDomain: " + metricsDomain)

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

	// Create A Watcher On The Observability ConfigMap & Dynamically Update Observability Configuration
	cmw := sharedmain.SetupConfigMapWatchOrDie(ctx, logger)

	// Start The Observability ConfigMap Watcher
	sharedmain.WatchObservabilityConfigOrDie(ctx, cmw, profilingHandler, logger, metricsDomain)
	if err := cmw.Start(ctx.Done()); err != nil {
		logger.Fatalw("Failed to start observability configuration manager", zap.Error(err))
	}

	logger.Info("EDV:  ~InitializeObservability")
}
