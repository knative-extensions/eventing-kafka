package k8s

import (
	"context"
	"go.uber.org/zap"
	"k8s.io/client-go/kubernetes"
	"knative.dev/eventing/pkg/tracing"
	injectionclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/system"
	tracingconfig "knative.dev/pkg/tracing/config"
)

//
// Initialize The Specified Context With A Tracer (ConfigMap Watcher)
// Assumes ctx Has K8S Client Injected Via LoggingContext (Or Similar)
//
func InitializeTracing(logger *zap.SugaredLogger, ctx context.Context, service string) {
	k8sClient, ok := ctx.Value(injectionclient.Key{}).(kubernetes.Interface)
	if !ok {
		logger.Fatalw("Error getting kubernetes client from context (invalid interface)")
	}

	// Create A Watcher On The Tracing ConfigMap & Dynamically Update Tracing Configuration
	cmw := configmap.NewInformedWatcher(k8sClient, system.Namespace())

	if err := tracing.SetupDynamicPublishing(logger, cmw, service, tracingconfig.ConfigName); err != nil {
		logger.Fatalw("Error setting up dynamic trace publishing", zap.Error(err))
	}

	// Start The Tracing ConfigMap Watcher
	if err := cmw.Start(ctx.Done()); err != nil {
		logger.Fatalw("Failed To Start ConfigMap Watcher", zap.Error(err))
	}
}
