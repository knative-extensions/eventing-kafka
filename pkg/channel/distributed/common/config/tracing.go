package config

import (
	"context"
	"errors"

	"go.uber.org/zap"
	"k8s.io/client-go/kubernetes"
	injectionclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/system"
	"knative.dev/pkg/tracing"
	tracingconfig "knative.dev/pkg/tracing/config"
)

//
// Initialize The Specified Context With A Tracer (ConfigMap Watcher)
// Assumes ctx Has K8S Client Injected Via LoggingContext (Or Similar)
//
func InitializeTracing(logger *zap.SugaredLogger, ctx context.Context, service string) error {
	k8sClient, ok := ctx.Value(injectionclient.Key{}).(kubernetes.Interface)
	if !ok {
		err := errors.New("error getting kubernetes client from context (invalid interface)")
		logger.Error("Kubernetes Client Not Present In Context", zap.Error(err))
		return err
	}

	// Create A Watcher On The Tracing ConfigMap & Dynamically Update Tracing Configuration
	cmw := configmap.NewInformedWatcher(k8sClient, system.Namespace())

	if err := tracing.SetupDynamicPublishing(logger, cmw, service, tracingconfig.ConfigName); err != nil {
		logger.Error("Error setting up dynamic trace publishing", zap.Error(err))
		return err
	}

	// Start The Tracing ConfigMap Watcher
	if err := cmw.Start(ctx.Done()); err != nil {
		logger.Error("Failed To Start ConfigMap Watcher", zap.Error(err))
		return err
	}

	return nil
}
