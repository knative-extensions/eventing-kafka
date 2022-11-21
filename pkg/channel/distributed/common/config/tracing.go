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

	"go.uber.org/zap"
	"k8s.io/client-go/kubernetes"
	injectionclient "knative.dev/pkg/client/injection/kube/client"
	configmap "knative.dev/pkg/configmap/informer"
	"knative.dev/pkg/tracing"
	tracingconfig "knative.dev/pkg/tracing/config"
)

// Initialize The Specified Context With A Tracer (ConfigMap Watcher)
// Assumes ctx Has K8S Client Injected Via LoggingContext (Or Similar)
func InitializeTracing(logger *zap.SugaredLogger, ctx context.Context, service string, namespace string) error {
	k8sClient, ok := ctx.Value(injectionclient.Key{}).(kubernetes.Interface)
	if !ok {
		err := errors.New("error getting kubernetes client from context (invalid interface)")
		logger.Error("Kubernetes Client Not Present In Context", zap.Error(err))
		return err
	}

	// Create A Watcher On The Tracing ConfigMap & Dynamically Update Tracing Configuration
	cmw := configmap.NewInformedWatcher(k8sClient, namespace)

	tracer, err := tracing.SetupPublishingWithDynamicConfig(logger, cmw, service, tracingconfig.ConfigName)
	if err != nil {
		logger.Error("Error setting up dynamic trace publishing", zap.Error(err))
		return err
	}
	tracer.Shutdown(context.Background())

	// Start The Tracing ConfigMap Watcher
	if err := cmw.Start(ctx.Done()); err != nil {
		logger.Error("Failed To Start ConfigMap Watcher", zap.Error(err))
		return err
	}

	return nil
}
