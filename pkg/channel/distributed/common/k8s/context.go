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

package k8s

import (
	"context"
	"log"

	"go.uber.org/zap"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	configmap "knative.dev/pkg/configmap/informer"
	"knative.dev/pkg/injection/sharedmain"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/system"
)

//
// Initialize The Specified Context With A K8S Client & Logger (ConfigMap Watcher)
//
// Note - This logic represents a stepping stone on our path towards alignment with the Knative eventing-contrib implementations.
//        The Receiver / Dispatcher are not "injected controllers" in the knative-eventing injection framework, but still want to
//        leverage that implementation as much as possible to ease future refactoring.  This will allow us to use the default
//        knative-eventing logging configuration and dynamic updating.  To that end, we are setting up a basic context ourselves
//        that mirrors what the injection framework would have created.
//
func LoggingContext(ctx context.Context, component string, k8sClient kubernetes.Interface) context.Context {
	// Get The Logging Config From Knative SharedMain
	loggingConfig, err := sharedmain.GetLoggingConfig(ctx)
	if err != nil {
		log.Fatalf("Failed To Read/Parse Logging Configuration: %v", err)
	}

	// Create A New Logger From The Logging Config & Add To Context
	logger, atomicLevel := logging.NewLoggerFromConfig(loggingConfig, component)
	ctx = logging.WithLogger(ctx, logger)

	// Create A Watcher On The Logging ConfigMap & Dynamically Update Log Levels
	cmw := configmap.NewInformedWatcher(k8sClient, system.Namespace()) // Note - Have removed cmLabelReqs filtering here.
	if _, err := k8sClient.CoreV1().ConfigMaps(system.Namespace()).Get(ctx, logging.ConfigMapName(), metav1.GetOptions{}); err == nil {
		logger.Info("Setting Logging ConfigMap Watcher")
		cmw.Watch(logging.ConfigMapName(), logging.UpdateLevelFromConfigMap(logger, atomicLevel, component))
	} else if !apierrors.IsNotFound(err) {
		logger.Fatalf("Error Reading Logging ConfigMap %q", logging.ConfigMapName(), zap.Error(err))
	}

	// Start The Logging ConfigMap Watcher
	if err := cmw.Start(ctx.Done()); err != nil {
		logger.Fatalw("Failed To Start ConfigMap Watcher", zap.Error(err))
	}

	// Return The Initialized Context
	return ctx
}
