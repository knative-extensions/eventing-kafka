/*
Copyright 2021 The Knative Authors

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

package controller

import (
	"context"

	"go.uber.org/zap"
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/injection"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/system"

	"knative.dev/eventing-kafka/pkg/client/injection/informers/kafka/v1alpha1/resetoffset"
	resetoffsetreconciler "knative.dev/eventing-kafka/pkg/client/injection/reconciler/kafka/v1alpha1/resetoffset"
	"knative.dev/eventing-kafka/pkg/common/commands/resetoffset/refmappers"
	commonconfig "knative.dev/eventing-kafka/pkg/common/config"
)

// NewControllerFactory returns a ControllerConstructor function capable of creating a "typed" ResetOffset Controller
func NewControllerFactory(refMapperFactory refmappers.ResetOffsetRefMapperFactory) injection.ControllerConstructor {

	// Return The New ResetOffset ControllerConstructor Function
	return func(ctx context.Context, cmw configmap.Watcher) *controller.Impl {

		// Get A Logger
		logger := logging.FromContext(ctx)

		// Get The Needed Informers
		resetoffsetInformer := resetoffset.Get(ctx)

		// Create The RefMapper Via The Supplied Factory Using Initialized Context
		refMapper := refMapperFactory.Create(ctx)

		// Create A ResetOffset Reconciler
		reconciler := &Reconciler{
			resetoffsetLister: resetoffsetInformer.Lister(),
			refMapper:         refMapper,
		}

		// Setup Reconciler To Watch The Kafka ConfigMap For Changes
		err := commonconfig.InitializeKafkaConfigMapWatcher(ctx, cmw, logger, reconciler.updateKafkaConfig, system.Namespace())
		if err != nil {
			logger.Fatal("Failed To Initialize ConfigMap Watcher", zap.Error(err))
		}

		// Create A New ResetOffset Controller Impl With The Reconciler
		controllerImpl := resetoffsetreconciler.NewImpl(ctx, reconciler)

		// Configure The Informers' EventHandlers
		logger.Info("Setting Up EventHandlers")
		resetoffsetInformer.Informer().AddEventHandler(controller.HandleAll(controllerImpl.Enqueue))

		// Return The ResetOffset Controller
		return controllerImpl
	}
}

// Shutdown performs clean tear-down of resources.
func Shutdown() {
	// Currently nothing to do
}
