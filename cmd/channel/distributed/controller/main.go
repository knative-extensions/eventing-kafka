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

package main

import (
	// Uncomment the following line to load the gcp plugin (only required to authenticate against GKE clusters).
	// _ "k8s.io/client-go/plugin/pkg/client/auth/gcp"

	"context"

	"go.uber.org/zap"
	ctrlreconciler "knative.dev/control-protocol/pkg/reconciler"
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/injection/sharedmain"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/signals"

	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/constants"
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/env"
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/kafkachannel"
	controllerutil "knative.dev/eventing-kafka/pkg/channel/distributed/controller/util"
	resetoffset "knative.dev/eventing-kafka/pkg/common/commands/resetoffset/controller"
	"knative.dev/eventing-kafka/pkg/common/commands/resetoffset/refmappers"
	"knative.dev/eventing-kafka/pkg/common/configmaploader"
)

// Eventing-Kafka Distributed KafkaChannel Controller Entry Point
func main() {

	// Shutdown / Cleanup Hook For Controllers
	defer kafkachannel.Shutdown()
	defer resetoffset.Shutdown()

	// Setup The Context
	ctx := signals.NewContext()
	logger := logging.FromContext(ctx).Desugar()
	environment, err := env.GetEnvironment(logger)
	if err != nil {
		logger.Fatal("Failed To Load Environment Variables - Terminating!", zap.Error(err))
	}
	ctx = controller.WithResyncPeriod(ctx, environment.ResyncPeriod)
	ctx = context.WithValue(ctx, env.Key{}, environment)
	ctx = context.WithValue(ctx, configmaploader.Key{}, configmap.Load)

	// Create A Subscription RefMapper Factory With Custom Topic/Group Naming
	subscriptionRefMapperFactory := refmappers.NewSubscriptionRefMapperFactory(
		controllerutil.TopicNameMapper,
		controllerutil.GroupIdMapper,
		controllerutil.ConnectionPoolKeyMapper,
		controllerutil.DataPlaneNamespaceMapper,
		controllerutil.DataPlaneLabelsMapper,
	)

	// Create A control-protocol ControlPlaneConnectionPool
	connectionPool := ctrlreconciler.NewInsecureControlPlaneConnectionPool()
	defer connectionPool.Close(ctx)

	// Create A ResetOffset ControllerConstructor Factory With Custom Subscription Ref Mapping
	resetOffsetControllerConstructor := resetoffset.NewControllerFactory(subscriptionRefMapperFactory, connectionPool)

	// Create The SharedMain Instance With The Various Controllers
	sharedmain.MainWithContext(ctx, constants.ControllerComponentName, kafkachannel.NewController, resetOffsetControllerConstructor)
}
