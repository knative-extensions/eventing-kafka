/*
Copyright 2019 The Knative Authors

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

package source

import (
	"context"
	"os"

	"k8s.io/client-go/tools/cache"
	ctrlservice "knative.dev/control-protocol/pkg/service"

	"knative.dev/eventing/pkg/apis/sources/v1alpha1"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	deploymentinformer "knative.dev/pkg/client/injection/kube/informers/apps/v1/deployment"
	podinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/pod"
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/resolver"

	ctrlreconciler "knative.dev/control-protocol/pkg/reconciler"

	kafkaclient "knative.dev/eventing-kafka/pkg/client/injection/client"
	kafkainformer "knative.dev/eventing-kafka/pkg/client/injection/informers/sources/v1beta1/kafkasource"
	"knative.dev/eventing-kafka/pkg/client/injection/reconciler/sources/v1beta1/kafkasource"
	kafkasourcecontrol "knative.dev/eventing-kafka/pkg/source/control"
)

func NewController(
	ctx context.Context,
	cmw configmap.Watcher,
) *controller.Impl {

	raImage, defined := os.LookupEnv(raImageEnvVar)
	if !defined {
		logging.FromContext(ctx).Errorf("required environment variable '%s' not defined", raImageEnvVar)
		return nil
	}

	kafkaInformer := kafkainformer.Get(ctx)
	deploymentInformer := deploymentinformer.Get(ctx)
	podInformer := podinformer.Get(ctx)

	c := &Reconciler{
		KubeClientSet:       kubeclient.Get(ctx),
		kafkaClientSet:      kafkaclient.Get(ctx),
		kafkaLister:         kafkaInformer.Lister(),
		deploymentLister:    deploymentInformer.Lister(),
		receiveAdapterImage: raImage,
		loggingContext:      ctx,
		configs:             WatchConfigurations(ctx, component, cmw),
		podIpGetter:         ctrlreconciler.PodIpGetter{Lister: podInformer.Lister()},
		connectionPool: ctrlreconciler.NewInsecureControlPlaneConnectionPool(
			ctrlreconciler.WithServiceWrapper(ctrlservice.WithCachingService(ctx)),
		),
	}

	impl := kafkasource.NewImpl(ctx, c)
	c.sinkResolver = resolver.NewURIResolver(ctx, impl.EnqueueKey)

	c.contractUpdateNotificationStore = ctrlreconciler.NewAsyncCommandNotificationStore(impl.EnqueueKey)
	c.claimsNotificationStore = ctrlreconciler.NewNotificationStore(impl.EnqueueKey, kafkasourcecontrol.ClaimsParser)

	logging.FromContext(ctx).Info("Setting up kafka event handlers")

	kafkaInformer.Informer().AddEventHandler(controller.HandleAll(impl.Enqueue))

	deploymentInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: controller.FilterControllerGK(v1alpha1.Kind("KafkaSource")),
		Handler:    controller.HandleAll(impl.EnqueueControllerOf),
	})

	podInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: controller.FilterControllerGK(v1alpha1.Kind("KafkaSource")),
		Handler:    controller.HandleAll(impl.EnqueueControllerOf),
	})

	return impl
}
