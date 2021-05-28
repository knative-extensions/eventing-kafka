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

package controller

import (
	"context"

	"github.com/kelseyhightower/envconfig"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"

	"knative.dev/eventing-kafka/pkg/apis/messaging/v1beta1"
	"knative.dev/eventing-kafka/pkg/channel/consolidated/status"
	kafkamessagingv1beta1 "knative.dev/eventing-kafka/pkg/client/informers/externalversions/messaging/v1beta1"
	kafkaChannelClient "knative.dev/eventing-kafka/pkg/client/injection/client"
	"knative.dev/eventing-kafka/pkg/client/injection/informers/messaging/v1beta1/kafkachannel"
	kafkaChannelReconciler "knative.dev/eventing-kafka/pkg/client/injection/reconciler/messaging/v1beta1/kafkachannel"
	commonconfig "knative.dev/eventing-kafka/pkg/common/config"
	eventingduckv1 "knative.dev/eventing/pkg/apis/duck/v1"
	eventingClient "knative.dev/eventing/pkg/client/injection/client"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/client/injection/kube/informers/apps/v1/deployment"
	endpointsinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/endpoints"
	podinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/pod"
	"knative.dev/pkg/client/injection/kube/informers/core/v1/service"
	"knative.dev/pkg/client/injection/kube/informers/core/v1/serviceaccount"
	"knative.dev/pkg/client/injection/kube/informers/rbac/v1/rolebinding"
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"
	knativeReconciler "knative.dev/pkg/reconciler"
	"knative.dev/pkg/system"
)

const (
	channelLabelKey   = "messaging.knative.dev/channel"
	channelLabelValue = "kafka-channel"
	roleLabelKey      = "messaging.knative.dev/role"
	roleLabelValue    = "dispatcher"
)

// NewController initializes the controller and is called by the generated code.
// Registers event handlers to enqueue events.
func NewController(
	ctx context.Context,
	cmw configmap.Watcher,
) *controller.Impl {
	logger := logging.FromContext(ctx)
	kafkaChannelInformer := kafkachannel.Get(ctx)
	deploymentInformer := deployment.Get(ctx)
	endpointsInformer := endpointsinformer.Get(ctx)
	serviceAccountInformer := serviceaccount.Get(ctx)
	roleBindingInformer := rolebinding.Get(ctx)
	serviceInformer := service.Get(ctx)
	podInformer := podinformer.Get(ctx)

	r := &Reconciler{
		systemNamespace:      system.Namespace(),
		KubeClientSet:        kubeclient.Get(ctx),
		kafkaClientSet:       kafkaChannelClient.Get(ctx),
		EventingClientSet:    eventingClient.Get(ctx),
		kafkachannelLister:   kafkaChannelInformer.Lister(),
		kafkachannelInformer: kafkaChannelInformer.Informer(),
		deploymentLister:     deploymentInformer.Lister(),
		serviceLister:        serviceInformer.Lister(),
		endpointsLister:      endpointsInformer.Lister(),
		serviceAccountLister: serviceAccountInformer.Lister(),
		roleBindingLister:    roleBindingInformer.Lister(),
	}

	env := &envConfig{}
	if err := envconfig.Process("", env); err != nil {
		logger.Panicf("unable to process Kafka channel's required environment variables: %v", err)
	}

	r.dispatcherImage = env.Image
	r.dispatcherServiceAccount = env.DispatcherServiceAccount

	impl := kafkaChannelReconciler.NewImpl(ctx, r)

	statusProber := status.NewProber(
		logger.Named("status-manager"),
		NewProbeTargetLister(logger, endpointsInformer.Lister()),
		func(c v1beta1.KafkaChannel, s eventingduckv1.SubscriberSpec) {
			logger.Debugf("Ready callback triggered for channel: %s/%s subscription: %s", c.Namespace, c.Name, string(s.UID))
			impl.EnqueueKey(types.NamespacedName{Namespace: c.Namespace, Name: c.Name})
		},
	)
	r.statusManager = statusProber
	statusProber.Start(ctx.Done())

	// Call GlobalResync on kafkachannels.
	grCh := func(obj interface{}) {
		logger.Debug("Changes detected, doing global resync")
		impl.GlobalResync(kafkaChannelInformer.Informer())
	}

	handleKafkaConfigMapChange := func(ctx context.Context, configMap *corev1.ConfigMap) {
		logger.Info("Configmap is updated or, it is being read for the first time")
		r.updateKafkaConfig(ctx, configMap)
		grCh(configMap)
	}

	// Get and Watch the Kakfa config map and dynamically update Kafka configuration.
	err := commonconfig.InitializeKafkaConfigMapWatcher(ctx, cmw, logger, handleKafkaConfigMapChange, system.Namespace())
	if err != nil {
		logger.Fatal("Failed To Initialize ConfigMap Watcher", zap.Error(err))
	}

	logger.Info("Setting up event handlers")
	kafkaChannelInformer.Informer().AddEventHandler(controller.HandleAll(impl.Enqueue))

	// Set up watches for dispatcher resources we care about, since any changes to these
	// resources will affect our Channels. So, set up a watch here, that will cause
	// a global Resync for all the channels to take stock of their health when these change.
	filterFn := controller.FilterWithName(dispatcherName)

	deploymentInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: filterFn,
		Handler:    controller.HandleAll(grCh),
	})
	serviceInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: filterFn,
		Handler:    controller.HandleAll(grCh),
	})
	endpointsInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: filterFn,
		Handler:    controller.HandleAll(grCh),
	})
	serviceAccountInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: filterFn,
		Handler:    controller.HandleAll(grCh),
	})
	roleBindingInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: filterFn,
		Handler:    controller.HandleAll(grCh),
	})

	podInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: knativeReconciler.ChainFilterFuncs(
			knativeReconciler.LabelFilterFunc(channelLabelKey, channelLabelValue, false),
			knativeReconciler.LabelFilterFunc(roleLabelKey, roleLabelValue, false),
		),
		Handler: cache.ResourceEventHandlerFuncs{
			// Cancel probing when a Pod is deleted
			DeleteFunc: getPodInformerEventHandler(ctx, logger, statusProber, impl, kafkaChannelInformer, "Delete"),
			AddFunc:    getPodInformerEventHandler(ctx, logger, statusProber, impl, kafkaChannelInformer, "Add"),
		},
	})

	return impl
}

func getPodInformerEventHandler(ctx context.Context, logger *zap.SugaredLogger, statusProber *status.Prober, impl *controller.Impl, kafkaChannelInformer kafkamessagingv1beta1.KafkaChannelInformer, handlerType string) func(obj interface{}) {
	return func(obj interface{}) {
		pod, ok := obj.(*corev1.Pod)
		if ok && pod != nil {
			logger.Debugw("%s pods. Refreshing pod probing.", handlerType,
				zap.String("pod", pod.GetName()))
			statusProber.RefreshPodProbing(ctx)
			impl.GlobalResync(kafkaChannelInformer.Informer())
		}
	}
}
