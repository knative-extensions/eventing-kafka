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
	"fmt"

	"go.uber.org/zap"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/cache"
	"knative.dev/eventing/pkg/apis/eventing"
	"knative.dev/eventing/pkg/channel/fanout"
	"knative.dev/eventing/pkg/kncloudevents"
	"knative.dev/pkg/configmap"
	configmapinformer "knative.dev/pkg/configmap/informer"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/injection"
	"knative.dev/pkg/logging"
	pkgreconciler "knative.dev/pkg/reconciler"
	"knative.dev/pkg/tracing"

	"knative.dev/eventing-kafka/pkg/apis/messaging/v1beta1"
	"knative.dev/eventing-kafka/pkg/channel/consolidated/dispatcher"
	"knative.dev/eventing-kafka/pkg/channel/consolidated/utils"
	kafkaclientset "knative.dev/eventing-kafka/pkg/client/clientset/versioned"
	kafkaScheme "knative.dev/eventing-kafka/pkg/client/clientset/versioned/scheme"
	kafkaclientsetinjection "knative.dev/eventing-kafka/pkg/client/injection/client"
	"knative.dev/eventing-kafka/pkg/client/injection/informers/messaging/v1beta1/kafkachannel"
	kafkachannelreconciler "knative.dev/eventing-kafka/pkg/client/injection/reconciler/messaging/v1beta1/kafkachannel"
	listers "knative.dev/eventing-kafka/pkg/client/listers/messaging/v1beta1"
	"knative.dev/eventing-kafka/pkg/common/configmaploader"
	"knative.dev/eventing-kafka/pkg/common/constants"
	kafkasarama "knative.dev/eventing-kafka/pkg/common/kafka/sarama"
)

const dispatcherClientId = "kafka-ch-dispatcher"

func init() {
	// Add run types to the default Kubernetes Scheme so Events can be
	// logged for run types.
	_ = kafkaScheme.AddToScheme(scheme.Scheme)
}

// Reconciler reconciles Kafka Channels.
type Reconciler struct {
	kafkaDispatcher *dispatcher.KafkaDispatcher

	kafkaClientSet       kafkaclientset.Interface
	kafkachannelLister   listers.KafkaChannelLister
	kafkachannelInformer cache.SharedIndexInformer
	impl                 *controller.Impl
}

var _ kafkachannelreconciler.Interface = (*Reconciler)(nil)

// NewController initializes the controller and is called by the generated code.
// Registers event handlers to enqueue events.
func NewController(ctx context.Context, cmw configmap.Watcher) *controller.Impl {
	logger := logging.FromContext(ctx)

	err := tracing.SetupDynamicPublishing(logger, cmw.(*configmapinformer.InformedWatcher), "kafka-ch-dispatcher", "config-tracing")
	if err != nil {
		logger.Fatalw("unable to setup tracing", zap.Error(err))
	}

	// Get the configmap loader
	configmapLoader, err := configmaploader.FromContext(ctx)
	if err != nil {
		logger.Fatal("Failed To Get ConfigmapLoader From Context - Terminating!", zap.Error(err))
	}

	configMap, err := configmapLoader(constants.SettingsConfigMapMountPath)
	if err != nil {
		logger.Fatalw("error loading configuration", zap.Error(err))
	}

	kafkaConfig, err := utils.GetKafkaConfig(ctx, dispatcherClientId, configMap, kafkasarama.LoadAuthConfig)
	if err != nil {
		logger.Fatalw("Error loading kafka config", zap.Error(err))
	}

	// Configure connection arguments - to be done exactly once per process
	kncloudevents.ConfigureConnectionArgs(&kncloudevents.ConnectionArgs{
		MaxIdleConns:        kafkaConfig.EventingKafka.CloudEvents.MaxIdleConns,
		MaxIdleConnsPerHost: kafkaConfig.EventingKafka.CloudEvents.MaxIdleConnsPerHost,
	})

	kafkaChannelInformer := kafkachannel.Get(ctx)
	args := &dispatcher.KafkaDispatcherArgs{
		Brokers:   kafkaConfig.Brokers,
		Config:    kafkaConfig.EventingKafka,
		TopicFunc: utils.TopicName,
	}
	r := &Reconciler{
		kafkaClientSet:       kafkaclientsetinjection.Get(ctx),
		kafkachannelLister:   kafkaChannelInformer.Lister(),
		kafkachannelInformer: kafkaChannelInformer.Informer(),
	}
	r.impl = kafkachannelreconciler.NewImpl(ctx, r, func(impl *controller.Impl) controller.Options {
		return controller.Options{SkipStatusUpdates: true}
	})

	kafkaDispatcher, err := dispatcher.NewDispatcher(ctx, args, r.impl.EnqueueKey)
	if err != nil {
		logger.Fatalw("Unable to create kafka dispatcher", zap.Error(err))
	}
	logger.Info("Starting the Kafka dispatcher")
	logger.Infow("Kafka broker configuration", zap.Strings("Brokers", kafkaConfig.Brokers))

	r.kafkaDispatcher = kafkaDispatcher

	logger.Info("Setting up event handlers")

	// Watch for kafka channels.
	kafkaChannelInformer.Informer().AddEventHandler(
		cache.FilteringResourceEventHandler{
			FilterFunc: filterWithAnnotation(injection.HasNamespaceScope(ctx)),
			Handler: cache.ResourceEventHandlerFuncs{
				AddFunc:    r.impl.Enqueue,
				UpdateFunc: controller.PassNew(r.impl.Enqueue),
				DeleteFunc: func(obj interface{}) {
					// TODO when finalize kind is fixed, we'll need to handle that error properly
					if err := r.CleanupChannel(obj.(*v1beta1.KafkaChannel)); err != nil {
						logger.Warnw("Unable to remove kafka channel", zap.Any("kafkachannel", obj), zap.Error(err))
					}
				},
			},
		})

	logger.Info("Starting dispatcher.")
	go func() {
		if err := kafkaDispatcher.Start(ctx); err != nil {
			logger.Errorw("Cannot start dispatcher", zap.Error(err))
		}
	}()

	return r.impl
}

func filterWithAnnotation(namespaced bool) func(obj interface{}) bool {
	if namespaced {
		return pkgreconciler.AnnotationFilterFunc(eventing.ScopeAnnotationKey, "namespace", false)
	}
	return pkgreconciler.AnnotationFilterFunc(eventing.ScopeAnnotationKey, "cluster", true)
}

func (r *Reconciler) ReconcileKind(ctx context.Context, kc *v1beta1.KafkaChannel) pkgreconciler.Event {
	logging.FromContext(ctx).Debugw("ReconcileKind for channel", zap.String("channel", kc.Name))
	return r.syncChannel(ctx, kc)
}

func (r *Reconciler) ObserveKind(ctx context.Context, kc *v1beta1.KafkaChannel) pkgreconciler.Event {
	logging.FromContext(ctx).Debugw("ObserveKind for channel", zap.String("channel", kc.Name))
	return r.syncChannel(ctx, kc)
}

func (r *Reconciler) syncChannel(ctx context.Context, kc *v1beta1.KafkaChannel) pkgreconciler.Event {
	if !kc.Status.IsReady() {
		logging.FromContext(ctx).Debugw("KafkaChannel still not ready, short-circuiting the reconciler", zap.String("channel", kc.Name))
		return nil
	}

	config := r.newConfigFromKafkaChannel(kc)

	// Update receiver side
	if err := r.kafkaDispatcher.RegisterChannelHost(config); err != nil {
		logging.FromContext(ctx).Error("Error updating host to channel map in dispatcher")
		return err
	}

	// Update dispatcher side
	err := r.kafkaDispatcher.ReconcileConsumers(ctx, config)
	if err != nil {
		logging.FromContext(ctx).Errorw("Some kafka subscriptions failed to subscribe", zap.Error(err))
		return fmt.Errorf("some kafka subscriptions failed to subscribe: %v", err)
	}
	return nil
}

func (r *Reconciler) CleanupChannel(kc *v1beta1.KafkaChannel) pkgreconciler.Event {
	return r.kafkaDispatcher.CleanupChannel(kc.Name, kc.Namespace, kc.Status.Address.URL.Host)
}

// newConfigFromKafkaChannel creates a new Config from the list of kafka channels.
func (r *Reconciler) newConfigFromKafkaChannel(c *v1beta1.KafkaChannel) *dispatcher.ChannelConfig {
	channelConfig := dispatcher.ChannelConfig{
		Namespace: c.Namespace,
		Name:      c.Name,
		HostName:  c.Status.Address.URL.Host,
	}
	if c.Spec.SubscribableSpec.Subscribers != nil {
		newSubs := make([]dispatcher.Subscription, 0, len(c.Spec.SubscribableSpec.Subscribers))
		for _, source := range c.Spec.SubscribableSpec.Subscribers {
			innerSub, _ := fanout.SubscriberSpecToFanoutConfig(source)

			newSubs = append(newSubs, dispatcher.Subscription{
				Subscription: *innerSub,
				UID:          source.UID,
			})
		}
		channelConfig.Subscriptions = newSubs
	}

	return &channelConfig
}
