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
	"fmt"
	"net/url"

	v12 "knative.dev/eventing/pkg/apis/duck/v1"

	"knative.dev/eventing-kafka/pkg/channel/consolidated/status"

	"knative.dev/eventing/pkg/apis/eventing"

	"k8s.io/apimachinery/pkg/types"

	"k8s.io/apimachinery/pkg/util/sets"

	corev1listers "k8s.io/client-go/listers/core/v1"

	"github.com/kelseyhightower/envconfig"
	"go.uber.org/zap"
	commonconfig "knative.dev/eventing-kafka/pkg/common/config"

	"k8s.io/client-go/tools/cache"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/client/injection/kube/informers/apps/v1/deployment"
	endpointsinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/endpoints"

	"knative.dev/pkg/client/injection/kube/informers/core/v1/service"
	"knative.dev/pkg/client/injection/kube/informers/core/v1/serviceaccount"
	"knative.dev/pkg/client/injection/kube/informers/rbac/v1/rolebinding"

	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/system"

	"knative.dev/eventing-kafka/pkg/apis/messaging/v1beta1"
	kafkaChannelClient "knative.dev/eventing-kafka/pkg/client/injection/client"
	"knative.dev/eventing-kafka/pkg/client/injection/informers/messaging/v1beta1/kafkachannel"
	kafkaChannelReconciler "knative.dev/eventing-kafka/pkg/client/injection/reconciler/messaging/v1beta1/kafkachannel"
	eventingClient "knative.dev/eventing/pkg/client/injection/client"
)

type TargetLister struct {
	endpointLister corev1listers.EndpointsLister
}

func (t *TargetLister) ListProbeTargets(ctx context.Context, kc v1beta1.KafkaChannel) ([]status.ProbeTarget, error) {
	scope, ok := kc.Annotations[eventing.ScopeAnnotationKey]
	if !ok {
		scope = scopeCluster
	}

	dispatcherNamespace := system.Namespace()
	if scope == scopeNamespace {
		dispatcherNamespace = kc.Namespace
	}

	// Get the Dispatcher Service Endpoints and propagate the status to the Channel
	// endpoints has the same name as the service, so not a bug.
	eps, err := t.endpointLister.Endpoints(dispatcherNamespace).Get(dispatcherName)
	if err != nil {
		return nil, fmt.Errorf("failed to get internal service: %w", err)
	}
	var readyIPs []string

	for _, sub := range eps.Subsets {
		for _, address := range sub.Addresses {
			readyIPs = append(readyIPs, address.IP)
		}
	}

	if len(readyIPs) == 0 {
		return nil, fmt.Errorf("no gateway pods available")
	}

	u, _ := url.Parse(fmt.Sprintf("http://%s.%s/%s/%s", dispatcherName, dispatcherNamespace, kc.Namespace, kc.Name))

	uls := []*url.URL{u}

	return []status.ProbeTarget{
		{
			PodIPs:  sets.NewString(readyIPs...),
			PodPort: "8081", Port: "8081", URLs: uls,
		},
	}, nil
}

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

	if env.Image == "" {
		logger.Panic("unable to process Kafka channel's required environment variables (missing DISPATCHER_IMAGE)")
	}

	r.dispatcherImage = env.Image

	impl := kafkaChannelReconciler.NewImpl(ctx, r)

	statusProber := status.NewProber(
		logger.Named("status-manager"),
		NewProbeTargetLister(logger, endpointsInformer.Lister()),
		func(c v1beta1.KafkaChannel, s v12.SubscriberSpec) {
			logger.Debugf("Ready callback triggered for channel: %s/%s subscription: %s", c.Namespace, c.Name, string(s.UID))
			impl.EnqueueKey(types.NamespacedName{Namespace: c.Namespace, Name: c.Name})
		},
	)
	r.statusManager = statusProber
	statusProber.Start(ctx.Done())
	// Get and Watch the Kakfa config map and dynamically update Kafka configuration.
	err := commonconfig.InitializeKafkaConfigMapWatcher(ctx, cmw, logger, r.updateKafkaConfig, system.Namespace())
	if err != nil {
		logger.Fatal("Failed To Initialize ConfigMap Watcher", zap.Error(err))
	}

	logger.Info("Setting up event handlers")
	kafkaChannelInformer.Informer().AddEventHandler(controller.HandleAll(impl.Enqueue))

	// Set up watches for dispatcher resources we care about, since any changes to these
	// resources will affect our Channels. So, set up a watch here, that will cause
	// a global Resync for all the channels to take stock of their health when these change.
	filterFn := controller.FilterWithName(dispatcherName)

	// Call GlobalResync on kafkachannels.
	grCh := func(obj interface{}) {
		impl.GlobalResync(kafkaChannelInformer.Informer())
	}

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

	return impl
}

func NewProbeTargetLister(logger *zap.SugaredLogger, lister corev1listers.EndpointsLister) status.ProbeTargetLister {
	tl := TargetLister{
		endpointLister: lister,
	}
	return &tl
}
