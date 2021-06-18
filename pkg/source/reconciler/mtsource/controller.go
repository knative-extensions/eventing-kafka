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

package mtsource

import (
	"context"
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/kelseyhightower/envconfig"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/types"
	"knative.dev/pkg/apis/duck"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/resolver"
	"knative.dev/pkg/system"

	"knative.dev/eventing/pkg/reconciler/source"

	duckv1alpha1 "knative.dev/eventing-kafka/pkg/apis/duck/v1alpha1"
	sourcesv1beta1 "knative.dev/eventing-kafka/pkg/apis/sources/v1beta1"
	kafkaclient "knative.dev/eventing-kafka/pkg/client/injection/client"
	kafkainformer "knative.dev/eventing-kafka/pkg/client/injection/informers/sources/v1beta1/kafkasource"
	"knative.dev/eventing-kafka/pkg/client/injection/reconciler/sources/v1beta1/kafkasource"
	scheduler "knative.dev/eventing-kafka/pkg/common/scheduler"
	stsscheduler "knative.dev/eventing-kafka/pkg/common/scheduler/statefulset"
	nodeinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/node"
)

type envConfig struct {
	SchedulerRefreshPeriod int64                            `envconfig:"AUTOSCALER_REFRESH_PERIOD" required:"true"`
	PodCapacity            int32                            `envconfig:"POD_CAPACITY" required:"true"`
	SchedulerPolicy        stsscheduler.SchedulerPolicyType `envconfig:"SCHEDULER_POLICY_TYPE" required:"true"`
}

func NewController(
	ctx context.Context,
	cmw configmap.Watcher,
) *controller.Impl {
	logger := logging.FromContext(ctx)

	env := &envConfig{}
	if err := envconfig.Process("", env); err != nil {
		logger.Panicf("unable to process required environment variables: %v", err)
	}

	kafkaInformer := kafkainformer.Get(ctx)
	nodeInformer := nodeinformer.Get(ctx)

	c := &Reconciler{
		KubeClientSet:  kubeclient.Get(ctx),
		kafkaClientSet: kafkaclient.Get(ctx),
		kafkaLister:    kafkaInformer.Lister(),
		configs:        source.WatchConfigurations(ctx, component, cmw),
	}

	impl := kafkasource.NewImpl(ctx, c)

	c.sinkResolver = resolver.NewURIResolver(ctx, impl.EnqueueKey)

	// Use a different set of conditions
	sourcesv1beta1.RegisterAlternateKafkaConditionSet(sourcesv1beta1.KafkaMTSourceCondSet)

	rp := time.Duration(env.SchedulerRefreshPeriod) * time.Second
	evictor := func(vpod scheduler.VPod, from *duckv1alpha1.Placement) error {
		key := vpod.GetKey()
		sources := kafkaclient.Get(ctx).SourcesV1beta1().KafkaSources(key.Namespace)

		before, err := sources.Get(ctx, key.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}

		after := before.DeepCopy()

		bp := after.GetPlacements()
		ap := make([]duckv1alpha1.Placement, 0, len(bp)-1)
		for _, p := range bp {
			if p.PodName != from.PodName {
				ap = append(ap, p)
			}
		}
		after.Status.Placement = ap

		jsonPatch, err := duck.CreatePatch(before, after)
		if err != nil {
			return err
		}

		// If there is nothing to patch, we are good, just return.
		// Empty patch is [], hence we check for that.
		if len(jsonPatch) == 0 {
			return nil
		}

		patch, err := jsonPatch.MarshalJSON()
		if err != nil {
			return fmt.Errorf("marshaling JSON patch: %w", err)
		}

		patched, err := kafkaclient.Get(ctx).SourcesV1beta1().KafkaSources(key.Namespace).Patch(ctx, key.Name, types.JSONPatchType, patch, metav1.PatchOptions{}, "status")
		if err != nil {
			return fmt.Errorf("Failed patching: %w", err)
		}
		logging.FromContext(ctx).Debugw("Patched resource", zap.Any("patch", patch), zap.Any("patched", patched))
		return nil
	}

	c.scheduler = stsscheduler.NewScheduler(ctx,
		system.Namespace(), mtadapterName, c.vpodLister, rp, env.PodCapacity, env.SchedulerPolicy,
		nodeInformer.Lister(), evictor)

	logging.FromContext(ctx).Info("Setting up kafka event handlers")
	kafkaInformer.Informer().AddEventHandler(controller.HandleAll(impl.Enqueue))

	return impl
}
