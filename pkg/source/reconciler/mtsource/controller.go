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

	"github.com/kelseyhightower/envconfig"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"knative.dev/pkg/apis/duck"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	nodeinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/node"
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/resolver"
	"knative.dev/pkg/system"

	"knative.dev/eventing/pkg/reconciler/source"

	sourcesv1beta1 "knative.dev/eventing-kafka/pkg/apis/sources/v1beta1"
	kafkaclient "knative.dev/eventing-kafka/pkg/client/injection/client"
	kafkainformer "knative.dev/eventing-kafka/pkg/client/injection/informers/sources/v1beta1/kafkasource"
	"knative.dev/eventing-kafka/pkg/client/injection/reconciler/sources/v1beta1/kafkasource"
	duckv1alpha1 "knative.dev/eventing/pkg/apis/duck/v1alpha1"
	scheduler "knative.dev/eventing/pkg/scheduler"
	stsscheduler "knative.dev/eventing/pkg/scheduler/statefulset"
)

type envConfig struct {
	SchedulerRefreshPeriod        int64  `envconfig:"AUTOSCALER_REFRESH_PERIOD" required:"true"`
	PodCapacity                   int32  `envconfig:"POD_CAPACITY" required:"true"`
	VReplicaMPS                   int32  `envconfig:"VREPLICA_LIMITS_MPS" required:"false" default:"-1"`
	MaxEventPerSecondPerPartition int32  `envconfig:"MAX_MPS_PER_PARTITION" required:"false" default:"-1"`
	SchedulerPolicyConfigMap      string `envconfig:"SCHEDULER_CONFIG" required:"true"`
	DeSchedulerPolicyConfigMap    string `envconfig:"DESCHEDULER_CONFIG" required:"true"`
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
		KubeClientSet:                 kubeclient.Get(ctx),
		kafkaClientSet:                kafkaclient.Get(ctx),
		kafkaLister:                   kafkaInformer.Lister(),
		configs:                       source.WatchConfigurations(ctx, component, cmw),
		VReplicaMPS:                   env.VReplicaMPS,
		MaxEventPerSecondPerPartition: env.MaxEventPerSecondPerPartition,
	}

	impl := kafkasource.NewImpl(ctx, c)

	c.sinkResolver = resolver.NewURIResolverFromTracker(ctx, impl.Tracker)

	// Use a different set of conditions
	sourcesv1beta1.RegisterAlternateKafkaConditionSet(sourcesv1beta1.KafkaMTSourceCondSet)

	rp := time.Duration(env.SchedulerRefreshPeriod) * time.Second
	evictor := func(pod *corev1.Pod, vpod scheduler.VPod, from *duckv1alpha1.Placement) error {
		//First, annotate pod as unschedulable
		newPod := pod.DeepCopy()
		annots := newPod.ObjectMeta.Annotations
		if annots == nil {
			annots = make(map[string]string)
		}
		annots[scheduler.PodAnnotationKey] = "true" //scheduling disabled
		newPod.ObjectMeta.Annotations = annots

		updated, err := kubeclient.Get(ctx).CoreV1().Pods(newPod.ObjectMeta.Namespace).Update(ctx, newPod, metav1.UpdateOptions{})
		if err != nil {
			return err
		}
		logging.FromContext(ctx).Debugw("Updated pod to be unschedulable", zap.Any("updated", updated))

		//Second, remove placements on that pod for this vpod
		key := vpod.GetKey()
		sources := kafkaclient.Get(ctx).SourcesV1beta1().KafkaSources(key.Namespace)

		before, err := sources.Get(ctx, key.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}

		statusCond := before.Status.GetCondition(sourcesv1beta1.KafkaConditionScheduled)
		if statusCond.Status == corev1.ConditionTrue && statusCond.Type == sourcesv1beta1.KafkaConditionScheduled { //do not evict when scheduling is in-progress
			logger.Info("evicting vreplica(s)", zap.String("name", key.Name), zap.String("namespace", key.Namespace), zap.String("podname", from.PodName), zap.Int("vreplicas", int(from.VReplicas)))

			after := before.DeepCopy()

			bp := after.GetPlacements()
			ap := make([]duckv1alpha1.Placement, 0, len(bp)-1)
			for _, p := range bp {
				if p.PodName != from.PodName {
					ap = append(ap, p)
				}
			}
			after.Status.Placements = ap

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
				return fmt.Errorf("failed patching: %w", err)
			}
			logging.FromContext(ctx).Debugw("Patched resource", zap.Any("patch", patch), zap.Any("patched", patched))
		}
		return nil
	}

	policy := &scheduler.SchedulerPolicy{}
	if env.SchedulerPolicyConfigMap != "" {
		if err := initPolicyFromConfigMap(ctx, env.SchedulerPolicyConfigMap, policy); err != nil {
			panic(err)
		}
	}

	removalpolicy := &scheduler.SchedulerPolicy{}
	if env.DeSchedulerPolicyConfigMap != "" {
		if err := initPolicyFromConfigMap(ctx, env.DeSchedulerPolicyConfigMap, removalpolicy); err != nil {
			panic(err)
		}
	}

	logging.FromContext(ctx).Debugw("Scheduler Policy Config Map read", zap.Any("policy", policy))
	c.scheduler = stsscheduler.NewScheduler(ctx,
		system.Namespace(),
		mtadapterName,
		c.vpodLister,
		rp,
		env.PodCapacity,
		"", //  scheduler.SchedulerPolicyType field only applicable for old scheduler policy
		nodeInformer.Lister(),
		evictor,
		policy,
		removalpolicy)

	logging.FromContext(ctx).Info("Setting up kafka event handlers")
	kafkaInformer.Informer().AddEventHandler(controller.HandleAll(impl.Enqueue))

	return impl
}
