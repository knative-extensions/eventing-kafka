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

package statefulset

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/types"
	clientappsv1 "k8s.io/client-go/kubernetes/typed/apps/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/utils/integer"

	kubeclient "knative.dev/pkg/client/injection/kube/client"
	statefulsetinformer "knative.dev/pkg/client/injection/kube/informers/apps/v1/statefulset"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"

	duckv1alpha1 "knative.dev/eventing-kafka/pkg/apis/duck/v1alpha1"
	"knative.dev/eventing-kafka/pkg/common/scheduler"
)

// NewScheduler creates a new scheduler with pod autoscaling enabled.
func NewScheduler(ctx context.Context, namespace, name string, lister scheduler.VPodLister) scheduler.Scheduler {
	capacity := int32(10) // TODO: config

	stateAccessor := newStateBuilder(logging.FromContext(ctx), lister, capacity)
	autoscaler := NewAutoscaler(ctx, namespace, name, stateAccessor, 10*time.Second) // TODO: config
	return NewStatefulSetScheduler(ctx, namespace, name, lister, stateAccessor, autoscaler)
}

// StatefulSetScheduler is a scheduler placing VPod into statefulset-managed set of pods
type StatefulSetScheduler struct {
	logger            *zap.SugaredLogger
	statefulSetName   string
	statefulSetClient clientappsv1.StatefulSetInterface
	vpodLister        scheduler.VPodLister
	lock              sync.Locker
	stateAccessor     stateAccessor
	autoscaler        Autoscaler

	// replicas is the (cached) number of statefulset replicas.
	replicas int32

	// pending tracks the number of virtual replicas that haven't been scheduled yet.
	// Used by the autoscaler
	pending map[types.NamespacedName]int32
}

func NewStatefulSetScheduler(ctx context.Context, namespace, name string, lister scheduler.VPodLister, stateAccessor stateAccessor, autoscaler Autoscaler) scheduler.Scheduler {
	scheduler := &StatefulSetScheduler{
		logger:            logging.FromContext(ctx),
		statefulSetName:   name,
		statefulSetClient: kubeclient.Get(ctx).AppsV1().StatefulSets(namespace),
		vpodLister:        lister,
		pending:           make(map[types.NamespacedName]int32),
		lock:              new(sync.Mutex),
		stateAccessor:     stateAccessor,
		autoscaler:        autoscaler,
	}

	// Monitor our statefulset
	statefulsetInformer := statefulsetinformer.Get(ctx)
	statefulsetInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: controller.FilterWithNameAndNamespace(namespace, name),
		Handler:    controller.HandleAll(scheduler.updateStatefulset),
	})

	return scheduler
}

func (s *StatefulSetScheduler) Schedule(vpod scheduler.VPod) ([]duckv1alpha1.Placement, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	logger := s.logger.With("key", zap.Any("key", vpod.GetKey()))
	logger.Info("scheduling")

	// Get the current placements state
	// Quite an expensive operation but safe and simple.
	state, err := s.stateAccessor.State()
	if err != nil {
		logger.Info("error while refreshing scheduler state (will retry)", zap.Error(err))
		return nil, err
	}

	if s.replicas == 0 {
		logger.Info("scheduling failed (no replicas available)")
		s.pending[vpod.GetKey()] = vpod.GetVReplicas()

		// Trigger the autoscaler
		if s.autoscaler != nil {
			s.autoscaler.Autoscale(s.pendingVReplicas())
		}

		return make([]duckv1alpha1.Placement, 0), scheduler.ErrNoReplicas
	}

	placements := vpod.GetPlacements()

	// The scheduler
	// - allocates as many vreplicas as possible to the same pod(s) (TODO: HA)
	// - allocates remaining vreplicas to new pods

	// Exact number of vreplicas => do nothing
	tr := scheduler.GetTotalVReplicas(placements)
	if tr == vpod.GetVReplicas() {
		logger.Info("scheduling succeeded (already scheduled)")
		delete(s.pending, vpod.GetKey())

		// Fully placed. Nothing to do
		return placements, nil
	}

	// Need less => scale down
	if tr > vpod.GetVReplicas() {
		logger.Infow("scaling down", zap.Int32("vreplicas", tr), zap.Int32("new vreplicas", vpod.GetVReplicas()))
		placements := s.removeReplicas(tr-vpod.GetVReplicas(), placements)

		// Do not trigger the autoscaler to avoid churn

		return placements, nil
	}

	// Need more => scale up
	logger.Infow("scaling up", zap.Int32("vreplicas", tr), zap.Int32("new vreplicas", vpod.GetVReplicas()))
	placements, left := s.addReplicas(state, vpod.GetVReplicas()-tr, placements)
	if left > 0 {
		// Give time for the autoscaler to do its job
		logger.Info("scheduling failed (not enough pod replicas)", zap.Any("placement", placements))

		s.pending[vpod.GetKey()] = left

		// Trigger the autoscaler
		if s.autoscaler != nil {
			s.autoscaler.Autoscale(s.pendingVReplicas())
		}

		return placements, scheduler.ErrNotEnoughReplicas
	}

	logger.Infow("scheduling successful", zap.Any("placement", placements))
	delete(s.pending, vpod.GetKey())
	return placements, nil
}

func (s *StatefulSetScheduler) removeReplicas(diff int32, placements []duckv1alpha1.Placement) []duckv1alpha1.Placement {
	newPlacements := make([]duckv1alpha1.Placement, 0, len(placements))
	for i := 0; i < len(placements); i++ {
		if diff >= placements[i].VReplicas {
			// remove the entire placement
			diff -= placements[i].VReplicas
		} else {
			newPlacements = append(newPlacements, duckv1alpha1.Placement{
				PodName:   placements[i].PodName,
				VReplicas: placements[i].VReplicas - diff,
			})
			diff = 0
		}
	}
	return newPlacements
}

func (s *StatefulSetScheduler) addReplicas(state *state, diff int32, placements []duckv1alpha1.Placement) ([]duckv1alpha1.Placement, int32) {
	// Pod affinity algorithm: prefer adding replicas to existing pods before considering other replicas
	// In the future, we might want to spread replicas across pods in different regions.
	newPlacements := make([]duckv1alpha1.Placement, 0, len(placements))

	// Add to existing
	for i := 0; i < len(placements); i++ {
		podName := placements[i].PodName
		ordinal := ordinalFromPodName(podName)

		// Is there space in PodName?
		f := state.Free(ordinal)
		if diff >= 0 && f > 0 {
			allocation := integer.Int32Min(f, diff)
			newPlacements = append(newPlacements, duckv1alpha1.Placement{
				PodName:   podName,
				VReplicas: placements[i].VReplicas + allocation,
			})

			diff -= allocation
			state.SetFree(ordinal, f-allocation)
		} else {
			newPlacements = append(newPlacements, placements[i])
		}
	}

	if diff > 0 {
		// Needs to allocate replicas to additional pods
		for ordinal := int32(0); ordinal < s.replicas; ordinal++ {
			f := state.Free(ordinal)
			if f > 0 {
				allocation := integer.Int32Min(f, diff)
				newPlacements = append(newPlacements, duckv1alpha1.Placement{
					PodName:   podNameFromOrdinal(s.statefulSetName, ordinal),
					VReplicas: allocation,
				})

				diff -= allocation
				state.SetFree(ordinal, f-allocation)
			}

			if diff == 0 {
				break
			}
		}
	}

	return newPlacements, diff
}

// pendingReplicas returns the total number of vreplicas
// that haven't been scheduled yet
func (s *StatefulSetScheduler) pendingVReplicas() int32 {
	t := int32(0)
	for _, v := range s.pending {
		t += v
	}
	return t
}

func (s *StatefulSetScheduler) updateStatefulset(obj interface{}) {
	statefulset, ok := obj.(*appsv1.StatefulSet)
	if !ok {
		s.logger.Fatalw("expected a Statefulset object", zap.Any("object", obj))
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	if statefulset.Spec.Replicas == nil {
		s.replicas = 1
	} else {
		s.replicas = *statefulset.Spec.Replicas
	}

	s.logger.Infow("updated statefulset replicas", zap.Int32("replicas", s.replicas))
}
