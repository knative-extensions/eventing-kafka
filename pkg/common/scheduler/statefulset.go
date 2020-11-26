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

package scheduler

import (
	"context"
	"math"
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clientappsv1 "k8s.io/client-go/kubernetes/typed/apps/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/utils/integer"

	kubeclient "knative.dev/pkg/client/injection/kube/client"
	statefulsetinformer "knative.dev/pkg/client/injection/kube/informers/apps/v1/statefulset"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"

	duckv1alpha1 "knative.dev/eventing-kafka/pkg/apis/duck/v1alpha1"
)

type StatefulSetScheduler struct {
	logger            *zap.SugaredLogger
	statefulSetName   string
	statefulSetClient clientappsv1.StatefulSetInterface
	lock              sync.Locker

	// replicas is the number of statefulset replicas
	replicas int32

	// placements is a snapshot of current placements for all schedulable
	placements map[types.UID][]duckv1alpha1.Placement

	// free tracks the free capacity of each pod. Incrementally derived from placements.
	free map[string]int32

	// capacity is the total number of slots available per pod.
	capacity int32
}

func NewStatefulSetScheduler(ctx context.Context, namespace, name string) Scheduler {
	scheduler := &StatefulSetScheduler{
		logger:            logging.FromContext(ctx),
		statefulSetName:   name,
		statefulSetClient: kubeclient.Get(ctx).AppsV1().StatefulSets(namespace),
		capacity:          10, // TODO: from annotation on statefulset, configmap?
		lock:              new(sync.Mutex),
		placements:        make(map[types.UID][]duckv1alpha1.Placement),
		free:              make(map[string]int32),
	}

	// Monitor our statefulset
	statefulsetInformer := statefulsetinformer.Get(ctx)
	statefulsetInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: controller.FilterWithNameAndNamespace(namespace, name),
		Handler:    controller.HandleAll(scheduler.updateStatefulset),
	})

	// Start the autoscaler. Eventually we may want to rely on HPA + custom metrics
	// in particular to handle fluctuations
	go scheduler.autoscale(ctx)

	return scheduler
}

func (s *StatefulSetScheduler) Init(listerFn SchedulableLister) error {
	schedulables, err := listerFn()
	if err != nil {
		return err
	}

	for _, schedulable := range schedulables {
		ps := schedulable.GetPlacements()
		s.placements[schedulable.GetId()] = CopyPlacements(ps)

		for i := 0; i < len(ps); i++ {
			podName := ps[i].PodName
			replicas := ps[i].Replicas

			if _, ok := s.free[podName]; !ok {
				s.free[podName] = s.capacity
			}
			s.free[podName] -= replicas

			if s.free[podName] < 0 {
				// Over committed. The autoscaler will fix it.
				s.logger.Infow("pod is over committed", zap.String("podName", podName))
			}
		}
	}

	return nil
}

func (s *StatefulSetScheduler) Schedule(schedulable Schedulable) ([]duckv1alpha1.Placement, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.logger.Info("scheduling")

	if s.replicas == 0 {
		s.logger.Info("scheduling failed (no replicas available)")
		return nil, ErrUnschedulable
	}

	// For now keep it simple:
	// - if the schedulable is already placed, allocates as many replicas as possible to the same pods
	// - Then starting for the first pod allocates many replicas as possible.
	placements := CopyPlacements(schedulable.GetPlacements())

	// Exact number of replicas => do nothing
	tr := GetTotalReplicas(placements)
	if tr == schedulable.GetReplicas() {
		s.logger.Info("scheduling succeeded (already scheduled)")

		// Fully placed. Nothing to do
		return placements, nil
	}

	// Need less => scale down
	if tr > schedulable.GetReplicas() {
		s.logger.Infow("scaling down", zap.Int32("replicas", tr), zap.Int32("new replicas", schedulable.GetReplicas()))
		s.removeReplicas(schedulable, tr-schedulable.GetReplicas(), placements)
		return placements, nil
	}

	// Need more => scale up
	s.logger.Infow("scaling up", zap.Int32("replicas", tr), zap.Int32("new replicas", schedulable.GetReplicas()))
	placements, left := s.addReplicas(schedulable, schedulable.GetReplicas()-tr, placements)
	if left > 0 {
		// Give time for the autoscaler to do its job
		s.logger.Info("scheduling failed (not enough pod replicas)")
		return nil, ErrUnschedulable
	}

	s.logger.Infow("scheduling successful", zap.Any("placement", placements))
	return placements, nil
}

func (s *StatefulSetScheduler) removeReplicas(schedulable Schedulable, diff int32, placements []duckv1alpha1.Placement) {
	for i := 0; i < len(placements); i++ {
		p := placements[i]
		remove := integer.Int32Min(p.Replicas, diff)
		p.Replicas -= remove
		diff -= remove

		s.free[p.PodName] += remove

		if diff == 0 {
			break
		}
	}

	s.placements[schedulable.GetId()] = placements
}

func (s *StatefulSetScheduler) addReplicas(schedulable Schedulable, diff int32, placements []duckv1alpha1.Placement) ([]duckv1alpha1.Placement, int32) {
	// Pod affinity algorithm: prefer adding replicas to existing pods before considering other replicas
	// In the future, we might want to spread replicas across pods in different regions. But not now

	// Only update cache when adding replicas succeeded.
	delayedFrees := make([]duckv1alpha1.Placement, 10)

	for i := 0; i < len(placements); i++ {
		podName := placements[i].PodName

		free := s.free[podName]
		if free > 0 {
			add := integer.Int32Min(free, diff)

			placements[i].Replicas += add

			delayedFrees = append(delayedFrees, duckv1alpha1.Placement{PodName: podName, Replicas: add})
			diff -= add

			if diff == 0 {
				break
			}
		}
	}

	if diff > 0 {
		// Needs to allocate more replicas. Start from the first statefulset pod
		for i := int32(0); i < s.replicas; i++ {
			podName := s.podNameFromOrdinal(i)

			if _, ok := s.free[podName]; !ok {
				s.free[podName] = s.capacity
			}

			free := s.free[podName]
			if free > 0 {
				add := integer.Int32Min(free, diff)

				placements = append(placements, duckv1alpha1.Placement{
					PodName:  podName,
					Replicas: add,
				})

				delayedFrees = append(delayedFrees, duckv1alpha1.Placement{PodName: podName, Replicas: add})
				diff -= add

				if diff == 0 {
					break
				}
			}
		}
	}

	if diff == 0 {
		// Success: apply frees
		for i := 0; i < len(delayedFrees); i++ {
			s.free[delayedFrees[i].PodName] -= delayedFrees[i].Replicas
		}
		return placements, 0
	}

	// Failed.
	// TODO: consider partial allocation
	return nil, diff
}

func (s *StatefulSetScheduler) updateStatefulset(obj interface{}) {
	// Update the internal cache and evict schedulable if needed
	statefulset, ok := obj.(*appsv1.StatefulSet)
	if !ok {
		s.logger.Fatalw("expected a Statefulset object", zap.Any("object", obj))
	}

	s.lock.Lock()
	defer s.lock.Unlock()
	if s.replicas < statefulset.Status.Replicas {
		// TODO: Evict scheduled resources on deleted replicas
	} else if s.replicas > statefulset.Status.Replicas {
		// noop: controller will retry to schedule resources
	}

	s.replicas = statefulset.Status.Replicas

	for i := int32(0); i < s.replicas; i++ {
		podName := s.podNameFromOrdinal(i)
		if _, ok := s.free[podName]; !ok {
			s.free[podName] = s.capacity
		}
	}

	s.logger.Infow("updated statefulset replicas", zap.Int32("replicas", s.replicas))
}

func (s *StatefulSetScheduler) podNameFromOrdinal(ordinal int32) string {
	return s.statefulSetName + "-" + strconv.Itoa(int(ordinal))
}

// freeCapacity returns the free capacity across all pods
func (s *StatefulSetScheduler) freeCapacity() int32 {
	t := int32(0)
	for i := int32(0); i < s.replicas; i++ {
		t += s.free[s.podNameFromOrdinal(i)]
	}
	return t
}

// usedCapacity returns the used capacity across all pods
func (s *StatefulSetScheduler) usedCapacity() int32 {
	return s.capacity*s.replicas - s.freeCapacity()
}

// avgUsedCapacityPerPod returns the average used capacity per replica
func (s *StatefulSetScheduler) avgUsedCapacityPerPod() float64 {
	return float64(s.usedCapacity()) / float64(s.replicas)
}

func (s *StatefulSetScheduler) autoscale(ctx context.Context) {
	// Regularly compute the average number of replicas per pods.
	// When the average goes above a certain ratio, scale up
	// otherwise scale down.

	for {
		func() {
			s.lock.Lock()
			defer s.lock.Unlock()

			s.logger.Infow("checking adapter capacity",
				zap.Int32("free", s.freeCapacity()),
				zap.Int32("used", s.usedCapacity()),
				zap.Int32("replicas", s.replicas))

			ratio := s.avgUsedCapacityPerPod() / float64(s.capacity)

			if ratio > 0.7 || ratio < 0.3 {
				// Scale up when capacity is above 80% (TODO: configurable)
				// Scale down when capacity is below 30% (TODO: configurable)
				s.logger.Infow("autoscaling statefulset", zap.Int("avg used capacity per pod", int(ratio*100.0)))

				scale, err := s.statefulSetClient.GetScale(ctx, s.statefulSetName, metav1.GetOptions{})
				if err != nil {
					// skip beat
					return
				}

				// Desired ratio is 0.5 (TODO: configurable)
				scale.Spec.Replicas = int32(math.Ceil(float64(s.usedCapacity()) / (float64(s.capacity) * 0.5)))

				s.logger.Infow("updating adapter replicas", zap.Int32("replicas", scale.Spec.Replicas))

				_, err = s.statefulSetClient.UpdateScale(ctx, s.statefulSetName, scale, metav1.UpdateOptions{})
				if err != nil {
					s.logger.Errorw("updating scale subresource failed", zap.Error(err))
				}
			}
		}()

		time.Sleep(10 * time.Second)
	}
}
