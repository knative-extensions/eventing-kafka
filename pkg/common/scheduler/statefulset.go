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
	"errors"
	"strconv"
	"sync"

	duckv1alpha1 "knative.dev/eventing-kafka/pkg/apis/duck/v1alpha1"

	"knative.dev/pkg/logging"

	"go.uber.org/zap"
	appsv1 "k8s.io/api/apps/v1"

	"k8s.io/client-go/tools/cache"
	"knative.dev/pkg/controller"

	statefulsetinformer "knative.dev/pkg/client/injection/kube/informers/apps/v1/statefulset"
)

type StatefulSetScheduler struct {
	logger          *zap.SugaredLogger
	statefulSetName string

	// replicas is the number of statefulset replicas
	replicas int32

	// free tracks the free pod capacity
	free map[string]int32

	lock sync.Locker

	// capacity is the total number of slots available per pod
	capacity int32
}

func NewStatefulSetScheduler(ctx context.Context, namespace, name string) Scheduler {
	scheduler := &StatefulSetScheduler{
		logger:          logging.FromContext(ctx),
		statefulSetName: name,
		capacity:        10, // TODO: from annotation on statefulset, configmap?
		lock:            &sync.Mutex{},
		free:            nil,
	}

	statefulsetInformer := statefulsetinformer.Get(ctx)
	statefulsetInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: controller.FilterWithNameAndNamespace(namespace, name),
		Handler:    controller.HandleAll(scheduler.updateStatefulset),
	})

	return scheduler
}

func (s *StatefulSetScheduler) EnsureInitialized(lister SchedulableLister) error {

	// Keep track of free capacity
	if s.free == nil {
		s.free = make(map[string]int32, s.replicas)
		for i := int32(0); i < s.replicas; i++ {
			s.free[s.statefulSetName+"-"+strconv.Itoa(int(i))] = s.capacity
		}

		schedulales, err := lister()
		if err != nil {
			return err
		}

		for _, schedulale := range schedulales {
			for _, p := range schedulale.GetPlacements() {

				if _, ok := s.free[p.PodName]; !ok {
					s.logger.Fatal()
				}

				s.free[p.PodName] -= p.Replicas

				// Sanity check
				if s.free[p.PodName] < 0 {
					return errors.New("inconsistent scheduler internal state")
				}
			}
		}
	}

	return nil
}

func (s *StatefulSetScheduler) Schedule(schedulable Schedulable) []duckv1alpha1.Placement {
	s.logger.Info("scheduling")
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.replicas == 0 {
		s.logger.Info("scheduling failed (no replicas available)")

		schedulable.MarkAllUnschedulable()
		return nil
	}

	// For now keep it simple:
	// - if the schedulable is already placed, allocates as many replicas as possible to the same pods
	// - Then starting for the first pod allocates many replicas as possible.
	placements := schedulable.GetPlacements()

	tr := GetTotalReplicas(placements)
	if tr == schedulable.GetReplicas() {
		s.logger.Info("scheduling succeeded (already scheduled)")

		// Fully placed. Nothing to do
		return placements
	}

	if tr > schedulable.GetReplicas() {
		s.logger.Infow("scaling down", zap.Int32("replicas", tr), zap.Int32("new replicas", schedulable.GetReplicas()))
		s.removeReplicas(schedulable, tr-schedulable.GetReplicas(), placements)
		return placements
	}

	// scale up
	s.logger.Infow("scaling up", zap.Int32("replicas", tr), zap.Int32("new replicas", schedulable.GetReplicas()))
	placements, left := s.addReplicas(schedulable, schedulable.GetReplicas()-tr, placements)
	if left > 0 {
		s.logger.Info("scheduling failed (not enough pod replicas)")
		schedulable.MarkAllUnschedulable()
		return placements
	}

	s.logger.Infow("scheduling successful", zap.Any("placement", placements))
	return placements
}

func (s *StatefulSetScheduler) removeReplicas(schedulable Schedulable, diff int32, placements []duckv1alpha1.Placement) {
	for i := 0; i < len(placements); i++ {
		remove := Min(placements[i].Replicas, diff)
		placements[i].Replicas -= remove
		diff -= remove

		if diff == 0 {
			break
		}
	}
}

func (s *StatefulSetScheduler) addReplicas(schedulable Schedulable, diff int32, placements []duckv1alpha1.Placement) ([]duckv1alpha1.Placement, int32) {
	// By default, try first to add replicas to existing pods
	// In the future, we might want to spread replicas across pods in different regions. But not now

	for i := 0; i < len(placements); i++ {
		free := s.free[placements[i].PodName]
		if free > 0 {
			add := Min(free, diff)

			placements[i].Replicas += add
			s.free[placements[i].PodName] -= add

			diff -= add

			if diff == 0 {
				break
			}
		}
	}

	if diff > 0 {
		// Needs to allocate more replicas. Start from the first statefulset pod
		for i := int32(0); i < s.replicas; i++ {
			podName := s.statefulSetName + "-" + strconv.Itoa(int(i))
			free := s.free[podName]
			if free > 0 {
				add := Min(free, diff)

				placements = append(placements, duckv1alpha1.Placement{
					PodName:  podName,
					Replicas: add,
				})

				diff -= add

				if diff == 0 {
					break
				}
			}
		}
	}

	return placements, diff
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
		// Evict schedulable on deleted replicas
	} else if s.replicas > statefulset.Status.Replicas {
		// Schedule unschedulables

	}

	s.replicas = statefulset.Status.Replicas

	s.logger.Infow("updated statefulset replicas", zap.Int32("replicas", s.replicas))
}
