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

	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"knative.dev/eventing-kafka/pkg/common/scheduler"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/logging"
)

type stateAccessor interface {
	// State returns the current state about placed vpods get
	State() (*state, error)
}

// state provides information about the current scheduling of all vpods
// It is used by for the scheduler and the autoscaler
type state struct {
	// free tracks the free capacity of each pod.
	free []int32

	// lastOrdinal is the ordinal index corresponding to the last statefulset replica
	// with placed vpods.
	lastOrdinal int32

	// Pod capacity.
	capacity int32

	// Number of zones in cluster
	numZones int32

	// Scheduling policy type for placing vreplicas on pods
	schedulerPolicy SchedulerPolicyType

	// Mapping node names of nodes currently in cluster to their zone info
	nodeToZoneMap map[string]string
}

// Free safely returns the free capacity at the given ordinal
func (s *state) Free(ordinal int32) int32 {
	if int32(len(s.free)) <= ordinal {
		return s.capacity
	}
	return s.free[ordinal]
}

// SetFree safely sets the free capacity at the given ordinal
func (s *state) SetFree(ordinal int32, value int32) {
	s.free = grow(s.free, ordinal, s.capacity)
	s.free[int(ordinal)] = value
}

// freeCapacity returns the number of vreplicas that can be used,
// up to the last ordinal
func (s *state) freeCapacity() int32 {
	t := int32(0)
	for i := int32(0); i <= s.lastOrdinal; i++ {
		t += s.free[i]
	}
	return t
}

// stateBuilder reconstruct the state from scratch, by listing vpods
type stateBuilder struct {
	ctx             context.Context
	logger          *zap.SugaredLogger
	vpodLister      scheduler.VPodLister
	capacity        int32
	schedulerPolicy SchedulerPolicyType
}

// newStateBuilder returns a StateAccessor recreating the state from scratch each time it is requested
func newStateBuilder(ctx context.Context, lister scheduler.VPodLister, podCapacity int32, schedulerPolicy SchedulerPolicyType) stateAccessor {
	return &stateBuilder{
		ctx:             ctx,
		logger:          logging.FromContext(ctx),
		vpodLister:      lister,
		capacity:        podCapacity,
		schedulerPolicy: schedulerPolicy,
	}
}

func (s *stateBuilder) State() (*state, error) {
	vpods, err := s.vpodLister()
	if err != nil {
		return nil, err
	}

	free := make([]int32, 0, 256)
	last := int32(-1)

	for _, vpod := range vpods {
		ps := vpod.GetPlacements()

		for i := 0; i < len(ps); i++ {
			podName := ps[i].PodName
			vreplicas := ps[i].VReplicas

			ordinal := ordinalFromPodName(podName)

			free = grow(free, ordinal, s.capacity)

			free[ordinal] -= vreplicas

			if free[ordinal] < 0 {
				// Over committed. The autoscaler will fix it.
				s.logger.Infow("pod is overcommitted", zap.String("podName", podName))
			}

			if ordinal > last && free[ordinal] != s.capacity {
				last = ordinal
			}
		}
	}

	if s.schedulerPolicy == EvenSpread {
		//TODO: need a node watch to see if # nodes/ # zones have gone up or down
		nodes, err := kubeclient.Get(s.ctx).CoreV1().Nodes().List(s.ctx, metav1.ListOptions{})
		if err != nil {
			return nil, err
		}

		nodeToZoneMap := make(map[string]string, len(nodes.Items))
		zoneMap := make(map[string]struct{})
		for i := 0; i < len(nodes.Items); i++ {
			node := nodes.Items[i]
			zoneName, ok := node.GetLabels()[ZoneLabel]
			if !ok {
				continue //ignore node that doesn't have zone info (maybe a test setup or control node)
			}

			nodeToZoneMap[node.Name] = zoneName
			zoneMap[zoneName] = struct{}{}
		}

		return &state{free: free, lastOrdinal: last, capacity: s.capacity, numZones: int32(len(zoneMap)), schedulerPolicy: s.schedulerPolicy, nodeToZoneMap: nodeToZoneMap}, nil

	}
	return &state{free: free, lastOrdinal: last, capacity: s.capacity, schedulerPolicy: s.schedulerPolicy}, nil
}

func grow(slice []int32, ordinal int32, def int32) []int32 {
	l := int32(len(slice))
	diff := ordinal - l + 1

	if diff <= 0 {
		return slice
	}

	for i := int32(0); i < diff; i++ {
		slice = append(slice, def)
	}
	return slice
}
