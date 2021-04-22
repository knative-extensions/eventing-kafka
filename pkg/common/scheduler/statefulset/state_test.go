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
	"fmt"
	"reflect"
	"testing"

	duckv1alpha1 "knative.dev/eventing-kafka/pkg/apis/duck/v1alpha1"
	tscheduler "knative.dev/eventing-kafka/pkg/common/scheduler/testing"
)

func TestStateBuilder(t *testing.T) {
	testCases := []struct {
		name     string
		vpods    [][]duckv1alpha1.Placement
		expected state
		freec    int32
		err      error
	}{
		{
			name:     "no vpods",
			vpods:    [][]duckv1alpha1.Placement{},
			expected: state{capacity: 10, free: []int32{}, lastOrdinal: -1},
			freec:    int32(0),
		},
		{
			name:     "one vpods",
			vpods:    [][]duckv1alpha1.Placement{{{PodName: "statefulset-name-0", VReplicas: 1}}},
			expected: state{capacity: 10, free: []int32{int32(9)}, lastOrdinal: 0},
			freec:    int32(9),
		},
		{
			name: "many vpods, no gaps",
			vpods: [][]duckv1alpha1.Placement{
				{{PodName: "statefulset-name-0", VReplicas: 1}, {PodName: "statefulset-name-2", VReplicas: 5}},
				{{PodName: "statefulset-name-1", VReplicas: 2}},
				{{PodName: "statefulset-name-1", VReplicas: 3}, {PodName: "statefulset-name-0", VReplicas: 1}},
			},
			expected: state{capacity: 10, free: []int32{int32(8), int32(5), int32(5)}, lastOrdinal: 2},
			freec:    int32(18),
		},
		{
			name: "many vpods, with gaps",
			vpods: [][]duckv1alpha1.Placement{
				{{PodName: "statefulset-name-0", VReplicas: 1}, {PodName: "statefulset-name-2", VReplicas: 5}},
				{{PodName: "statefulset-name-1", VReplicas: 0}},
				{{PodName: "statefulset-name-1", VReplicas: 0}, {PodName: "statefulset-name-3", VReplicas: 0}},
			},
			expected: state{capacity: 10, free: []int32{int32(9), int32(10), int32(5), int32(10)}, lastOrdinal: 2},
			freec:    int32(24),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, _ := setupFakeContext(t)
			vpodClient := tscheduler.NewVPodClient()

			for i, placements := range tc.vpods {
				vpodName := fmt.Sprint("vpod-name-", i)
				vpodNamespace := fmt.Sprint("vpod-ns-", i)

				vpodClient.Create(vpodNamespace, vpodName, 1, placements)
			}

			stateBuilder := newStateBuilder(ctx, vpodClient.List, int32(10), MaxFillup)
			state, err := stateBuilder.State()
			if err != nil {
				t.Fatal("unexpected error", err)
			}

			if !reflect.DeepEqual(*state, tc.expected) {
				t.Errorf("unexpected state, got %v, want %v", state, tc.expected)
			}

			if state.freeCapacity() != tc.freec {
				t.Errorf("unexpected free capacity, got %d, want %d", state.freeCapacity(), tc.freec)
			}
		})
	}
}
