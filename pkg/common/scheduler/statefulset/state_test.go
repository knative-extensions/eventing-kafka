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

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	duckv1alpha1 "knative.dev/eventing-kafka/pkg/apis/duck/v1alpha1"
	tscheduler "knative.dev/eventing-kafka/pkg/common/scheduler/testing"
	listers "knative.dev/eventing/pkg/reconciler/testing/v1"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
)

func TestStateBuilder(t *testing.T) {
	testCases := []struct {
		name            string
		vpods           [][]duckv1alpha1.Placement
		expected        state
		freec           int32
		schedulerPolicy SchedulerPolicyType
		reserved        map[types.NamespacedName]map[string]int32
		nodes           []*v1.Node
		err             error
	}{
		{
			name:            "no vpods",
			vpods:           [][]duckv1alpha1.Placement{},
			expected:        state{capacity: 10, free: []int32{}, lastOrdinal: -1, schedulerPolicy: MAXFILLUP},
			freec:           int32(0),
			schedulerPolicy: MAXFILLUP,
		},
		{
			name:            "one vpods",
			vpods:           [][]duckv1alpha1.Placement{{{PodName: "statefulset-name-0", VReplicas: 1}}},
			expected:        state{capacity: 10, free: []int32{int32(9)}, lastOrdinal: 0, schedulerPolicy: MAXFILLUP},
			freec:           int32(9),
			schedulerPolicy: MAXFILLUP,
		},
		{
			name: "many vpods, no gaps",
			vpods: [][]duckv1alpha1.Placement{
				{{PodName: "statefulset-name-0", VReplicas: 1}, {PodName: "statefulset-name-2", VReplicas: 5}},
				{{PodName: "statefulset-name-1", VReplicas: 2}},
				{{PodName: "statefulset-name-1", VReplicas: 3}, {PodName: "statefulset-name-0", VReplicas: 1}},
			},
			expected:        state{capacity: 10, free: []int32{int32(8), int32(5), int32(5)}, lastOrdinal: 2, schedulerPolicy: MAXFILLUP},
			freec:           int32(18),
			schedulerPolicy: MAXFILLUP,
		},
		{
			name: "many vpods, with gaps",
			vpods: [][]duckv1alpha1.Placement{
				{{PodName: "statefulset-name-0", VReplicas: 1}, {PodName: "statefulset-name-2", VReplicas: 5}},
				{{PodName: "statefulset-name-1", VReplicas: 0}},
				{{PodName: "statefulset-name-1", VReplicas: 0}, {PodName: "statefulset-name-3", VReplicas: 0}},
			},
			expected:        state{capacity: 10, free: []int32{int32(9), int32(10), int32(5), int32(10)}, lastOrdinal: 2, schedulerPolicy: MAXFILLUP},
			freec:           int32(24),
			schedulerPolicy: MAXFILLUP,
		},
		{
			name: "many vpods, with gaps and reserved vreplicas",
			vpods: [][]duckv1alpha1.Placement{
				{{PodName: "statefulset-name-0", VReplicas: 1}, {PodName: "statefulset-name-2", VReplicas: 5}},
				{{PodName: "statefulset-name-1", VReplicas: 0}},
				{{PodName: "statefulset-name-1", VReplicas: 0}, {PodName: "statefulset-name-3", VReplicas: 0}},
			},
			expected: state{capacity: 10, free: []int32{int32(4), int32(10), int32(5), int32(10)}, lastOrdinal: 2, schedulerPolicy: MAXFILLUP},
			freec:    int32(19),
			reserved: map[types.NamespacedName]map[string]int32{
				{Name: "vpod-name-3", Namespace: testNs}: {
					"statefulset-name-0": 5,
				},
			},
			schedulerPolicy: MAXFILLUP,
		},
		{
			name: "many vpods, with gaps and reserved vreplicas on existing and new placements, fully committed",
			vpods: [][]duckv1alpha1.Placement{
				{{PodName: "statefulset-name-0", VReplicas: 1}, {PodName: "statefulset-name-2", VReplicas: 5}},
				{{PodName: "statefulset-name-1", VReplicas: 0}},
				{{PodName: "statefulset-name-1", VReplicas: 0}, {PodName: "statefulset-name-3", VReplicas: 0}},
			},
			expected: state{capacity: 10, free: []int32{int32(4), int32(7), int32(5), int32(10), int32(5)}, lastOrdinal: 4, schedulerPolicy: MAXFILLUP},
			freec:    int32(31),
			reserved: map[types.NamespacedName]map[string]int32{
				{Name: "vpod-name-3", Namespace: "vpod-ns-3"}: {
					"statefulset-name-4": 5,
				},
				{Name: "vpod-name-4", Namespace: "vpod-ns-4"}: {
					"statefulset-name-0": 5,
					"statefulset-name-1": 3,
				},
			},
			schedulerPolicy: MAXFILLUP,
		},
		{
			name: "many vpods, with gaps and reserved vreplicas on existing and new placements, partially committed",
			vpods: [][]duckv1alpha1.Placement{
				{{PodName: "statefulset-name-0", VReplicas: 1}, {PodName: "statefulset-name-2", VReplicas: 5}},
				{{PodName: "statefulset-name-1", VReplicas: 0}},
				{{PodName: "statefulset-name-1", VReplicas: 0}, {PodName: "statefulset-name-3", VReplicas: 0}},
			},
			expected: state{capacity: 10, free: []int32{int32(4), int32(7), int32(2), int32(10)}, lastOrdinal: 2, schedulerPolicy: MAXFILLUP},
			freec:    int32(13),
			reserved: map[types.NamespacedName]map[string]int32{
				{Name: "vpod-name-0", Namespace: "vpod-ns-0"}: {
					"statefulset-name-2": 8,
				},
				{Name: "vpod-name-4", Namespace: "vpod-ns-4"}: {
					"statefulset-name-0": 5,
					"statefulset-name-1": 3,
				},
			},
			schedulerPolicy: MAXFILLUP,
		},
		{
			name:            "no vpods, all nodes with zone labels",
			vpods:           [][]duckv1alpha1.Placement{},
			expected:        state{capacity: 10, free: []int32{}, lastOrdinal: -1, numZones: 3, numNodes: 4, schedulerPolicy: EVENSPREAD, nodeToZoneMap: map[string]string{"node-0": "zone-0", "node-1": "zone-1", "node-2": "zone-2", "node-3": "zone-2"}},
			freec:           int32(0),
			schedulerPolicy: EVENSPREAD,
			nodes:           []*v1.Node{makeNode("node-0", "zone-0"), makeNode("node-1", "zone-1"), makeNode("node-2", "zone-2"), makeNode("node-3", "zone-2")},
		},
		{
			name:            "no vpods, one node with no label",
			vpods:           [][]duckv1alpha1.Placement{},
			expected:        state{capacity: 10, free: []int32{}, lastOrdinal: -1, numZones: 2, numNodes: 3, schedulerPolicy: EVENSPREAD, nodeToZoneMap: map[string]string{"node-0": "zone-0", "node-2": "zone-2", "node-3": "zone-2"}},
			freec:           int32(0),
			schedulerPolicy: EVENSPREAD,
			nodes:           []*v1.Node{makeNode("node-0", "zone-0"), makeNodeNoLabel("node-1"), makeNode("node-2", "zone-2"), makeNode("node-3", "zone-2")},
		},
		{
			name:            "no vpods, all nodes with zone labels",
			vpods:           [][]duckv1alpha1.Placement{},
			expected:        state{capacity: 10, free: []int32{}, lastOrdinal: -1, numZones: 3, numNodes: 4, schedulerPolicy: EVENSPREAD_BYNODE, nodeToZoneMap: map[string]string{"node-0": "zone-0", "node-1": "zone-1", "node-2": "zone-2", "node-3": "zone-2"}},
			freec:           int32(0),
			schedulerPolicy: EVENSPREAD_BYNODE,
			nodes:           []*v1.Node{makeNode("node-0", "zone-0"), makeNode("node-1", "zone-1"), makeNode("node-2", "zone-2"), makeNode("node-3", "zone-2")},
		},
		{
			name:            "no vpods, one node with no label",
			vpods:           [][]duckv1alpha1.Placement{},
			expected:        state{capacity: 10, free: []int32{}, lastOrdinal: -1, numZones: 2, numNodes: 3, schedulerPolicy: EVENSPREAD_BYNODE, nodeToZoneMap: map[string]string{"node-0": "zone-0", "node-2": "zone-2", "node-3": "zone-2"}},
			freec:           int32(0),
			schedulerPolicy: EVENSPREAD_BYNODE,
			nodes:           []*v1.Node{makeNode("node-0", "zone-0"), makeNodeNoLabel("node-1"), makeNode("node-2", "zone-2"), makeNode("node-3", "zone-2")},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, _ := setupFakeContext(t)
			vpodClient := tscheduler.NewVPodClient()
			nodelist := make([]runtime.Object, 0, len(tc.nodes))

			for i, placements := range tc.vpods {
				vpodName := fmt.Sprint("vpod-name-", i)
				vpodNamespace := fmt.Sprint("vpod-ns-", i)

				vpodClient.Create(vpodNamespace, vpodName, 1, placements)
			}

			if tc.schedulerPolicy == EVENSPREAD || tc.schedulerPolicy == EVENSPREAD_BYNODE {
				for i := 0; i < len(tc.nodes); i++ {
					node, err := kubeclient.Get(ctx).CoreV1().Nodes().Create(ctx, tc.nodes[i], metav1.CreateOptions{})
					if err != nil {
						t.Fatal("unexpected error", err)
					}
					nodelist = append(nodelist, node)
				}
			}

			ls := listers.NewListers(nodelist)
			stateBuilder := newStateBuilder(ctx, vpodClient.List, int32(10), tc.schedulerPolicy, ls.GetNodeLister())
			state, err := stateBuilder.State(tc.reserved)
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
