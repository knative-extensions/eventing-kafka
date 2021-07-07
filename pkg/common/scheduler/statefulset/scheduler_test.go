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
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	kubeclient "knative.dev/pkg/client/injection/kube/client/fake"
	_ "knative.dev/pkg/client/injection/kube/informers/apps/v1/statefulset/fake"

	duckv1alpha1 "knative.dev/eventing-kafka/pkg/apis/duck/v1alpha1"
	"knative.dev/eventing-kafka/pkg/common/scheduler"
	"knative.dev/eventing-kafka/pkg/common/scheduler/state"
	tscheduler "knative.dev/eventing-kafka/pkg/common/scheduler/testing"
	listers "knative.dev/eventing/pkg/reconciler/testing/v1"
)

const (
	sfsName       = "statefulset-name"
	vpodName      = "source-name"
	vpodNamespace = "source-namespace"
	numZones      = 3
	numNodes      = 6
)

func TestStatefulsetScheduler(t *testing.T) {
	testCases := []struct {
		name                string
		vreplicas           int32
		replicas            int32
		placements          []duckv1alpha1.Placement
		expected            []duckv1alpha1.Placement
		err                 error
		schedulerPolicyType scheduler.SchedulerPolicyType
		schedulerPolicy     *SchedulerPolicy
	}{
		{
			name:                "no replicas, no vreplicas",
			vreplicas:           0,
			replicas:            int32(0),
			expected:            nil,
			schedulerPolicyType: scheduler.MAXFILLUP,
		},
		{
			name:                "no replicas, 1 vreplicas, fail.",
			vreplicas:           1,
			replicas:            int32(0),
			err:                 scheduler.ErrNotEnoughReplicas,
			expected:            []duckv1alpha1.Placement{},
			schedulerPolicyType: scheduler.MAXFILLUP,
		},
		{
			name:                "one replica, one vreplicas",
			vreplicas:           1,
			replicas:            int32(1),
			expected:            []duckv1alpha1.Placement{{PodName: "statefulset-name-0", VReplicas: 1}},
			schedulerPolicyType: scheduler.MAXFILLUP,
		},
		{
			name:                "one replica, 3 vreplicas",
			vreplicas:           3,
			replicas:            int32(1),
			expected:            []duckv1alpha1.Placement{{PodName: "statefulset-name-0", VReplicas: 3}},
			schedulerPolicyType: scheduler.MAXFILLUP,
		},
		{
			name:                "one replica, 15 vreplicas, unschedulable",
			vreplicas:           15,
			replicas:            int32(1),
			err:                 scheduler.ErrNotEnoughReplicas,
			expected:            []duckv1alpha1.Placement{{PodName: "statefulset-name-0", VReplicas: 10}},
			schedulerPolicyType: scheduler.MAXFILLUP,
		},
		{
			name:      "two replicas, 15 vreplicas, scheduled",
			vreplicas: 15,
			replicas:  int32(2),
			expected: []duckv1alpha1.Placement{
				{PodName: "statefulset-name-0", VReplicas: 10},
				{PodName: "statefulset-name-1", VReplicas: 5},
			},
			schedulerPolicyType: scheduler.MAXFILLUP,
		},
		{
			name:      "two replicas, 15 vreplicas, already scheduled",
			vreplicas: 15,
			replicas:  int32(2),
			placements: []duckv1alpha1.Placement{
				{PodName: "statefulset-name-0", VReplicas: 10},
				{PodName: "statefulset-name-1", VReplicas: 5},
			},
			expected: []duckv1alpha1.Placement{
				{PodName: "statefulset-name-0", VReplicas: 10},
				{PodName: "statefulset-name-1", VReplicas: 5},
			},
			schedulerPolicyType: scheduler.MAXFILLUP,
		},
		{
			name:      "two replicas, 20 vreplicas, scheduling",
			vreplicas: 20,
			replicas:  int32(2),
			placements: []duckv1alpha1.Placement{
				{PodName: "statefulset-name-0", VReplicas: 5},
				{PodName: "statefulset-name-1", VReplicas: 5},
			},
			expected: []duckv1alpha1.Placement{
				{PodName: "statefulset-name-0", VReplicas: 10},
				{PodName: "statefulset-name-1", VReplicas: 10},
			},
			schedulerPolicyType: scheduler.MAXFILLUP,
		},
		{
			name:      "two replicas, 15 vreplicas, too much scheduled (scale down)",
			vreplicas: 15,
			replicas:  int32(2),
			placements: []duckv1alpha1.Placement{
				{PodName: "statefulset-name-0", VReplicas: 10},
				{PodName: "statefulset-name-1", VReplicas: 10},
			},
			expected: []duckv1alpha1.Placement{
				{PodName: "statefulset-name-0", VReplicas: 10},
				{PodName: "statefulset-name-1", VReplicas: 5},
			},
			schedulerPolicyType: scheduler.MAXFILLUP,
		},
		{
			name:                "no replicas, no vreplicas, HA scheduling",
			vreplicas:           0,
			replicas:            int32(0),
			expected:            nil,
			schedulerPolicyType: scheduler.EVENSPREAD,
		},
		{
			name:                "no replicas, 1 vreplicas, fail, HA scheduling",
			vreplicas:           1,
			replicas:            int32(0),
			err:                 scheduler.ErrNotEnoughReplicas,
			expected:            []duckv1alpha1.Placement{},
			schedulerPolicyType: scheduler.EVENSPREAD,
		},
		{
			name:                "one replica, one vreplicas, HA scheduling",
			vreplicas:           1,
			replicas:            int32(1),
			expected:            []duckv1alpha1.Placement{{PodName: "statefulset-name-0", VReplicas: 1}},
			schedulerPolicyType: scheduler.EVENSPREAD,
		},
		{
			name:                "one replica, 3 vreplicas, HA scheduling",
			vreplicas:           3,
			replicas:            int32(1),
			err:                 scheduler.ErrNotEnoughReplicas,
			expected:            []duckv1alpha1.Placement{{PodName: "statefulset-name-0", VReplicas: 1}},
			schedulerPolicyType: scheduler.EVENSPREAD,
		},
		{
			name:                "one replica, 15 vreplicas, unschedulable, HA scheduling",
			vreplicas:           15,
			replicas:            int32(1),
			err:                 scheduler.ErrNotEnoughReplicas,
			expected:            []duckv1alpha1.Placement{{PodName: "statefulset-name-0", VReplicas: 5}},
			schedulerPolicyType: scheduler.EVENSPREAD,
		},
		{
			name:      "two replicas, 15 vreplicas, scheduled, HA scheduling",
			vreplicas: 15,
			replicas:  int32(2),
			err:       scheduler.ErrNotEnoughReplicas,
			expected: []duckv1alpha1.Placement{
				{PodName: "statefulset-name-0", VReplicas: 5},
				{PodName: "statefulset-name-1", VReplicas: 5},
			},
			schedulerPolicyType: scheduler.EVENSPREAD,
		},
		{
			name:      "two replicas, 15 vreplicas, already scheduled, HA scheduling",
			vreplicas: 15,
			replicas:  int32(2),
			placements: []duckv1alpha1.Placement{
				{PodName: "statefulset-name-0", VReplicas: 10},
				{PodName: "statefulset-name-1", VReplicas: 5},
			},
			expected: []duckv1alpha1.Placement{
				{PodName: "statefulset-name-0", VReplicas: 10},
				{PodName: "statefulset-name-1", VReplicas: 5},
			},
			schedulerPolicyType: scheduler.EVENSPREAD,
		},
		{
			name:      "three replicas, 30 vreplicas, HA scheduling",
			vreplicas: 30,
			replicas:  int32(3),
			placements: []duckv1alpha1.Placement{
				{PodName: "statefulset-name-0", VReplicas: 5},
				{PodName: "statefulset-name-1", VReplicas: 5},
				{PodName: "statefulset-name-2", VReplicas: 10},
			},
			expected: []duckv1alpha1.Placement{
				{PodName: "statefulset-name-0", VReplicas: 10},
				{PodName: "statefulset-name-1", VReplicas: 10},
				{PodName: "statefulset-name-2", VReplicas: 10},
			},
			schedulerPolicyType: scheduler.EVENSPREAD,
		},
		{
			name:      "three replicas, 15 vreplicas, too much scheduled (scale down), HA scheduling",
			vreplicas: 15,
			replicas:  int32(3),
			placements: []duckv1alpha1.Placement{
				{PodName: "statefulset-name-0", VReplicas: 10},
				{PodName: "statefulset-name-1", VReplicas: 10},
				{PodName: "statefulset-name-2", VReplicas: 5},
			},
			expected: []duckv1alpha1.Placement{
				{PodName: "statefulset-name-0", VReplicas: 5},
				{PodName: "statefulset-name-1", VReplicas: 5},
				{PodName: "statefulset-name-2", VReplicas: 5},
			},
			schedulerPolicyType: scheduler.EVENSPREAD,
		},
		{
			name:      "three replicas, 15 vreplicas, HA scheduling",
			vreplicas: 15,
			replicas:  int32(3),
			expected: []duckv1alpha1.Placement{
				{PodName: "statefulset-name-0", VReplicas: 5},
				{PodName: "statefulset-name-1", VReplicas: 5},
				{PodName: "statefulset-name-2", VReplicas: 5},
			},
			schedulerPolicyType: scheduler.EVENSPREAD,
		},
		{
			name:      "three replicas, 20 vreplicas, HA scheduling",
			vreplicas: 20,
			replicas:  int32(3),
			expected: []duckv1alpha1.Placement{
				{PodName: "statefulset-name-0", VReplicas: 7},
				{PodName: "statefulset-name-1", VReplicas: 7},
				{PodName: "statefulset-name-2", VReplicas: 6},
			},
			schedulerPolicyType: scheduler.EVENSPREAD,
		},
		{
			name:      "three replicas, 2 vreplicas, too much scheduled (scale down), HA scheduling",
			vreplicas: 2,
			replicas:  int32(3),
			placements: []duckv1alpha1.Placement{
				{PodName: "statefulset-name-0", VReplicas: 1},
				{PodName: "statefulset-name-1", VReplicas: 1},
				{PodName: "statefulset-name-2", VReplicas: 1},
			},
			expected: []duckv1alpha1.Placement{
				{PodName: "statefulset-name-1", VReplicas: 1},
				{PodName: "statefulset-name-2", VReplicas: 1},
			},
			schedulerPolicyType: scheduler.EVENSPREAD,
		},
		{
			name:      "three replicas, 3 vreplicas, too much scheduled (scale down), HA scheduling",
			vreplicas: 3,
			replicas:  int32(3),
			placements: []duckv1alpha1.Placement{
				{PodName: "statefulset-name-0", VReplicas: 2},
				{PodName: "statefulset-name-1", VReplicas: 2},
				{PodName: "statefulset-name-2", VReplicas: 2},
			},
			expected: []duckv1alpha1.Placement{
				{PodName: "statefulset-name-0", VReplicas: 1},
				{PodName: "statefulset-name-1", VReplicas: 1},
				{PodName: "statefulset-name-2", VReplicas: 1},
			},
			schedulerPolicyType: scheduler.EVENSPREAD,
		},
		{
			name:      "four replicas, 7 vreplicas, too much scheduled (scale down), HA scheduling",
			vreplicas: 7,
			replicas:  int32(4),
			placements: []duckv1alpha1.Placement{
				{PodName: "statefulset-name-0", VReplicas: 4},
				{PodName: "statefulset-name-1", VReplicas: 3},
				{PodName: "statefulset-name-2", VReplicas: 4},
				{PodName: "statefulset-name-3", VReplicas: 3},
			},
			expected: []duckv1alpha1.Placement{
				{PodName: "statefulset-name-0", VReplicas: 2},
				{PodName: "statefulset-name-1", VReplicas: 2},
				{PodName: "statefulset-name-2", VReplicas: 3},
			},
			schedulerPolicyType: scheduler.EVENSPREAD,
		},
		{
			name:                "no replicas, no vreplicas, HA scheduling by node",
			vreplicas:           0,
			replicas:            int32(0),
			expected:            nil,
			schedulerPolicyType: scheduler.EVENSPREAD_BYNODE,
		},
		{
			name:                "no replicas, 1 vreplicas, fail, HA scheduling by node",
			vreplicas:           1,
			replicas:            int32(0),
			err:                 scheduler.ErrNotEnoughReplicas,
			expected:            []duckv1alpha1.Placement{},
			schedulerPolicyType: scheduler.EVENSPREAD_BYNODE,
		},
		{
			name:                "one replica, one vreplicas, HA scheduling by node",
			vreplicas:           1,
			replicas:            int32(1),
			expected:            []duckv1alpha1.Placement{{PodName: "statefulset-name-0", VReplicas: 1}},
			schedulerPolicyType: scheduler.EVENSPREAD_BYNODE,
		},
		{
			name:                "one replica, 3 vreplicas, HA scheduling by node",
			vreplicas:           3,
			replicas:            int32(1),
			err:                 scheduler.ErrNotEnoughReplicas,
			expected:            []duckv1alpha1.Placement{{PodName: "statefulset-name-0", VReplicas: 1}},
			schedulerPolicyType: scheduler.EVENSPREAD_BYNODE,
		},
		{
			name:                "one replica, 15 vreplicas, unschedulable, HA scheduling by node",
			vreplicas:           15,
			replicas:            int32(1),
			err:                 scheduler.ErrNotEnoughReplicas,
			expected:            []duckv1alpha1.Placement{{PodName: "statefulset-name-0", VReplicas: 3}},
			schedulerPolicyType: scheduler.EVENSPREAD_BYNODE,
		},
		{
			name:      "five replicas, 15 vreplicas, scheduled, HA scheduling by node",
			vreplicas: 15,
			replicas:  int32(5),
			expected: []duckv1alpha1.Placement{
				{PodName: "statefulset-name-0", VReplicas: 3},
				{PodName: "statefulset-name-1", VReplicas: 3},
				{PodName: "statefulset-name-2", VReplicas: 3},
				{PodName: "statefulset-name-3", VReplicas: 3},
				{PodName: "statefulset-name-4", VReplicas: 3},
			},
			schedulerPolicyType: scheduler.EVENSPREAD_BYNODE,
		},
		{
			name:      "two replicas, 15 vreplicas, already scheduled, HA scheduling by node",
			vreplicas: 15,
			replicas:  int32(2),
			placements: []duckv1alpha1.Placement{
				{PodName: "statefulset-name-0", VReplicas: 10},
				{PodName: "statefulset-name-1", VReplicas: 5},
			},
			expected: []duckv1alpha1.Placement{
				{PodName: "statefulset-name-0", VReplicas: 10},
				{PodName: "statefulset-name-1", VReplicas: 5},
			},
			schedulerPolicyType: scheduler.EVENSPREAD_BYNODE,
		},
		{
			name:      "three replicas, 30 vreplicas, HA scheduling by node",
			vreplicas: 30,
			replicas:  int32(3),
			placements: []duckv1alpha1.Placement{
				{PodName: "statefulset-name-0", VReplicas: 5},
				{PodName: "statefulset-name-1", VReplicas: 5},
				{PodName: "statefulset-name-2", VReplicas: 2},
			},
			err: scheduler.ErrNotEnoughReplicas,
			expected: []duckv1alpha1.Placement{
				{PodName: "statefulset-name-0", VReplicas: 5},
				{PodName: "statefulset-name-1", VReplicas: 5},
				{PodName: "statefulset-name-2", VReplicas: 5},
			},
			schedulerPolicyType: scheduler.EVENSPREAD_BYNODE,
		},
		{
			name:      "three replicas, 6 vreplicas, too much scheduled (scale down), HA scheduling by node",
			vreplicas: 7,
			replicas:  int32(3),
			placements: []duckv1alpha1.Placement{
				{PodName: "statefulset-name-0", VReplicas: 10},
				{PodName: "statefulset-name-1", VReplicas: 10},
				{PodName: "statefulset-name-2", VReplicas: 5},
			},
			expected: []duckv1alpha1.Placement{
				{PodName: "statefulset-name-0", VReplicas: 1},
				{PodName: "statefulset-name-1", VReplicas: 1},
				{PodName: "statefulset-name-2", VReplicas: 5},
			},
			schedulerPolicyType: scheduler.EVENSPREAD_BYNODE,
		},
		{
			name:      "three replicas, 15 vreplicas, HA scheduling by node",
			vreplicas: 15,
			replicas:  int32(3),
			err:       scheduler.ErrNotEnoughReplicas,
			expected: []duckv1alpha1.Placement{
				{PodName: "statefulset-name-0", VReplicas: 3},
				{PodName: "statefulset-name-1", VReplicas: 3},
				{PodName: "statefulset-name-2", VReplicas: 3},
			},
			schedulerPolicyType: scheduler.EVENSPREAD_BYNODE,
		},
		{
			name:      "five replicas, 20 vreplicas, HA scheduling by node",
			vreplicas: 20,
			replicas:  int32(5),
			expected: []duckv1alpha1.Placement{
				{PodName: "statefulset-name-0", VReplicas: 4},
				{PodName: "statefulset-name-1", VReplicas: 4},
				{PodName: "statefulset-name-2", VReplicas: 4},
				{PodName: "statefulset-name-3", VReplicas: 4},
				{PodName: "statefulset-name-4", VReplicas: 4},
			},
			schedulerPolicyType: scheduler.EVENSPREAD_BYNODE,
		},
		{
			name:      "three replicas, 2 vreplicas, too much scheduled (scale down), HA scheduling by node",
			vreplicas: 2,
			replicas:  int32(3),
			placements: []duckv1alpha1.Placement{
				{PodName: "statefulset-name-0", VReplicas: 1},
				{PodName: "statefulset-name-1", VReplicas: 1},
				{PodName: "statefulset-name-2", VReplicas: 1},
			},
			expected: []duckv1alpha1.Placement{
				{PodName: "statefulset-name-1", VReplicas: 1},
				{PodName: "statefulset-name-2", VReplicas: 1},
			},
			schedulerPolicyType: scheduler.EVENSPREAD_BYNODE,
		},
		{
			name:      "four replicas, 6 vreplicas, too much scheduled (scale down), HA scheduling by node",
			vreplicas: 6,
			replicas:  int32(4),
			placements: []duckv1alpha1.Placement{
				{PodName: "statefulset-name-0", VReplicas: 2},
				{PodName: "statefulset-name-1", VReplicas: 2},
				{PodName: "statefulset-name-2", VReplicas: 2},
				{PodName: "statefulset-name-3", VReplicas: 2},
			},
			expected: []duckv1alpha1.Placement{
				{PodName: "statefulset-name-0", VReplicas: 1},
				{PodName: "statefulset-name-1", VReplicas: 1},
				{PodName: "statefulset-name-2", VReplicas: 2},
				{PodName: "statefulset-name-3", VReplicas: 2},
			},
			schedulerPolicyType: scheduler.EVENSPREAD_BYNODE,
		},
		{
			name:      "three replicas, 7 vreplicas, too much scheduled (scale down), HA scheduling by node",
			vreplicas: 7,
			replicas:  int32(3),
			placements: []duckv1alpha1.Placement{
				{PodName: "statefulset-name-0", VReplicas: 4},
				{PodName: "statefulset-name-1", VReplicas: 3},
				{PodName: "statefulset-name-2", VReplicas: 4},
			},
			expected: []duckv1alpha1.Placement{
				{PodName: "statefulset-name-0", VReplicas: 1},
				{PodName: "statefulset-name-1", VReplicas: 2},
				{PodName: "statefulset-name-2", VReplicas: 4},
			},
			schedulerPolicyType: scheduler.EVENSPREAD_BYNODE,
		},
		{
			name:      "no replicas, no vreplicas with Predicates and Priorities",
			vreplicas: 0,
			replicas:  int32(0),
			expected:  nil,
			schedulerPolicy: &SchedulerPolicy{
				Predicates: []PredicatePolicy{
					{Name: "PodFitsResources"},
				},
				Priorities: []PriorityPolicy{
					{Name: "LowestOrdinalPriority", Weight: 1},
				},
			},
		},
		{
			name:      "no replicas, 1 vreplicas, fail with Predicates and Priorities",
			vreplicas: 1,
			replicas:  int32(0),
			err:       scheduler.ErrNotEnoughReplicas,
			expected:  nil,
			schedulerPolicy: &SchedulerPolicy{
				Predicates: []PredicatePolicy{
					{Name: "PodFitsResources"},
				},
				Priorities: []PriorityPolicy{
					{Name: "LowestOrdinalPriority", Weight: 1},
				},
			},
		},
		{
			name:      "one replica, one vreplicas with Predicates and Priorities",
			vreplicas: 1,
			replicas:  int32(1),
			expected:  []duckv1alpha1.Placement{{PodName: "statefulset-name-0", VReplicas: 1}},
			schedulerPolicy: &SchedulerPolicy{
				Predicates: []PredicatePolicy{
					{Name: "PodFitsResources"},
				},
				Priorities: []PriorityPolicy{
					{Name: "LowestOrdinalPriority", Weight: 1},
				},
			},
		},
		{
			name:      "one replica, 3 vreplicas with Predicates and Priorities",
			vreplicas: 3,
			replicas:  int32(1),
			expected:  []duckv1alpha1.Placement{{PodName: "statefulset-name-0", VReplicas: 3}},
			schedulerPolicy: &SchedulerPolicy{
				Predicates: []PredicatePolicy{
					{Name: "PodFitsResources"},
				},
				Priorities: []PriorityPolicy{
					{Name: "LowestOrdinalPriority", Weight: 1},
				},
			},
		},
		{
			name:      "one replica, 15 vreplicas, unschedulable with Predicates and Priorities",
			vreplicas: 15,
			replicas:  int32(1),
			err:       scheduler.ErrNotEnoughReplicas,
			expected:  []duckv1alpha1.Placement{{PodName: "statefulset-name-0", VReplicas: 10}},
			schedulerPolicy: &SchedulerPolicy{
				Predicates: []PredicatePolicy{
					{Name: "PodFitsResources"},
				},
				Priorities: []PriorityPolicy{
					{Name: "LowestOrdinalPriority", Weight: 1},
				},
			},
		},
		{
			name:      "two replicas, 15 vreplicas, scheduled with Predicates and Priorities",
			vreplicas: 15,
			replicas:  int32(2),
			expected: []duckv1alpha1.Placement{
				{PodName: "statefulset-name-0", VReplicas: 10},
				{PodName: "statefulset-name-1", VReplicas: 5},
			},
			schedulerPolicy: &SchedulerPolicy{
				Predicates: []PredicatePolicy{
					{Name: "PodFitsResources"},
				},
				Priorities: []PriorityPolicy{
					{Name: "LowestOrdinalPriority", Weight: 1},
				},
			},
		},
		{
			name:      "two replicas, 15 vreplicas, already scheduled with Predicates and Priorities",
			vreplicas: 15,
			replicas:  int32(2),
			placements: []duckv1alpha1.Placement{
				{PodName: "statefulset-name-0", VReplicas: 10},
				{PodName: "statefulset-name-1", VReplicas: 5},
			},
			expected: []duckv1alpha1.Placement{
				{PodName: "statefulset-name-0", VReplicas: 10},
				{PodName: "statefulset-name-1", VReplicas: 5},
			},
			schedulerPolicy: &SchedulerPolicy{
				Predicates: []PredicatePolicy{
					{Name: "PodFitsResources"},
				},
				Priorities: []PriorityPolicy{
					{Name: "LowestOrdinalPriority", Weight: 1},
				},
			},
		},
		{
			name:      "two replicas, 20 vreplicas, scheduling with Predicates and Priorities",
			vreplicas: 20,
			replicas:  int32(2),
			placements: []duckv1alpha1.Placement{
				{PodName: "statefulset-name-0", VReplicas: 5},
				{PodName: "statefulset-name-1", VReplicas: 5},
			},
			expected: []duckv1alpha1.Placement{
				{PodName: "statefulset-name-0", VReplicas: 10},
				{PodName: "statefulset-name-1", VReplicas: 10},
			},
			schedulerPolicy: &SchedulerPolicy{
				Predicates: []PredicatePolicy{
					{Name: "PodFitsResources"},
				},
				Priorities: []PriorityPolicy{
					{Name: "LowestOrdinalPriority", Weight: 1},
				},
			},
		},
		{
			name:      "no replicas, no vreplicas with two Predicates and two Priorities",
			vreplicas: 0,
			replicas:  int32(0),
			expected:  nil,
			schedulerPolicy: &SchedulerPolicy{
				Predicates: []PredicatePolicy{
					{Name: "PodFitsResources"},
					{Name: "EvenPodSpread",
						Args: "{\"MaxSkew\": 2}"},
				},
				Priorities: []PriorityPolicy{
					{Name: "LowestOrdinalPriority", Weight: 2},
					{Name: "AvailabilityZonePriority", Weight: 10, Args: "{\"MaxSkew\": 2}"},
				},
			},
		},
		{
			name:      "no replicas, 1 vreplicas, fail with two Predicates and two Priorities",
			vreplicas: 1,
			replicas:  int32(0),
			err:       scheduler.ErrNotEnoughReplicas,
			expected:  nil,
			schedulerPolicy: &SchedulerPolicy{
				Predicates: []PredicatePolicy{
					{Name: "PodFitsResources"},
					{Name: "EvenPodSpread", Args: "{\"MaxSkew\": 2}"},
				},
				Priorities: []PriorityPolicy{
					{Name: "LowestOrdinalPriority", Weight: 2},
					{Name: "AvailabilityZonePriority", Weight: 10, Args: "{\"MaxSkew\": 2}"},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, _ := tscheduler.SetupFakeContext(t)
			nodelist := make([]runtime.Object, 0, numZones)
			podlist := make([]runtime.Object, 0, tc.replicas)
			vpodClient := tscheduler.NewVPodClient()

			for i := int32(0); i < numZones; i++ {
				for j := int32(0); j < numNodes/numZones; j++ {
					nodeName := "node" + fmt.Sprint((j*((numNodes/numZones)+1))+i)
					zoneName := "zone" + fmt.Sprint(i)
					node, err := kubeclient.Get(ctx).CoreV1().Nodes().Create(ctx, tscheduler.MakeNode(nodeName, zoneName), metav1.CreateOptions{})
					if err != nil {
						t.Fatal("unexpected error", err)
					}
					nodelist = append(nodelist, node)
				}
			}
			for i := int32(0); i < tc.replicas; i++ {
				nodeName := "node" + fmt.Sprint(i)
				podName := sfsName + "-" + fmt.Sprint(i)
				pod, err := kubeclient.Get(ctx).CoreV1().Pods(testNs).Create(ctx, tscheduler.MakePod(testNs, podName, nodeName), metav1.CreateOptions{})
				if err != nil {
					t.Fatal("unexpected error", err)
				}
				podlist = append(podlist, pod)
			}

			_, err := kubeclient.Get(ctx).AppsV1().StatefulSets(testNs).Create(ctx, tscheduler.MakeStatefulset(testNs, sfsName, tc.replicas), metav1.CreateOptions{})
			if err != nil {
				t.Fatal("unexpected error", err)
			}
			lsp := listers.NewListers(podlist)
			lsn := listers.NewListers(nodelist)
			sa := state.NewStateBuilder(ctx, sfsName, vpodClient.List, 10, tc.schedulerPolicyType, lsp.GetPodLister().Pods(testNs), lsn.GetNodeLister())
			s := NewStatefulSetScheduler(ctx, testNs, sfsName, vpodClient.List, sa, nil, lsp.GetPodLister().Pods(testNs), tc.schedulerPolicy).(*StatefulSetScheduler)

			// Give some time for the informer to notify the scheduler and set the number of replicas
			time.Sleep(200 * time.Millisecond)

			func() {
				s.lock.Lock()
				defer s.lock.Unlock()
				if s.replicas != tc.replicas {
					t.Fatalf("expected number of statefulset replica to be %d (got %d)", tc.replicas, s.replicas)
				}
			}()

			vpod := vpodClient.Create(vpodNamespace, vpodName, tc.vreplicas, tc.placements)
			placements, err := s.Schedule(vpod)

			if tc.err == nil && err != nil {
				t.Fatal("unexpected error", err)
			}

			if tc.err != nil && err == nil {
				t.Fatal("expected error, got none")
			}

			if !reflect.DeepEqual(placements, tc.expected) {
				t.Errorf("got %v, want %v", placements, tc.expected)
			}

		})
	}
}
