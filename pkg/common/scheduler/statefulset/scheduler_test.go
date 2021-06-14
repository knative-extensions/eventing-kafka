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
	"fmt"
	"reflect"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	gtesting "k8s.io/client-go/testing"

	kubeclient "knative.dev/pkg/client/injection/kube/client/fake"
	_ "knative.dev/pkg/client/injection/kube/informers/apps/v1/statefulset/fake"
	"knative.dev/pkg/controller"
	rectesting "knative.dev/pkg/reconciler/testing"

	duckv1alpha1 "knative.dev/eventing-kafka/pkg/apis/duck/v1alpha1"
	"knative.dev/eventing-kafka/pkg/common/scheduler"
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
		name            string
		vreplicas       int32
		replicas        int32
		placements      []duckv1alpha1.Placement
		expected        []duckv1alpha1.Placement
		err             error
		schedulerPolicy SchedulerPolicyType
	}{
		{
			name:      "no replicas, no vreplicas",
			vreplicas: 0,
			replicas:  int32(0),
			expected:  nil,
		},
		{
			name:      "no replicas, 1 vreplicas, fail.",
			vreplicas: 1,
			replicas:  int32(0),
			err:       scheduler.ErrNotEnoughReplicas,
			expected:  []duckv1alpha1.Placement{},
		},
		{
			name:      "one replica, one vreplicas",
			vreplicas: 1,
			replicas:  int32(1),
			expected:  []duckv1alpha1.Placement{{PodName: "statefulset-name-0", VReplicas: 1}},
		},
		{
			name:      "one replica, 3 vreplicas",
			vreplicas: 3,
			replicas:  int32(1),
			expected:  []duckv1alpha1.Placement{{PodName: "statefulset-name-0", VReplicas: 3}},
		},
		{
			name:      "one replica, 15 vreplicas, unschedulable",
			vreplicas: 15,
			replicas:  int32(1),
			err:       scheduler.ErrNotEnoughReplicas,
			expected:  []duckv1alpha1.Placement{{PodName: "statefulset-name-0", VReplicas: 10}},
		},
		{
			name:      "two replicas, 15 vreplicas, scheduled",
			vreplicas: 15,
			replicas:  int32(2),
			expected: []duckv1alpha1.Placement{
				{PodName: "statefulset-name-0", VReplicas: 10},
				{PodName: "statefulset-name-1", VReplicas: 5},
			},
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
		},
		{
			name:            "no replicas, no vreplicas, HA scheduling",
			vreplicas:       0,
			replicas:        int32(0),
			expected:        nil,
			schedulerPolicy: EVENSPREAD,
		},
		{
			name:            "no replicas, 1 vreplicas, fail, HA scheduling",
			vreplicas:       1,
			replicas:        int32(0),
			err:             scheduler.ErrNotEnoughReplicas,
			expected:        []duckv1alpha1.Placement{},
			schedulerPolicy: EVENSPREAD,
		},
		{
			name:            "one replica, one vreplicas, HA scheduling",
			vreplicas:       1,
			replicas:        int32(1),
			expected:        []duckv1alpha1.Placement{{PodName: "statefulset-name-0", VReplicas: 1}},
			schedulerPolicy: EVENSPREAD,
		},
		{
			name:            "one replica, 3 vreplicas, HA scheduling",
			vreplicas:       3,
			replicas:        int32(1),
			err:             scheduler.ErrNotEnoughReplicas,
			expected:        []duckv1alpha1.Placement{{PodName: "statefulset-name-0", VReplicas: 1}},
			schedulerPolicy: EVENSPREAD,
		},
		{
			name:            "one replica, 15 vreplicas, unschedulable, HA scheduling",
			vreplicas:       15,
			replicas:        int32(1),
			err:             scheduler.ErrNotEnoughReplicas,
			expected:        []duckv1alpha1.Placement{{PodName: "statefulset-name-0", VReplicas: 5}},
			schedulerPolicy: EVENSPREAD,
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
			schedulerPolicy: EVENSPREAD,
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
			schedulerPolicy: EVENSPREAD,
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
			schedulerPolicy: EVENSPREAD,
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
			schedulerPolicy: EVENSPREAD,
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
			schedulerPolicy: EVENSPREAD,
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
			schedulerPolicy: EVENSPREAD,
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
			schedulerPolicy: EVENSPREAD,
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
			schedulerPolicy: EVENSPREAD,
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
			schedulerPolicy: EVENSPREAD,
		},
		{
			name:            "no replicas, no vreplicas, HA scheduling by node",
			vreplicas:       0,
			replicas:        int32(0),
			expected:        nil,
			schedulerPolicy: EVENSPREAD_BYNODE,
		},
		{
			name:            "no replicas, 1 vreplicas, fail, HA scheduling by node",
			vreplicas:       1,
			replicas:        int32(0),
			err:             scheduler.ErrNotEnoughReplicas,
			expected:        []duckv1alpha1.Placement{},
			schedulerPolicy: EVENSPREAD_BYNODE,
		},
		{
			name:            "one replica, one vreplicas, HA scheduling by node",
			vreplicas:       1,
			replicas:        int32(1),
			expected:        []duckv1alpha1.Placement{{PodName: "statefulset-name-0", VReplicas: 1}},
			schedulerPolicy: EVENSPREAD_BYNODE,
		},
		{
			name:            "one replica, 3 vreplicas, HA scheduling by node",
			vreplicas:       3,
			replicas:        int32(1),
			err:             scheduler.ErrNotEnoughReplicas,
			expected:        []duckv1alpha1.Placement{{PodName: "statefulset-name-0", VReplicas: 1}},
			schedulerPolicy: EVENSPREAD_BYNODE,
		},
		{
			name:            "one replica, 15 vreplicas, unschedulable, HA scheduling by node",
			vreplicas:       15,
			replicas:        int32(1),
			err:             scheduler.ErrNotEnoughReplicas,
			expected:        []duckv1alpha1.Placement{{PodName: "statefulset-name-0", VReplicas: 3}},
			schedulerPolicy: EVENSPREAD_BYNODE,
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
			schedulerPolicy: EVENSPREAD_BYNODE,
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
			schedulerPolicy: EVENSPREAD_BYNODE,
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
			schedulerPolicy: EVENSPREAD_BYNODE,
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
			schedulerPolicy: EVENSPREAD_BYNODE,
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
			schedulerPolicy: EVENSPREAD_BYNODE,
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
			schedulerPolicy: EVENSPREAD_BYNODE,
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
			schedulerPolicy: EVENSPREAD_BYNODE,
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
			schedulerPolicy: EVENSPREAD_BYNODE,
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
			schedulerPolicy: EVENSPREAD_BYNODE,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, _ := setupFakeContext(t)
			nodelist := make([]runtime.Object, 0, numZones)
			podlist := make([]runtime.Object, 0, tc.replicas)
			vpodClient := tscheduler.NewVPodClient()

			if tc.schedulerPolicy == EVENSPREAD || tc.schedulerPolicy == EVENSPREAD_BYNODE {
				for i := int32(0); i < numZones; i++ {
					for j := int32(0); j < numNodes/numZones; j++ {
						nodeName := "node" + fmt.Sprint((j*((numNodes/numZones)+1))+i)
						zoneName := "zone" + fmt.Sprint(i)
						node, err := kubeclient.Get(ctx).CoreV1().Nodes().Create(ctx, makeNode(nodeName, zoneName), metav1.CreateOptions{})
						if err != nil {
							t.Fatal("unexpected error", err)
						}
						nodelist = append(nodelist, node)
					}
				}
				for i := int32(0); i < tc.replicas; i++ {
					nodeName := "node" + fmt.Sprint(i)
					podName := sfsName + "-" + fmt.Sprint(i)
					pod, err := kubeclient.Get(ctx).CoreV1().Pods(testNs).Create(ctx, makePod(testNs, podName, nodeName), metav1.CreateOptions{})
					if err != nil {
						t.Fatal("unexpected error", err)
					}
					podlist = append(podlist, pod)
				}
			}

			_, err := kubeclient.Get(ctx).AppsV1().StatefulSets(testNs).Create(ctx, makeStatefulset(testNs, sfsName, tc.replicas), metav1.CreateOptions{})
			if err != nil {
				t.Fatal("unexpected error", err)
			}
			lsn := listers.NewListers(nodelist)
			sa := newStateBuilder(ctx, vpodClient.List, 10, tc.schedulerPolicy, lsn.GetNodeLister())
			lsp := listers.NewListers(podlist)
			s := NewStatefulSetScheduler(ctx, testNs, sfsName, vpodClient.List, sa, nil, lsp.GetPodLister().Pods(testNs)).(*StatefulSetScheduler)

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

func makeStatefulset(ns, name string, replicas int32) *appsv1.StatefulSet {
	obj := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &replicas,
		},
		Status: appsv1.StatefulSetStatus{
			Replicas: replicas,
		},
	}

	return obj
}

func makeNode(name, zonename string) *corev1.Node {
	obj := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				ZoneLabel: zonename,
			},
		},
	}
	return obj
}

func makeNodeNoLabel(name string) *corev1.Node {
	obj := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
	}
	return obj
}

func makePod(ns, name, nodename string) *corev1.Pod {
	obj := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
		},
		Spec: corev1.PodSpec{
			NodeName: nodename,
		},
	}
	return obj
}

func setupFakeContext(t *testing.T) (context.Context, context.CancelFunc) {
	ctx, cancel, informers := rectesting.SetupFakeContextWithCancel(t)
	err := controller.StartInformers(ctx.Done(), informers...)
	if err != nil {
		t.Fatal("unexpected error", err)
	}

	kc := kubeclient.Get(ctx)
	kc.PrependReactor("create", "statefulsets", func(action gtesting.Action) (handled bool, ret runtime.Object, err error) {
		createAction := action.(gtesting.CreateActionImpl)
		sfs := createAction.GetObject().(*appsv1.StatefulSet)
		scale := &autoscalingv1.Scale{
			ObjectMeta: metav1.ObjectMeta{
				Name:      sfs.Name,
				Namespace: sfs.Namespace,
			},
			Spec: autoscalingv1.ScaleSpec{
				Replicas: func() int32 {
					if sfs.Spec.Replicas == nil {
						return 1
					}
					return *sfs.Spec.Replicas
				}(),
			},
		}
		kc.Tracker().Add(scale)
		return false, nil, nil
	})

	kc.PrependReactor("get", "statefulsets", func(action gtesting.Action) (handled bool, ret runtime.Object, err error) {
		getAction := action.(gtesting.GetAction)
		if action.GetSubresource() == "scale" {
			scale, err := kc.Tracker().Get(autoscalingv1.SchemeGroupVersion.WithResource("scales"), getAction.GetNamespace(), getAction.GetName())
			return true, scale, err

		}
		return false, nil, nil
	})

	kc.PrependReactor("update", "statefulsets", func(action gtesting.Action) (handled bool, ret runtime.Object, err error) {
		updateAction := action.(gtesting.UpdateActionImpl)
		if action.GetSubresource() == "scale" {
			scale := updateAction.GetObject().(*autoscalingv1.Scale)

			err := kc.Tracker().Update(autoscalingv1.SchemeGroupVersion.WithResource("scales"), scale, scale.GetNamespace())
			if err != nil {
				return true, nil, err
			}

			meta, err := meta.Accessor(updateAction.GetObject())
			if err != nil {
				return true, nil, err
			}

			obj, err := kc.Tracker().Get(appsv1.SchemeGroupVersion.WithResource("statefulsets"), meta.GetNamespace(), meta.GetName())
			if err != nil {
				return true, nil, err
			}

			sfs := obj.(*appsv1.StatefulSet)
			sfs.Spec.Replicas = &scale.Spec.Replicas

			err = kc.Tracker().Update(appsv1.SchemeGroupVersion.WithResource("statefulsets"), sfs, sfs.GetNamespace())
			if err != nil {
				return true, nil, err
			}

			return true, scale, nil

		}
		return false, nil, nil
	})

	return ctx, cancel
}
