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
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	gtesting "k8s.io/client-go/testing"

	listers "knative.dev/eventing/pkg/reconciler/testing/v1"
	kubeclient "knative.dev/pkg/client/injection/kube/client/fake"
	_ "knative.dev/pkg/client/injection/kube/informers/apps/v1/statefulset/fake"

	duckv1alpha1 "knative.dev/eventing-kafka/pkg/apis/duck/v1alpha1"
	"knative.dev/eventing-kafka/pkg/common/scheduler"
	tscheduler "knative.dev/eventing-kafka/pkg/common/scheduler/testing"
)

const (
	testNs = "test-ns"
)

func TestAutoscaler(t *testing.T) {
	testCases := []struct {
		name            string
		replicas        int32
		vpods           []scheduler.VPod
		pendings        int32
		scaleDown       bool
		wantReplicas    int32
		schedulerPolicy SchedulerPolicyType
	}{
		{
			name:     "no replicas, no placements, no pending",
			replicas: int32(0),
			vpods: []scheduler.VPod{
				tscheduler.NewVPod(testNs, "vpod-1", 0, nil),
			},
			pendings:     int32(0),
			wantReplicas: int32(0),
		},
		{
			name:     "no replicas, no placements, with pending",
			replicas: int32(0),
			vpods: []scheduler.VPod{
				tscheduler.NewVPod(testNs, "vpod-1", 5, nil),
			},
			pendings:     int32(5),
			wantReplicas: int32(1),
		},
		{
			name:     "no replicas, with placements, no pending",
			replicas: int32(0),
			vpods: []scheduler.VPod{
				tscheduler.NewVPod(testNs, "vpod-1", 15, []duckv1alpha1.Placement{
					{PodName: "pod-0", VReplicas: int32(8)},
					{PodName: "pod-1", VReplicas: int32(7)}}),
			},
			pendings:     int32(0),
			wantReplicas: int32(2),
		},
		{
			name:     "no replicas, with placements, with pending, enough capacity",
			replicas: int32(0),
			vpods: []scheduler.VPod{
				tscheduler.NewVPod(testNs, "vpod-1", 15, []duckv1alpha1.Placement{
					{PodName: "pod-0", VReplicas: int32(8)},
					{PodName: "pod-1", VReplicas: int32(7)}}),
			},
			pendings:     int32(3),
			wantReplicas: int32(3),
		},
		{
			name:     "no replicas, with placements, with pending, not enough capacity",
			replicas: int32(0),
			vpods: []scheduler.VPod{
				tscheduler.NewVPod(testNs, "vpod-1", 15, []duckv1alpha1.Placement{
					{PodName: "pod-0", VReplicas: int32(8)},
					{PodName: "pod-1", VReplicas: int32(7)}}),
			},
			pendings:     int32(8),
			wantReplicas: int32(3),
		},
		{
			name:     "with replicas, no placements, no pending, scale down",
			replicas: int32(3),
			vpods: []scheduler.VPod{
				tscheduler.NewVPod(testNs, "vpod-1", 0, nil),
			},
			pendings:     int32(0),
			scaleDown:    true,
			wantReplicas: int32(0),
		},
		{
			name:     "with replicas, no placements, with pending, scale down",
			replicas: int32(3),
			vpods: []scheduler.VPod{
				tscheduler.NewVPod(testNs, "vpod-1", 5, nil),
			},
			pendings:     int32(5),
			scaleDown:    true,
			wantReplicas: int32(1),
		},
		{
			name:     "with replicas, no placements, with pending, scale down disabled",
			replicas: int32(3),
			vpods: []scheduler.VPod{
				tscheduler.NewVPod(testNs, "vpod-1", 5, nil),
			},
			pendings:     int32(5),
			scaleDown:    false,
			wantReplicas: int32(3),
		},
		{
			name:     "with replicas, no placements, with pending, scale up",
			replicas: int32(3),
			vpods: []scheduler.VPod{
				tscheduler.NewVPod(testNs, "vpod-1", 5, nil),
			},
			pendings:     int32(40),
			wantReplicas: int32(4),
		},
		{
			name:     "with replicas, with placements, no pending, no change",
			replicas: int32(2),
			vpods: []scheduler.VPod{
				tscheduler.NewVPod(testNs, "vpod-1", 15, []duckv1alpha1.Placement{
					{PodName: "pod-0", VReplicas: int32(8)},
					{PodName: "pod-1", VReplicas: int32(7)}}),
			},
			pendings:     int32(0),
			wantReplicas: int32(2),
		},
		{
			name:     "with replicas, with placements, no pending, scale down",
			replicas: int32(5),
			vpods: []scheduler.VPod{
				tscheduler.NewVPod(testNs, "vpod-1", 15, []duckv1alpha1.Placement{
					{PodName: "pod-0", VReplicas: int32(8)},
					{PodName: "pod-1", VReplicas: int32(7)}}),
			},
			pendings:     int32(0),
			scaleDown:    true,
			wantReplicas: int32(2),
		},
		{
			name:     "with replicas, with placements, with pending, enough capacity",
			replicas: int32(2),
			vpods: []scheduler.VPod{
				tscheduler.NewVPod(testNs, "vpod-1", 15, []duckv1alpha1.Placement{
					{PodName: "pod-0", VReplicas: int32(8)},
					{PodName: "pod-1", VReplicas: int32(7)}}),
			},
			pendings:     int32(3),
			wantReplicas: int32(3),
		},
		{
			name:     "with replicas, with placements, with pending, not enough capacity",
			replicas: int32(2),
			vpods: []scheduler.VPod{
				tscheduler.NewVPod(testNs, "vpod-1", 15, []duckv1alpha1.Placement{
					{PodName: "pod-0", VReplicas: int32(8)},
					{PodName: "pod-1", VReplicas: int32(7)}}),
			},
			pendings:     int32(8),
			wantReplicas: int32(3),
		},
		{
			name:     "no replicas, with placements, with pending, enough capacity",
			replicas: int32(0),
			vpods: []scheduler.VPod{
				tscheduler.NewVPod(testNs, "vpod-1", 15, []duckv1alpha1.Placement{
					{PodName: "pod-0", VReplicas: int32(8)},
					{PodName: "pod-1", VReplicas: int32(7)}}),
			},
			pendings:        int32(3),
			wantReplicas:    int32(3),
			schedulerPolicy: EVENSPREAD,
		},
		{
			name:     "with replicas, with placements, with pending, enough capacity",
			replicas: int32(2),
			vpods: []scheduler.VPod{
				tscheduler.NewVPod(testNs, "vpod-1", 15, []duckv1alpha1.Placement{
					{PodName: "pod-0", VReplicas: int32(8)},
					{PodName: "pod-1", VReplicas: int32(7)}}),
			},
			pendings:        int32(3),
			wantReplicas:    int32(3),
			schedulerPolicy: EVENSPREAD,
		},
		{
			name:     "no replicas, with placements, with pending, enough capacity",
			replicas: int32(0),
			vpods: []scheduler.VPod{
				tscheduler.NewVPod(testNs, "vpod-1", 15, []duckv1alpha1.Placement{
					{PodName: "pod-0", VReplicas: int32(8)},
					{PodName: "pod-1", VReplicas: int32(7)}}),
			},
			pendings:        int32(3),
			wantReplicas:    int32(3),
			schedulerPolicy: EVENSPREAD_BYNODE,
		},
		{
			name:     "with replicas, with placements, with pending, enough capacity",
			replicas: int32(2),
			vpods: []scheduler.VPod{
				tscheduler.NewVPod(testNs, "vpod-1", 15, []duckv1alpha1.Placement{
					{PodName: "pod-0", VReplicas: int32(8)},
					{PodName: "pod-1", VReplicas: int32(7)}}),
			},
			pendings:        int32(3),
			wantReplicas:    int32(3),
			schedulerPolicy: EVENSPREAD_BYNODE,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, _ := setupFakeContext(t)

			vpodClient := tscheduler.NewVPodClient()
			ls := listers.NewListers(nil)
			stateAccessor := newStateBuilder(ctx, vpodClient.List, 10, tc.schedulerPolicy, ls.GetNodeLister())

			sfsClient := kubeclient.Get(ctx).AppsV1().StatefulSets(testNs)
			_, err := sfsClient.Create(ctx, makeStatefulset(testNs, sfsName, tc.replicas), metav1.CreateOptions{})
			if err != nil {
				t.Fatal("unexpected error", err)
			}

			noopEvictor := func(vpod scheduler.VPod, from *duckv1alpha1.Placement) error {
				return nil
			}

			autoscaler := NewAutoscaler(ctx, testNs, sfsName, vpodClient.List, stateAccessor, noopEvictor, 10*time.Second, int32(10)).(*autoscaler)

			for _, vpod := range tc.vpods {
				vpodClient.Append(vpod)
			}

			err = autoscaler.doautoscale(ctx, tc.scaleDown, tc.pendings)
			if err != nil {
				t.Fatal("unexpected error", err)
			}

			scale, err := sfsClient.GetScale(ctx, sfsName, metav1.GetOptions{})
			if err != nil {
				t.Fatal("unexpected error", err)
			}
			if scale.Spec.Replicas != tc.wantReplicas {
				t.Errorf("unexpected number of replicas, got %d, want %d", scale.Spec.Replicas, tc.wantReplicas)
			}

		})
	}
}

func TestAutoscalerScaleDownToZero(t *testing.T) {
	ctx, cancel := setupFakeContext(t)

	afterUpdate := make(chan bool)
	kubeclient.Get(ctx).PrependReactor("update", "statefulsets", func(action gtesting.Action) (handled bool, ret runtime.Object, err error) {
		if action.GetSubresource() == "scale" {
			afterUpdate <- true
		}
		return false, nil, nil
	})

	vpodClient := tscheduler.NewVPodClient()
	ls := listers.NewListers(nil)
	stateAccessor := newStateBuilder(ctx, vpodClient.List, 10, MAXFILLUP, ls.GetNodeLister())

	sfsClient := kubeclient.Get(ctx).AppsV1().StatefulSets(testNs)
	_, err := sfsClient.Create(ctx, makeStatefulset(testNs, sfsName, 10), metav1.CreateOptions{})
	if err != nil {
		t.Fatal("unexpected error", err)
	}

	noopEvictor := func(vpod scheduler.VPod, from *duckv1alpha1.Placement) error {
		return nil
	}

	autoscaler := NewAutoscaler(ctx, testNs, sfsName, vpodClient.List, stateAccessor, noopEvictor, 2*time.Second, int32(10)).(*autoscaler)

	done := make(chan bool)
	go func() {
		autoscaler.Start(ctx)
		done <- true
	}()

	select {
	case <-afterUpdate:
	case <-time.After(4 * time.Second):
		t.Fatal("timeout waiting for scale subresource to be updated")

	}

	sfs, err := sfsClient.Get(ctx, sfsName, metav1.GetOptions{})
	if err != nil {
		t.Fatal("unexpected error", err)
	}
	if *sfs.Spec.Replicas != 0 {
		t.Errorf("unexpected number of replicas, got %d, want 0", *sfs.Spec.Replicas)
	}

	cancel()

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for autoscaler to stop")
	}
}

func TestCompactor(t *testing.T) {
	testCases := []struct {
		name            string
		replicas        int32
		vpods           []scheduler.VPod
		schedulerPolicy SchedulerPolicyType
		wantEvictions   map[types.NamespacedName]duckv1alpha1.Placement
	}{
		{
			name:     "no replicas, no placements, no pending",
			replicas: int32(0),
			vpods: []scheduler.VPod{
				tscheduler.NewVPod(testNs, "vpod-1", 0, nil),
			},
			schedulerPolicy: MAXFILLUP,
			wantEvictions:   nil,
		},
		{
			name:     "one vpod, with placements in 2 pods, compacted",
			replicas: int32(2),
			vpods: []scheduler.VPod{
				tscheduler.NewVPod(testNs, "vpod-1", 15, []duckv1alpha1.Placement{
					{PodName: "pod-0", VReplicas: int32(8)},
					{PodName: "pod-1", VReplicas: int32(7)}}),
			},
			schedulerPolicy: MAXFILLUP,
			wantEvictions:   nil,
		},
		{
			name:     "one vpod, with  placements in 2 pods, compacted edge",
			replicas: int32(2),
			vpods: []scheduler.VPod{
				tscheduler.NewVPod(testNs, "vpod-1", 11, []duckv1alpha1.Placement{
					{PodName: "pod-0", VReplicas: int32(8)},
					{PodName: "pod-1", VReplicas: int32(3)}}),
			},
			schedulerPolicy: MAXFILLUP,
			wantEvictions:   nil,
		},
		{
			name:     "one vpod, with placements in 2 pods, not compacted",
			replicas: int32(2),
			vpods: []scheduler.VPod{
				tscheduler.NewVPod(testNs, "vpod-1", 10, []duckv1alpha1.Placement{
					{PodName: "pod-0", VReplicas: int32(8)},
					{PodName: "pod-1", VReplicas: int32(2)}}),
			},
			schedulerPolicy: MAXFILLUP,
			wantEvictions: map[types.NamespacedName]duckv1alpha1.Placement{
				{Name: "vpod-1", Namespace: testNs}: {PodName: "pod-1", VReplicas: int32(2)},
			},
		},
		{
			name:     "multiple vpods, with placements in multiple pods, compacted",
			replicas: int32(2),
			// pod-0:6, pod-1:8, pod-2:7
			vpods: []scheduler.VPod{
				tscheduler.NewVPod(testNs, "vpod-1", 12, []duckv1alpha1.Placement{
					{PodName: "pod-0", VReplicas: int32(4)},
					{PodName: "pod-1", VReplicas: int32(8)}}),
				tscheduler.NewVPod(testNs, "vpod-2", 9, []duckv1alpha1.Placement{
					{PodName: "pod-0", VReplicas: int32(2)},
					{PodName: "pod-2", VReplicas: int32(7)}}),
			},
			schedulerPolicy: MAXFILLUP,
			wantEvictions:   nil,
		},
		{
			name:     "multiple vpods, with placements in multiple pods, not compacted",
			replicas: int32(2),
			// pod-0:6, pod-1:7, pod-2:7
			vpods: []scheduler.VPod{
				tscheduler.NewVPod(testNs, "vpod-1", 6, []duckv1alpha1.Placement{
					{PodName: "pod-0", VReplicas: int32(4)},
					{PodName: "pod-1", VReplicas: int32(7)}}),
				tscheduler.NewVPod(testNs, "vpod-2", 15, []duckv1alpha1.Placement{
					{PodName: "pod-0", VReplicas: int32(2)},
					{PodName: "pod-2", VReplicas: int32(7)}}),
			},
			schedulerPolicy: MAXFILLUP,
			wantEvictions: map[types.NamespacedName]duckv1alpha1.Placement{
				{Name: "vpod-2", Namespace: testNs}: {PodName: "pod-2", VReplicas: int32(7)},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, _ := setupFakeContext(t)

			vpodClient := tscheduler.NewVPodClient()
			ls := listers.NewListers(nil)
			stateAccessor := newStateBuilder(ctx, vpodClient.List, 10, tc.schedulerPolicy, ls.GetNodeLister())

			evictions := make(map[types.NamespacedName]duckv1alpha1.Placement)
			recordEviction := func(vpod scheduler.VPod, from *duckv1alpha1.Placement) error {
				evictions[vpod.GetKey()] = *from
				return nil
			}

			autoscaler := NewAutoscaler(ctx, testNs, sfsName, vpodClient.List, stateAccessor, recordEviction, 10*time.Second, int32(10)).(*autoscaler)

			for _, vpod := range tc.vpods {
				vpodClient.Append(vpod)
			}

			state, err := stateAccessor.State(nil)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			autoscaler.mayCompact(state)

			if tc.wantEvictions == nil && len(evictions) != 0 {
				t.Fatalf("unexpected evictions: %v", evictions)

			}
			for key, placement := range tc.wantEvictions {
				got, ok := evictions[key]
				if !ok {
					t.Fatalf("unexpected %v to be evicted but was not", key)
				}

				if got != placement {
					t.Fatalf("expected evicted placement to be %v, but got %v", placement, got)
				}

				delete(evictions, key)
			}

			if len(evictions) != 0 {
				t.Fatalf("unexpected evictions %v", evictions)
			}
		})
	}
}
