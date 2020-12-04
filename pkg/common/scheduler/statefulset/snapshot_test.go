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

	"knative.dev/pkg/logging"

	duckv1alpha1 "knative.dev/eventing-kafka/pkg/apis/duck/v1alpha1"
	tscheduler "knative.dev/eventing-kafka/pkg/common/scheduler/testing"
	kubeclient "knative.dev/pkg/client/injection/kube/client/fake"
)

func TestSnapshot(t *testing.T) {
	ctx, cancel := setupFakeContext(t)

	testCases := []struct {
		name     string
		vpods    [][]duckv1alpha1.Placement
		replicas int32
		expected Snapshot
		err      error
	}{
		{
			name:     "no vpods, no replicas",
			vpods:    [][]duckv1alpha1.Placement{},
			replicas: int32(0),
			expected: Snapshot{free: map[string]int32{}},
		},
		{
			name:     "no vpods, one replica",
			vpods:    [][]duckv1alpha1.Placement{},
			replicas: int32(1),
			expected: Snapshot{free: map[string]int32{"statefulset-name-0": int32(10)}},
		},
		{
			name:     "one vpods, one replica",
			vpods:    [][]duckv1alpha1.Placement{{{PodName: "statefulset-name-0", VReplicas: 1}}},
			replicas: int32(1),
			expected: Snapshot{free: map[string]int32{"statefulset-name-0": int32(9)}},
		},
		{
			name: "many vpods, many replicas",
			vpods: [][]duckv1alpha1.Placement{
				{{PodName: "statefulset-name-0", VReplicas: 1}, {PodName: "statefulset-name-2", VReplicas: 5}},
				{{PodName: "statefulset-name-1", VReplicas: 2}},
				{{PodName: "statefulset-name-1", VReplicas: 3}, {PodName: "statefulset-name-0", VReplicas: 1}},
			},
			replicas: int32(4),
			expected: Snapshot{free: map[string]int32{
				"statefulset-name-0": int32(8),
				"statefulset-name-1": int32(5),
				"statefulset-name-2": int32(5),
				"statefulset-name-3": int32(10)}},
		},
	}

	for i, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			vpodClient := tscheduler.NewVPodClient()
			sfsNs := fmt.Sprintf("ns-%d", i)

			_, err := kubeclient.Get(ctx).AppsV1().StatefulSets(sfsNs).Create(ctx, makeStatefulset(ctx, sfsNs, sfsName, tc.replicas), metav1.CreateOptions{})
			if err != nil {
				t.Fatal("unexpected error", err)
			}

			s := NewStatefulSetScheduler(ctx, sfsNs, sfsName, vpodClient.List, nil).(*StatefulSetScheduler)

			// Give some time for the informer to notify the scheduler
			time.Sleep(500 * time.Millisecond)

			func() {
				s.lock.Lock()
				defer s.lock.Unlock()
				if s.replicas != tc.replicas {
					t.Fatalf("expected number of statefulset replica to be %d (got %d)", tc.replicas, s.replicas)
				}
			}()

			for i, placements := range tc.vpods {
				vpodName := fmt.Sprintf("vpod-name-%d", i)
				vpodNamespace := fmt.Sprintf("vpod-ns-%d", i)

				vpodClient.Create(vpodNamespace, vpodName, 1, placements)

			}

			shot, err := s.snapshot(logging.FromContext(ctx))
			if err != nil {
				t.Fatal("unexpected error", err)
			}

			if !reflect.DeepEqual(*shot, tc.expected) {
				t.Errorf("got %v, want %v", shot, tc.expected)
			}
		})
	}

	cancel()
	<-ctx.Done()
}
