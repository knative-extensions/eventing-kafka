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
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
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
)

const (
	sfsName = "statefulset-name"
	sfsNs   = "statefulset-namespace"
)

func TestStatefulsetScheduler(t *testing.T) {
	ctx, cancel := setupFakeContext(t)

	testCases := []struct {
		name       string
		vpods      []int32
		replicas   int32
		placements []duckv1alpha1.Placement
		err        error
	}{
		{
			name:       "no replicas",
			vpods:      []int32{1},
			replicas:   int32(0),
			err:        scheduler.ErrNoReplicas,
			placements: []duckv1alpha1.Placement{},
		},
		{
			name:       "one vpod, one vreplicas",
			vpods:      []int32{1},
			replicas:   int32(1),
			placements: []duckv1alpha1.Placement{{PodName: "statefulset-name-0", VReplicas: 1}},
		},
		{
			name:       "one vpod, 3 vreplicas",
			vpods:      []int32{3},
			replicas:   int32(1),
			placements: []duckv1alpha1.Placement{{PodName: "statefulset-name-0", VReplicas: 3}},
		},
		{
			name:       "one vpod, 15 vreplicas, partial scheduling",
			vpods:      []int32{15},
			replicas:   int32(1),
			err:        scheduler.ErrNotEnoughReplicas,
			placements: []duckv1alpha1.Placement{{PodName: "statefulset-name-0", VReplicas: 10}},
		},
		{
			name:     "one vpod, 15 vreplicas, full scheduling",
			vpods:    []int32{15},
			replicas: int32(2),
			placements: []duckv1alpha1.Placement{
				{PodName: "statefulset-name-0", VReplicas: 10},
				{PodName: "statefulset-name-1", VReplicas: 5},
			},
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

			for i, vreplicas := range tc.vpods {
				vpodName := fmt.Sprintf("vpod-name-%d", i)
				vpodNamespace := fmt.Sprintf("vpod-ns-%d", i)

				vpod := vpodClient.Create(vpodNamespace, vpodName, vreplicas, nil)
				placements, err := s.Schedule(vpod)

				if tc.err == nil && err != nil {
					t.Fatal("unexpected error", err)
				}

				if tc.err != nil && err == nil {
					t.Fatal("expected error, got none")
				}

				if !reflect.DeepEqual(placements, tc.placements) {
					t.Errorf("got %v, want %v", placements, tc.placements)
				}
			}
		})
	}
	cancel()
	<-ctx.Done()
}

func makeStatefulset(ctx context.Context, ns, name string, replicas int32) *appsv1.StatefulSet {
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

	kc := kubeclient.Get(ctx)
	kc.PrependReactor("get", "statefulsets", func(action gtesting.Action) (handled bool, ret runtime.Object, err error) {
		if action.GetSubresource() == "scale" {
			return true, &autoscalingv1.Scale{
				Spec: autoscalingv1.ScaleSpec{
					Replicas: obj.Status.Replicas,
				},
			}, nil
		}
		return false, nil, nil
	})

	return obj
}

func setupFakeContext(t *testing.T) (context.Context, context.CancelFunc) {
	ctx, cancel, informers := rectesting.SetupFakeContextWithCancel(t)
	err := controller.StartInformers(ctx.Done(), informers...)
	if err != nil {
		t.Fatal("unexpected error", err)
	}

	kc := kubeclient.Get(ctx)
	kc.PrependReactor("get", "statefulsets", func(action gtesting.Action) (handled bool, ret runtime.Object, err error) {
		if action.GetSubresource() == "scale" {
			return true, nil, errors.New("object not found")
		}
		return false, nil, nil
	})

	return ctx, cancel
}
