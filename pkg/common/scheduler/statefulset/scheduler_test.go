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

	"knative.dev/eventing-kafka/pkg/common/scheduler"

	duckv1alpha1 "knative.dev/eventing-kafka/pkg/apis/duck/v1alpha1"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	gtesting "k8s.io/client-go/testing"
	"k8s.io/utils/pointer"
	kubeclient "knative.dev/pkg/client/injection/kube/client/fake"
	_ "knative.dev/pkg/client/injection/kube/informers/apps/v1/statefulset/fake"
	"knative.dev/pkg/controller"
	rectesting "knative.dev/pkg/reconciler/testing"

	tscheduler "knative.dev/eventing-kafka/pkg/common/scheduler/testing"
)

const (
	sfsName = "statefulset-name"
	sfsNs   = "statefulset-namespace"
)

func TestNoStatefulsetScheduler(t *testing.T) {
	ctx := setupFakeContext(t)
	vpodClient := tscheduler.NewVPodClient()

	s := NewStatefulSetScheduler(ctx, sfsNs, sfsName, vpodClient.List, nil)

	vpod1 := vpodClient.Create("test-ns", "vpod1", 1)
	_, err := s.Schedule(vpod1)
	if err == nil {
		t.Error("expected error")
	}
}

func TestStatefulsetScheduler(t *testing.T) {
	ctx := setupFakeContext(t)

	testCases := []struct {
		name       string
		vreplicas  []int32
		placements []duckv1alpha1.Placement
		err        error
	}{
		{
			name:       "one vpod, one vreplicas",
			vreplicas:  []int32{1},
			placements: []duckv1alpha1.Placement{{"statefulset-name-0", 1}},
		},
		{
			name:       "one vpod, 3 vreplicas",
			vreplicas:  []int32{3},
			placements: []duckv1alpha1.Placement{{"statefulset-name-0", 3}},
		},
		{
			name:       "one vpod, 15 vreplicas, partial scheduling",
			vreplicas:  []int32{15},
			err:        scheduler.ErrNotEnoughReplicas,
			placements: []duckv1alpha1.Placement{{"statefulset-name-0", 10}},
		},
	}

	for i, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			vpodClient := tscheduler.NewVPodClient()
			sfsNs := fmt.Sprintf("ns-%d", i)

			_, err := kubeclient.Get(ctx).AppsV1().StatefulSets(sfsNs).Create(ctx, makeStatefulset(ctx, sfsNs, sfsName), metav1.CreateOptions{})
			if err != nil {
				t.Fatal("unexpected error", err)
			}

			s := NewStatefulSetScheduler(ctx, sfsNs, sfsName, vpodClient.List, nil)

			// Give some time for the informer to notify the scheduler
			time.Sleep(500 * time.Millisecond)

			if s.(*StatefulSetScheduler).replicas != 1 {
				t.Fatalf("expected number of statefulset replica to be 1 (got %d)", s.(*StatefulSetScheduler).replicas)
			}

			for i, vreplicas := range tc.vreplicas {
				vpodName := fmt.Sprintf("vpod-name-%d", i)
				vpodNamespace := fmt.Sprintf("vpod-ns-%d", i)

				vpod := vpodClient.Create(vpodNamespace, vpodName, vreplicas)
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
}

func makeStatefulset(ctx context.Context, ns, name string) *appsv1.StatefulSet {
	obj := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas: pointer.Int32Ptr(1),
		},
		Status: appsv1.StatefulSetStatus{
			Replicas: 1,
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

func setupFakeContext(t *testing.T) context.Context {
	ctx, informers := rectesting.SetupFakeContext(t)
	err := controller.StartInformers(ctx.Done(), informers...)
	if err != nil {
		t.Fatal("unexpected error", err)
	}
	return ctx
}
