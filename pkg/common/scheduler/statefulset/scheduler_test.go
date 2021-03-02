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
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	gtesting "k8s.io/client-go/testing"

	kubeclient "knative.dev/pkg/client/injection/kube/client/fake"
	_ "knative.dev/pkg/client/injection/kube/informers/apps/v1/statefulset/fake"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"
	rectesting "knative.dev/pkg/reconciler/testing"

	duckv1alpha1 "knative.dev/eventing-kafka/pkg/apis/duck/v1alpha1"
	"knative.dev/eventing-kafka/pkg/common/scheduler"
	tscheduler "knative.dev/eventing-kafka/pkg/common/scheduler/testing"
)

const (
	sfsName = "statefulset-name"
)

func init() {

}

func TestStatefulsetScheduler(t *testing.T) {
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

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, _ := setupFakeContext(t)

			vpodClient := tscheduler.NewVPodClient()

			_, err := kubeclient.Get(ctx).AppsV1().StatefulSets(testNs).Create(ctx, makeStatefulset(ctx, testNs, sfsName, tc.replicas), metav1.CreateOptions{})
			if err != nil {
				t.Fatal("unexpected error", err)
			}
			sa := newStateBuilder(logging.FromContext(ctx), vpodClient.List, 10)
			s := NewStatefulSetScheduler(ctx, testNs, sfsName, vpodClient.List, sa, nil).(*StatefulSetScheduler)

			// Give some time for the informer to notify the scheduler and set the number of replicas
			time.Sleep(200 * time.Millisecond)

			func() {
				s.lock.Lock()
				defer s.lock.Unlock()
				if s.replicas != tc.replicas {
					t.Fatalf("expected number of statefulset replica to be %d (got %d)", tc.replicas, s.replicas)
				}
			}()

			for i, vreplicas := range tc.vpods {
				vpodName := fmt.Sprint("vpod-name-", i)
				vpodNamespace := fmt.Sprint("vpod-ns-", i)

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
