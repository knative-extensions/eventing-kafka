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

package v1beta1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/pointer"
	"knative.dev/eventing-kafka/pkg/apis/duck/v1alpha1"
	"reflect"
	"testing"
)

func TestScheduling(t *testing.T) {
	testCases := map[string]struct {
		source     KafkaSource
		key        types.NamespacedName
		vreplicas  int32
		placements []v1alpha1.Placement
	}{
		"all empty": {
			source:     KafkaSource{},
			key:        types.NamespacedName{},
			vreplicas:  int32(1),
			placements: nil,
		},
		"all full": {
			source: KafkaSource{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "asource",
					Namespace: "anamespace",
				},
				Spec: KafkaSourceSpec{
					Consumers: pointer.Int32Ptr(4),
				},
				Status: KafkaSourceStatus{
					Placeable: v1alpha1.Placeable{Placement: []v1alpha1.Placement{
						{PodName: "apod", VReplicas: 4},
					}},
				},
			},
			key: types.NamespacedName{
				Namespace: "anamespace",
				Name:      "asource",
			},
			vreplicas: int32(4),
			placements: []v1alpha1.Placement{
				{PodName: "apod", VReplicas: 4},
			},
		},
	}

	for n, tc := range testCases {
		tc := tc
		t.Run(n, func(t *testing.T) {
			t.Parallel()

			if !reflect.DeepEqual(tc.source.GetKey(), tc.key) {
				t.Errorf("unexpected key (want %v, got %v)", tc.key, tc.source.GetKey())
			}
			if tc.source.GetVReplicas() != tc.vreplicas {
				t.Errorf("unexpected vreplicas (want %d, got %d)", tc.vreplicas, tc.source.GetVReplicas())
			}
			if !reflect.DeepEqual(tc.source.GetPlacements(), tc.placements) {
				t.Errorf("unexpected placements (want %v, got %v)", tc.placements, tc.source.GetPlacements())
			}
		})
	}
}
