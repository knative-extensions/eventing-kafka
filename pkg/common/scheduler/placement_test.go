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

package scheduler

import (
	"testing"

	duckv1alpha1 "knative.dev/eventing-kafka/pkg/apis/duck/v1alpha1"
)

func TestGetTotalVReplicas(t *testing.T) {
	testCases := []struct {
		name       string
		placements []duckv1alpha1.Placement
		vreplicas  int
	}{
		{
			name:       "nil placements",
			placements: nil,
			vreplicas:  0,
		},
		{
			name:       "empty placements",
			placements: []duckv1alpha1.Placement{},
			vreplicas:  0,
		},
		{
			name:       "one placement",
			placements: []duckv1alpha1.Placement{{PodName: "d", VReplicas: 2}},
			vreplicas:  2,
		},
		{
			name: "many placements",
			placements: []duckv1alpha1.Placement{
				{PodName: "d", VReplicas: 2},
				{PodName: "d", VReplicas: 6},
				{PodName: "d", VReplicas: 0}},
			vreplicas: 8,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			vreplicas := GetTotalVReplicas(tc.placements)
			if vreplicas != int32(tc.vreplicas) {
				t.Errorf("got %d, want %d", vreplicas, tc.vreplicas)
			}
		})
	}
}

func TestGetPlacementForPod(t *testing.T) {
	ps1 := []duckv1alpha1.Placement{{PodName: "p", VReplicas: 2}}
	ps2 := []duckv1alpha1.Placement{{PodName: "p", VReplicas: 2}, {PodName: "p2", VReplicas: 4}}
	testCases := []struct {
		name       string
		podName    string
		placements []duckv1alpha1.Placement
		expected   *duckv1alpha1.Placement
	}{
		{
			name:       "nil placements",
			podName:    "p",
			placements: nil,
			expected:   nil,
		},
		{
			name:       "empty placements",
			podName:    "p",
			placements: []duckv1alpha1.Placement{},
			expected:   nil,
		},
		{
			name:       "one placement",
			placements: ps1,
			podName:    "p",
			expected:   &ps1[0],
		}, {
			name:       "mayne placements",
			placements: ps2,
			podName:    "p2",
			expected:   &ps2[1],
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := GetPlacementForPod(tc.placements, tc.podName)
			if got != tc.expected {
				t.Errorf("got %v, want %v", got, tc.expected)
			}
		})
	}
}
