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

package testing

import (
	"k8s.io/apimachinery/pkg/types"
	duckv1alpha1 "knative.dev/eventing-kafka/pkg/apis/duck/v1alpha1"
)

type sampleVPod struct {
	key        types.NamespacedName
	vreplicas  int32
	placements []duckv1alpha1.Placement
}

func newVPod(ns, name string, vreplicas int32, placements []duckv1alpha1.Placement) *sampleVPod {
	return &sampleVPod{
		key: types.NamespacedName{
			Namespace: ns,
			Name:      name,
		},
		vreplicas:  vreplicas,
		placements: placements,
	}
}

func (d *sampleVPod) GetKey() types.NamespacedName {
	return d.key
}

func (d *sampleVPod) GetVReplicas() int32 {
	return d.vreplicas
}

func (d *sampleVPod) GetPlacements() []duckv1alpha1.Placement {
	return d.placements
}
