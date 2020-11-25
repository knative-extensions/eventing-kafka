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
	"knative.dev/eventing-kafka/pkg/apis/duck/v1alpha1"
)

func (k *KafkaSource) GetReplicas() int32 {
	if k.Spec.Consumers == nil {
		return 1
	}
	return *k.Spec.Consumers
}

func (k *KafkaSource) MarkAllUnschedulable() {
	// TODO: add condition
}

func (k *KafkaSource) MarkUnschedulable(replica int32) {
	// TODO: add condition
}

func (k *KafkaSource) Place(podName string, replicas int32) {
	placements := k.Status.Placeable.Placement
	for _, placement := range placements {
		if placement.PodName == podName {
			placement.Replicas = replicas
			return
		}
	}

	k.Status.Placeable.Placement = append(placements, v1alpha1.Placement{
		PodName:  podName,
		Replicas: replicas,
	})
}

func (k *KafkaSource) GetPlacements() []v1alpha1.Placement {
	if k.Status.Placeable.Placement == nil {
		return nil
	}
	return k.Status.Placeable.Placement
}
