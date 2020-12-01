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
	"errors"

	"k8s.io/apimachinery/pkg/types"

	duckv1alpha1 "knative.dev/eventing-kafka/pkg/apis/duck/v1alpha1"
)

var (
	ErrUnschedulable     = errors.New("scheduling failed (no replicas available)")
	ErrPartialScheduling = errors.New("resource is partially schedulable (not enough pod replicas)")
)

type SchedulableLister func() ([]Schedulable, error)

type Scheduler interface {
	// Schedule computes the new set of placements for the schedulable.
	Schedule(schedulable Schedulable) ([]duckv1alpha1.Placement, error)
}

type Schedulable interface {
	// GetKey returns the schedulable key.
	GetKey() types.NamespacedName

	// The number of virtual replicas to place.
	GetReplicas() int32

	// GetPlacements returns where the schedulable is currently placed.
	// Make sure to copy the slice before mutating it.
	GetPlacements() []duckv1alpha1.Placement
}
