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
	duckv1alpha1 "knative.dev/eventing-kafka/pkg/apis/duck/v1alpha1"
)

type Scheduler interface {
	EnsureInitialized(SchedulableLister) error
	Schedule(schedulable Schedulable) []duckv1alpha1.Placement
}

type SchedulableLister func() ([]Schedulable, error)

type Schedulable interface {
	// The number of replicas to place.
	GetReplicas() int32

	// GetPlacements returns where the schedulable is currently placed.
	GetPlacements() []duckv1alpha1.Placement

	// MarkAllUnschedulable indicates that none of the replicas can be scheduled.
	MarkAllUnschedulable()

	// MarkUnschedulable indicates that the given replica cannot be scheduled.
	MarkUnschedulable(replica int32)

	// Place allocates the number of replicas to podName
	Place(podName string, replicas int32)
}
