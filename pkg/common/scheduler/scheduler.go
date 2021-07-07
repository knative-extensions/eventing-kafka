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
	ErrNotEnoughReplicas = errors.New("scheduling failed (not enough pod replicas)")
)

type SchedulerPolicyType string

const (
	// MAXFILLUP policy type adds vreplicas to existing pods to fill them up before adding to new pods
	MAXFILLUP SchedulerPolicyType = "MAXFILLUP"
	// EVENSPREAD policy type spreads vreplicas uniformly across zones to reduce impact of failure
	EVENSPREAD = "EVENSPREAD"
	// EVENSPREAD_BYNODE policy type spreads vreplicas uniformly across nodes to reduce impact of failure
	EVENSPREAD_BYNODE = "EVENSPREAD_BYNODE"
)

const (
	ZoneLabel = "topology.kubernetes.io/zone"
)

// VPodLister is the function signature for returning a list of VPods
type VPodLister func() ([]VPod, error)

// Evictor allows for vreplicas to be evicted.
// For instance, the evictor is used by the statefulset scheduler to
// move vreplicas to pod with a lower ordinal.
type Evictor func(vpod VPod, from *duckv1alpha1.Placement) error

// Scheduler is responsible for placing VPods into real Kubernetes pods
type Scheduler interface {
	// Schedule computes the new set of placements for vpod.
	Schedule(vpod VPod) ([]duckv1alpha1.Placement, error)
}

// VPod represents virtual replicas placed into real Kubernetes pods
// The scheduler is responsible for placing VPods
type VPod interface {
	// GetKey returns the VPod key (namespace/name).
	GetKey() types.NamespacedName

	// GetVReplicas returns the number of expected virtual replicas
	GetVReplicas() int32

	// GetPlacements returns the current list of placements
	// Do not mutate!
	GetPlacements() []duckv1alpha1.Placement
}
