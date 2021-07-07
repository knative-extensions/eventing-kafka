/*
Copyright 2021 The Knative Authors

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

package availabilitynodepriority

import (
	"context"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	core "knative.dev/eventing-kafka/pkg/common/scheduler/core"
)

// AvailabilityNodePriority is a score plugin that favors pods that create an even spread of resources across nodes for HA
type AvailabilityNodePriority struct {
}

var _ core.ScorePlugin = &AvailabilityNodePriority{}

// Name of the plugin
const Name = core.AvailabilityNodePriority

// Name returns name of the plugin
func (pl *AvailabilityNodePriority) Name() string {
	return Name
}

// Score invoked at the score extension point.
func (pl *AvailabilityNodePriority) Score(ctx context.Context, pod *v1.Pod) (int64, *core.Status) {
	score := calculatePriority()

	return score, nil
}

// New initializes a new plugin and returns it.
func New(_ runtime.Object) (core.Plugin, error) {
	return &AvailabilityNodePriority{}, nil
}

// calculatePriority returns the priority of a pod. Given the ...
func calculatePriority() int64 {
	return 0
}
