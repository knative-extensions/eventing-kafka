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

package podfitsresources

import (
	"context"

	"go.uber.org/zap"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	core "knative.dev/eventing-kafka/pkg/common/scheduler/core"
	"knative.dev/pkg/logging"
)

// PodFitsResources plugin filters pods that do not have sufficient free capacity
// for a vreplica to be placed on it
type PodFitsResources struct {
}

var _ core.FilterPlugin = &PodFitsResources{}

// Name of the plugin
const Name = core.PodFitsResources

const (
	// ErrReasonUnknownCondition is used for NodeUnknownCondition predicate error.
	ErrReasonUnknownCondition = "pod(s) had unknown conditions"
	// ErrReasonUnschedulable is used for NodeUnschedulable predicate error.
	ErrReasonUnschedulable = "pod at full capacity"
)

// Name returns name of the plugin
func (pl *PodFitsResources) Name() string {
	return Name
}

// Filter invoked at the filter extension point.
func (pl *PodFitsResources) Filter(ctx context.Context, stateAccessor core.StateAccessor, pod *v1.Pod) *core.Status {
	logger := logging.FromContext(ctx).With("Filter", pl.Name())
	state, err := stateAccessor.State(nil)
	if err != nil {
		logger.Info("error while refreshing scheduler state (will retry)", zap.Error(err))
		return core.AsStatus(err)
	}

	ordinal := core.OrdinalFromPodName(pod.Name)
	if state.FreeCap[ordinal] == 0 {
		return core.NewStatus(core.Unschedulable, ErrReasonUnschedulable)
	}
	return nil
}

// New initializes a new plugin and returns it.
func New(_ runtime.Object) (core.Plugin, error) {
	return &PodFitsResources{}, nil
}
