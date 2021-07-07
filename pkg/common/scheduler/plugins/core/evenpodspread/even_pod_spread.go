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

package evenpodspread

import (
	"context"
	"encoding/json"
	"strings"

	"k8s.io/apimachinery/pkg/types"
	"knative.dev/eventing-kafka/pkg/common/scheduler/factory"
	state "knative.dev/eventing-kafka/pkg/common/scheduler/state"
	"knative.dev/pkg/logging"
)

// EvenPodSpread is a filter plugin that eliminates pods that do not create an equal spread of resources across pods
type EvenPodSpread struct {
}

// Verify EvenPodSpread Implements FilterPlugin Interface
var _ state.FilterPlugin = &EvenPodSpread{}

// Name of the plugin
const (
	Name                   = state.EvenPodSpread
	ErrReasonInvalidArg    = "invalid arguments"
	ErrReasonUnschedulable = "pod will cause an uneven spread"
)

func init() {
	factory.RegisterFP(Name, &EvenPodSpread{})
}

// Name returns name of the plugin
func (pl *EvenPodSpread) Name() string {
	return Name
}

// Filter invoked at the filter extension point.
func (pl *EvenPodSpread) Filter(ctx context.Context, args interface{}, states *state.State, key types.NamespacedName, podID int32) *state.Status {
	logger := logging.FromContext(ctx).With("Filter", pl.Name())

	spreadArgs, ok := args.(string)
	if !ok {
		logger.Errorf("Filter args %v for predicate %q are not valid", args, pl.Name())
		return state.NewStatus(state.Unschedulable, ErrReasonInvalidArg)
	}

	skewVal := state.EvenPodSpreadArgs{}
	decoder := json.NewDecoder(strings.NewReader(spreadArgs))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&skewVal); err != nil {
		return state.NewStatus(state.Unschedulable, ErrReasonInvalidArg)
	}

	if states.LastOrdinal >= 0 { //need atleast two pods to compute spread
		currentReps := states.PodSpread[key][state.PodNameFromOrdinal(states.StatefulSetName, podID)] //get #vreps on this podID
		var skew int32
		for otherPodID := int32(0); otherPodID < states.LastOrdinal; otherPodID++ { //compare with #vreps on other pods
			if otherPodID != podID {
				otherReps := states.PodSpread[key][state.PodNameFromOrdinal(states.StatefulSetName, otherPodID)]
				if skew = (currentReps + 1) - otherReps; skew < 0 {
					skew = skew * int32(-1)
				}

				logger.Infof("Current Pod %d with %d and Other Pod %d with %d causing skew %d", podID, currentReps, otherPodID, otherReps, skew)
				if skew > skewVal.MaxSkew {
					logger.Infof("Pod %d will cause an uneven spread", podID)
					return state.NewStatus(state.Unschedulable, ErrReasonUnschedulable)
				}
			}
		}
	}

	logger.Infof("Pod %d passed %q predicate successfully", podID, pl.Name())
	return state.NewStatus(state.Success)

}
