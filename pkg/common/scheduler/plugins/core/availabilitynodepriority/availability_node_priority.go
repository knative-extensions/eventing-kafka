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
	"encoding/json"
	"math"
	"strings"

	"k8s.io/apimachinery/pkg/types"
	"knative.dev/eventing-kafka/pkg/common/scheduler/factory"
	state "knative.dev/eventing-kafka/pkg/common/scheduler/state"
	"knative.dev/pkg/logging"
)

// AvailabilityNodePriority is a score plugin that favors pods that create an even spread of resources across nodes for HA
type AvailabilityNodePriority struct {
}

// Verify AvailabilityNodePriority Implements ScorePlugin Interface
var _ state.ScorePlugin = &AvailabilityNodePriority{}

// Name of the plugin
const Name = state.AvailabilityNodePriority

const (
	// When zone information is present, give 1/3 of the weighting to zone spreading, 2/3 to node spreading
	zoneWeighting       float64 = 1.0 / 3.0
	ErrReasonInvalidArg         = "invalid arguments"
	ErrReasonNoResource         = "node or zone does not exist"
)

func init() {
	factory.RegisterSP(Name, &AvailabilityNodePriority{})
}

// Name returns name of the plugin
func (pl *AvailabilityNodePriority) Name() string {
	return Name
}

// Score invoked at the score extension point. The "score" returned in this function is higher for pods with lower ordinals and higher free capacity
// It is normalized later with node info
func (pl *AvailabilityNodePriority) Score(ctx context.Context, args interface{}, states *state.State, key types.NamespacedName, podID int32) (uint64, *state.Status) {
	logger := logging.FromContext(ctx).With("Score", pl.Name())
	var score uint64 = 0

	spreadArgs, ok := args.(string)
	if !ok {
		logger.Errorf("Scoring args %v for predicate %q are not valid", args, pl.Name())
		return 0, state.NewStatus(state.Unschedulable, ErrReasonInvalidArg)
	}

	skewVal := state.AvailabilityNodePriorityArgs{}
	decoder := json.NewDecoder(strings.NewReader(spreadArgs))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&skewVal); err != nil {
		return 0, state.NewStatus(state.Unschedulable, ErrReasonInvalidArg)
	}

	if states.LastOrdinal >= 0 {
		var skew int32
		_, nodeName, err := states.GetPodInfo(state.PodNameFromOrdinal(states.StatefulSetName, podID))
		if err != nil {
			return score, state.NewStatus(state.Error, ErrReasonNoResource)
		}

		currentReps := states.NodeSpread[key][nodeName]   //get #vreps on this node
		for otherNodeName := range states.NodeToZoneMap { //compare with #vreps on other nodes
			if otherNodeName != nodeName {
				otherReps := states.NodeSpread[key][otherNodeName]
				if skew = (currentReps + 1) - otherReps; skew < 0 {
					skew = skew * int32(-1)
				}

				logger.Infof("Current Node %v with %d and Other Node %v with %d causing skew %d", nodeName, currentReps, otherNodeName, otherReps, skew)
				if skew > skewVal.MaxSkew {
					logger.Infof("Pod %d will cause an uneven node spread", podID)
				}
				score = score + uint64(skew)
			}
		}

		score = math.MaxUint64 - score //lesser skews get higher score
	}

	logger.Infof("Pod %v scored by %q priority successfully with score %v", podID, pl.Name(), score)
	return score, state.NewStatus(state.Success)
}

// ScoreExtensions of the Score plugin.
func (pl *AvailabilityNodePriority) ScoreExtensions() state.ScoreExtensions {
	return pl
}

// NormalizeScore invoked after scoring all pods.
func (pl *AvailabilityNodePriority) NormalizeScore(ctx context.Context, states *state.State, scores state.PodScoreList) *state.Status {
	return nil
}
