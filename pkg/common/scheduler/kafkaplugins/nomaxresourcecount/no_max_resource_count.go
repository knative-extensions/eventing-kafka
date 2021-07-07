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

package nomaxresourcecount

import (
	"context"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	core "knative.dev/eventing-kafka/pkg/common/scheduler/core"
)

// NoMaxResourceCount plugin filters pods that cause total pods with placements
// to exceed total partitioncount
type NoMaxResourceCount struct {
}

var _ core.FilterPlugin = &NoMaxResourceCount{}

// Name of the plugin
const Name = core.NoMaxResourceCount

const (
	// ErrReasonUnknownCondition is used for NodeUnknownCondition predicate error.
	ErrReasonUnknownCondition = "pod(s) had unknown conditions"
	// ErrReasonUnschedulable is used for NodeUnschedulable predicate error.
	ErrReasonUnschedulable = "pod(s) were unschedulable"
)

// Name returns name of the plugin. It is used in logs, etc.
func (pl *NoMaxResourceCount) Name() string {
	return Name
}

// Filter invoked at the filter extension point.
func (pl *NoMaxResourceCount) Filter(ctx context.Context, pod *v1.Pod) *core.Status {
	/* 	if nodeInfo == nil || nodeInfo.Node() == nil {
	   		return framework.NewStatus(framework.UnschedulableAndUnresolvable, ErrReasonUnknownCondition)
	   	}
	   	// If pod tolerate unschedulable taint, it's also tolerate `node.Spec.Unschedulable`.
	   	podToleratesUnschedulable := v1helper.TolerationsTolerateTaint(pod.Spec.Tolerations, &v1.Taint{
	   		Key:    v1.TaintNodeUnschedulable,
	   		Effect: v1.TaintEffectNoSchedule,
	   	})
	   	// TODO (k82cn): deprecates `node.Spec.Unschedulable` in 1.13.
	   	if nodeInfo.Node().Spec.Unschedulable && !podToleratesUnschedulable {
	   		return framework.NewStatus(framework.UnschedulableAndUnresolvable, ErrReasonUnschedulable)
	   	} */
	return nil
}

// New initializes a new plugin and returns it.
func New(_ runtime.Object) (core.Plugin, error) {
	return &NoMaxResourceCount{}, nil
}
