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

package core

import (
	"context"
	"errors"
	"math"
	"strings"

	v1 "k8s.io/api/core/v1"
)

const (
	// MaxPodScore is the maximum score a Score plugin is expected to return.
	MaxPodScore int64 = 100

	// MinPodScore is the minimum score a Score plugin is expected to return.
	MinPodScore int64 = 0

	// MaxTotalScore is the maximum total score.
	MaxTotalScore int64 = math.MaxInt64

	//MaxWeight int64
)

const (
	PodFitsResources         = "PodFitsResources"
	NoMaxResourceCount       = "NoMaxResourceCount"
	EvenPodSpreadPriority    = "EvenPodSpreadPriority"
	AvailabilityNodePriority = "AvailabilityNodePriority"
	AvailabilityZonePriority = "AvailabilityZonePriority"
	LowestOrdinalPriority    = "LowestOrdinalPriority"
)

const (
	// SchedulerPolicyConfigMapKey defines the key of the element in the
	// scheduler's policy ConfigMap that contains scheduler's policy config.
	SchedulerPolicyConfigMapKey = "policy.cfg"
)

// Policy describes a struct of a policy resource in api.
type SchedulerPolicy struct {
	// Holds the information to configure the fit predicate functions.
	// If unspecified, the default predicate functions will be applied.
	// If empty list, all predicates (except the mandatory ones) will be
	// bypassed.
	Predicates []PredicatePolicy
	// Holds the information to configure the priority functions.
	// If unspecified, the default priority functions will be applied.
	// If empty list, all priority functions will be bypassed.
	Priorities []PriorityPolicy
}

// PredicatePolicy describes a struct of a predicate policy.
type PredicatePolicy struct {
	// Identifier of the predicate policy
	// For a custom predicate, the name can be user-defined
	// For the Kubernetes provided predicates, the name is the identifier of the pre-defined predicate
	Name string
	// Holds the parameters to configure the given predicate
	//Argument *PredicateArgument
}

// PriorityPolicy describes a struct of a priority policy.
type PriorityPolicy struct {
	// Identifier of the priority policy
	// For a custom priority, the name can be user-defined
	// For the Kubernetes provided priority functions, the name is the identifier of the pre-defined priority function
	Name string
	// The numeric multiplier for the node scores that the priority function generates
	// The weight should be a positive integer
	Weight int64
	// Holds the parameters to configure the given priority function
	//Argument *PriorityArgument
}

type SchedulerPlugins struct {
	// Filter is a list of plugins that should be invoked when filtering out nodes that cannot run the Pod.
	Filter []Plugins

	// Score is a list of plugins that should be invoked when ranking nodes that have passed the filtering phase.
	Score []Plugins
}

type Plugins struct {
	// Name defines the name of plugin
	Name string
	// Weight defines the weight of plugin, only used for Score plugins.
	Weight int32
}

// Code is the Status code/type which is returned from plugins.
type Code int

// These are predefined codes used in a Status.
const (
	// Success means that plugin ran correctly and found pod schedulable.
	// NOTE: A nil status is also considered as "Success".
	Success Code = iota
	// Error is used for internal plugin errors, unexpected input, etc.
	Error
	// Unschedulable is used when a plugin finds a pod unschedulable. The scheduler might attempt to
	// preempt other pods to get this pod scheduled. Use UnschedulableAndUnresolvable to make the
	// scheduler skip preemption.
	// The accompanying status message should explain why the pod is unschedulable.
	Unschedulable
)

// Status indicates the result of running a plugin.
type Status struct {
	code    Code
	reasons []string
	err     error
}

// Code returns code of the Status.
func (s *Status) Code() Code {
	if s == nil {
		return Success
	}
	return s.code
}

// Message returns a concatenated message on reasons of the Status.
func (s *Status) Message() string {
	if s == nil {
		return ""
	}
	return strings.Join(s.reasons, ", ")
}

// NewStatus makes a Status out of the given arguments and returns its pointer.
func NewStatus(code Code, reasons ...string) *Status {
	s := &Status{
		code:    code,
		reasons: reasons,
	}
	if code == Error {
		s.err = errors.New(s.Message())
	}
	return s
}

// AsStatus wraps an error in a Status.
func AsStatus(err error) *Status {
	return &Status{
		code:    Error,
		reasons: []string{err.Error()},
		err:     err,
	}
}

// ExtensionPoint encapsulates desired and applied set of plugins at a specific extension
// point. This is used to simplify iterating over all extension points supported by the
// frameworkImpl.
type ExtensionPoint struct {
	// the set of plugins to be configured at this extension point.
	Plugins []Plugins
	// a pointer to the slice storing plugins implementations that will run at this
	// extension point.
	SlicePtr interface{}
}

// Plugin is the parent type for all the scheduling framework plugins.
type Plugin interface {
	Name() string
}

type FilterPlugin interface {
	Plugin
	// Filter is called by the scheduling framework.
	// All FilterPlugins should return "Success" to declare that
	// the given pod fits the vreplica.
	Filter(ctx context.Context, stateAccessor StateAccessor, pod *v1.Pod) *Status
}

type PodScore struct {
	Name  string
	Score int64
}

type PodScoreList []PodScore

// PluginToPodScores declares a map from plugin name to its PodScoreList.
type PluginToPodScores map[string]PodScoreList

type ScorePlugin interface {
	Plugin
	// Filter is called by the scheduling framework.
	// All FilterPlugins should return "Success" to declare that
	// the given pod fits the vreplica.
	Score(ctx context.Context, stateAccessor StateAccessor, pod *v1.Pod) (int64, *Status)
}

// PluginToStatus maps plugin name to status. Currently used to identify which Filter plugin
// returned which status.
type PluginToStatus map[string]*Status

// PluginsRunner abstracts operations to run some plugins.
type PluginsRunner interface {
	// RunScorePlugins runs the set of configured Score plugins. It returns a map that
	// stores for each Score plugin name the corresponding NodeScoreList(s).
	// It also returns *Status, which is set to non-success if any of the plugins returns
	// a non-success status.
	RunScorePlugins(context.Context, *v1.Pod) (PluginToPodScores, *Status)
	// RunFilterPlugins runs the set of configured Filter plugins for pod on
	// the given node.
	RunFilterPlugins(context.Context, *v1.Pod) PluginToStatus
}
