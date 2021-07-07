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

package statefulset

const (
	// MaxWeight is the maximum weight that can be assigned for a priority.
	MaxWeight uint64 = 10
	// MinWeight is the minimum weight that can be assigned for a priority.
	MinWeight uint64 = 0
)

// Policy describes a struct of a policy resource.
type SchedulerPolicy struct {
	// Holds the information to configure the fit predicate functions.
	Predicates []PredicatePolicy
	// Holds the information to configure the priority functions.
	Priorities []PriorityPolicy
}

// PredicatePolicy describes a struct of a predicate policy.
type PredicatePolicy struct {
	// Identifier of the predicate policy
	Name string
	// Holds the parameters to configure the given predicate
	Args interface{}
}

// PriorityPolicy describes a struct of a priority policy.
type PriorityPolicy struct {
	// Identifier of the priority policy
	Name string
	// The numeric multiplier for the pod scores that the priority function generates
	// The weight should be a positive integer
	Weight uint64
	// Holds the parameters to configure the given priority function
	Args interface{}
}
