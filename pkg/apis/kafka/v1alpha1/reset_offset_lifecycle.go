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

package v1alpha1

import (
	"sync"

	"knative.dev/pkg/apis"
)

var cs = apis.NewBatchConditionSet(
	ResetOffsetConditionResetInitiated,
	ResetOffsetConditionConsumerGroupsStopped,
	ResetOffsetConditionOffsetsUpdated,
	ResetOffsetConditionConsumerGroupsStarted)

var condSetLock = sync.RWMutex{}

const (
	// ResetOffsetConditionSucceeded has status True when all sub-conditions below have been set to True.
	ResetOffsetConditionSucceeded = apis.ConditionSucceeded

	// ResetOffsetConditionResetInitiated has status True when the ResetOffset is being processed
	// Until that time it is either "Unknown" when first encountered, or "Blocked" if another
	// ResetOffset is being processed for the same "Ref" resource.
	ResetOffsetConditionResetInitiated apis.ConditionType = "ResetInitiated"

	// ResetOffsetConditionConsumerGroupsStopped has status True when all of the ConsumerGroups
	// associated with the referenced object (Subscription, Trigger, etc.) have been stopped.
	ResetOffsetConditionConsumerGroupsStopped apis.ConditionType = "ConsumerGroupsStopped"

	// ResetOffsetConditionOffsetsUpdated has status True when all of the individual offsets
	// of each Partition in the Topic have been updated to their new values.
	ResetOffsetConditionOffsetsUpdated apis.ConditionType = "OffsetsUpdated"

	// ResetOffsetConditionConsumerGroupsStarted has status True when all of the ConsumerGroups
	// associated with the referenced object (Subscription, Trigger, etc.) have been restarted.
	ResetOffsetConditionConsumerGroupsStarted apis.ConditionType = "ConsumerGroupsStarted"
)

// RegisterAlternateResetOffsetConditionSet register a different apis.ConditionSet.
func RegisterAlternateResetOffsetConditionSet(conditionSet apis.ConditionSet) {
	condSetLock.Lock()
	defer condSetLock.Unlock()
	cs = conditionSet
}

// GetConditionSet retrieves the condition set for this resource. Implements the KRShaped interface.
func (*ResetOffset) GetConditionSet() apis.ConditionSet {
	condSetLock.RLock()
	defer condSetLock.RUnlock()
	return cs
}

// GetConditionSet retrieves the condition set for this resource.
func (*ResetOffsetStatus) GetConditionSet() apis.ConditionSet {
	condSetLock.RLock()
	defer condSetLock.RUnlock()
	return cs
}

// GetCondition returns the condition currently associated with the given type, or nil.
func (ros *ResetOffsetStatus) GetCondition(t apis.ConditionType) *apis.Condition {
	return ros.GetConditionSet().Manage(ros).GetCondition(t)
}

// IsCompleted returns true if the resource is ready overall.
func (ros *ResetOffsetStatus) IsCompleted() bool {
	return ros.GetConditionSet().Manage(ros).IsHappy()
}

// InitializeConditions sets relevant unset conditions to Unknown state.
func (ros *ResetOffsetStatus) InitializeConditions() {
	ros.GetConditionSet().Manage(ros).InitializeConditions()
}

func (ros *ResetOffsetStatus) MarkResetInitiatedFailed(reason, messageFormat string, messageA ...interface{}) {
	ros.GetConditionSet().Manage(ros).MarkFalse(ResetOffsetConditionResetInitiated, reason, messageFormat, messageA...)
}

func (ros *ResetOffsetStatus) MarkResetInitiatedTrue() {
	ros.GetConditionSet().Manage(ros).MarkTrue(ResetOffsetConditionResetInitiated)
}

func (ros *ResetOffsetStatus) MarkConsumerGroupsStoppedFailed(reason, messageFormat string, messageA ...interface{}) {
	ros.GetConditionSet().Manage(ros).MarkFalse(ResetOffsetConditionConsumerGroupsStopped, reason, messageFormat, messageA...)
}

func (ros *ResetOffsetStatus) MarkConsumerGroupsStoppedTrue() {
	ros.GetConditionSet().Manage(ros).MarkTrue(ResetOffsetConditionConsumerGroupsStopped)
}

func (ros *ResetOffsetStatus) MarkOffsetsUpdatedFailed(reason, messageFormat string, messageA ...interface{}) {
	ros.GetConditionSet().Manage(ros).MarkFalse(ResetOffsetConditionOffsetsUpdated, reason, messageFormat, messageA...)
}

func (ros *ResetOffsetStatus) MarkOffsetsUpdatedTrue() {
	ros.GetConditionSet().Manage(ros).MarkTrue(ResetOffsetConditionOffsetsUpdated)
}

func (ros *ResetOffsetStatus) MarkConsumerGroupsStartedFailed(reason, messageFormat string, messageA ...interface{}) {
	ros.GetConditionSet().Manage(ros).MarkFalse(ResetOffsetConditionConsumerGroupsStarted, reason, messageFormat, messageA...)
}

func (ros *ResetOffsetStatus) MarkConsumerGroupsStartedTrue() {
	ros.GetConditionSet().Manage(ros).MarkTrue(ResetOffsetConditionConsumerGroupsStarted)
}

func (ros *ResetOffsetStatus) GetTopic() string {
	return ros.Topic
}

func (ros *ResetOffsetStatus) SetTopic(topic string) {
	ros.Topic = topic
}

func (ros *ResetOffsetStatus) GetGroup() string {
	return ros.Group
}

func (ros *ResetOffsetStatus) SetGroup(group string) {
	ros.Group = group
}

func (ros *ResetOffsetStatus) GetPartitions() []OffsetMapping {
	return ros.Partitions
}

func (ros *ResetOffsetStatus) SetPartitions(offsetMappings []OffsetMapping) {
	ros.Partitions = offsetMappings
}
