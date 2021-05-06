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
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
)

var (
	condSucceeded             = apis.Condition{Type: ResetOffsetConditionSucceeded, Status: corev1.ConditionTrue}
	condResetInitiated        = apis.Condition{Type: ResetOffsetConditionResetInitiated, Status: corev1.ConditionTrue}
	condConsumerGroupsStopped = apis.Condition{Type: ResetOffsetConditionConsumerGroupsStopped, Status: corev1.ConditionTrue}
	condOffsetsUpdated        = apis.Condition{Type: ResetOffsetConditionOffsetsUpdated, Status: corev1.ConditionTrue}
	condConsumerGroupsStarted = apis.Condition{Type: ResetOffsetConditionConsumerGroupsStarted, Status: corev1.ConditionTrue}
)

func TestResetOffset_GetConditionSet(t *testing.T) {
	ro := &ResetOffset{}
	if got, want := ro.GetConditionSet().GetTopLevelConditionType(), apis.ConditionSucceeded; got != want {
		t.Errorf("GetTopLevelCondition=%v, want=%v", got, want)
	}
}

func TestResetOffsetStatus_GetCondition(t *testing.T) {
	tests := []struct {
		name          string
		status        *ResetOffsetStatus
		conditionType apis.ConditionType
		wantCondition *apis.Condition
	}{
		{
			name: "ResetOffsetConditionSucceeded",
			status: &ResetOffsetStatus{
				Status: duckv1.Status{
					Conditions: []apis.Condition{condSucceeded},
				},
			},
			conditionType: ResetOffsetConditionSucceeded,
			wantCondition: &condSucceeded,
		},
		{
			name: "ResetOffsetConditionResetInitiated",
			status: &ResetOffsetStatus{
				Status: duckv1.Status{
					Conditions: []apis.Condition{condResetInitiated},
				},
			},
			conditionType: ResetOffsetConditionResetInitiated,
			wantCondition: &condResetInitiated,
		},
		{
			name: "ResetOffsetConditionConsumerGroupsStopped",
			status: &ResetOffsetStatus{
				Status: duckv1.Status{
					Conditions: []apis.Condition{condConsumerGroupsStopped},
				},
			},
			conditionType: ResetOffsetConditionConsumerGroupsStopped,
			wantCondition: &condConsumerGroupsStopped,
		},
		{
			name: "ResetOffsetConditionOffsetsUpdated",
			status: &ResetOffsetStatus{
				Status: duckv1.Status{
					Conditions: []apis.Condition{condOffsetsUpdated},
				},
			},
			conditionType: ResetOffsetConditionOffsetsUpdated,
			wantCondition: &condOffsetsUpdated,
		},
		{
			name: "ResetOffsetConditionConsumerGroupsStarted",
			status: &ResetOffsetStatus{
				Status: duckv1.Status{
					Conditions: []apis.Condition{condConsumerGroupsStarted},
				},
			},
			conditionType: ResetOffsetConditionConsumerGroupsStarted,
			wantCondition: &condConsumerGroupsStarted,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			gotCondition := test.status.GetCondition(test.conditionType)
			if diff := cmp.Diff(test.wantCondition, gotCondition); diff != "" {
				t.Errorf("unexpected condition (-want, +got) = %v", diff)
			}
		})
	}
}

func TestResetOffsetStatus_InitializeConditions(t *testing.T) {
	tests := []struct {
		name          string
		initialStatus *ResetOffsetStatus
		wantStatus    *ResetOffsetStatus
	}{
		{
			name:          "empty",
			initialStatus: &ResetOffsetStatus{},
			wantStatus: &ResetOffsetStatus{
				Status: duckv1.Status{
					Conditions: []apis.Condition{
						{Type: ResetOffsetConditionConsumerGroupsStarted, Status: corev1.ConditionUnknown},
						{Type: ResetOffsetConditionConsumerGroupsStopped, Status: corev1.ConditionUnknown},
						{Type: ResetOffsetConditionOffsetsUpdated, Status: corev1.ConditionUnknown},
						{Type: ResetOffsetConditionResetInitiated, Status: corev1.ConditionUnknown},
						{Type: ResetOffsetConditionSucceeded, Status: corev1.ConditionUnknown},
					},
				},
			},
		},
		{
			name: "one false",
			initialStatus: &ResetOffsetStatus{
				Status: duckv1.Status{
					Conditions: []apis.Condition{
						{Type: ResetOffsetConditionConsumerGroupsStarted, Status: corev1.ConditionFalse},
					},
				},
			},
			wantStatus: &ResetOffsetStatus{
				Status: duckv1.Status{
					Conditions: []apis.Condition{
						{Type: ResetOffsetConditionConsumerGroupsStarted, Status: corev1.ConditionFalse},
						{Type: ResetOffsetConditionConsumerGroupsStopped, Status: corev1.ConditionUnknown},
						{Type: ResetOffsetConditionOffsetsUpdated, Status: corev1.ConditionUnknown},
						{Type: ResetOffsetConditionResetInitiated, Status: corev1.ConditionUnknown},
						{Type: ResetOffsetConditionSucceeded, Status: corev1.ConditionUnknown},
					},
				},
			},
		},
		{
			name: "one true",
			initialStatus: &ResetOffsetStatus{
				Status: duckv1.Status{
					Conditions: []apis.Condition{
						{Type: ResetOffsetConditionOffsetsUpdated, Status: corev1.ConditionTrue},
					},
				},
			},
			wantStatus: &ResetOffsetStatus{
				Status: duckv1.Status{
					Conditions: []apis.Condition{
						{Type: ResetOffsetConditionConsumerGroupsStarted, Status: corev1.ConditionUnknown},
						{Type: ResetOffsetConditionConsumerGroupsStopped, Status: corev1.ConditionUnknown},
						{Type: ResetOffsetConditionOffsetsUpdated, Status: corev1.ConditionTrue},
						{Type: ResetOffsetConditionResetInitiated, Status: corev1.ConditionUnknown},
						{Type: ResetOffsetConditionSucceeded, Status: corev1.ConditionUnknown},
					},
				},
			},
		},
		{
			name: "happy true",
			initialStatus: &ResetOffsetStatus{
				Status: duckv1.Status{
					Conditions: []apis.Condition{
						{Type: ResetOffsetConditionSucceeded, Status: corev1.ConditionTrue},
					},
				},
			},
			wantStatus: &ResetOffsetStatus{
				Status: duckv1.Status{
					Conditions: []apis.Condition{
						{Type: ResetOffsetConditionConsumerGroupsStarted, Status: corev1.ConditionTrue},
						{Type: ResetOffsetConditionConsumerGroupsStopped, Status: corev1.ConditionTrue},
						{Type: ResetOffsetConditionOffsetsUpdated, Status: corev1.ConditionTrue},
						{Type: ResetOffsetConditionResetInitiated, Status: corev1.ConditionTrue},
						{Type: ResetOffsetConditionSucceeded, Status: corev1.ConditionTrue},
					},
				},
			},
		},
	}

	ignoreAllButTypeAndStatus := cmpopts.IgnoreFields(apis.Condition{}, "LastTransitionTime", "Message", "Reason", "Severity")

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.initialStatus.InitializeConditions()
			if diff := cmp.Diff(test.wantStatus, test.initialStatus, ignoreAllButTypeAndStatus); diff != "" { // Note Order Specific Comparison ; )
				t.Errorf("unexpected conditions (-want, +got) = %v", diff)
			}
		})
	}
}

func TestResetOffsetStatus_IsCompleted(t *testing.T) {
	tests := []struct {
		name                      string
		markResetInitiated        bool
		markConsumerGroupsStopped bool
		markOffsetsUpdated        bool
		markConsumerGroupsStarted bool
		wantSucceeded             bool
	}{
		{
			name:                      "Happy",
			markResetInitiated:        true,
			markConsumerGroupsStopped: true,
			markOffsetsUpdated:        true,
			markConsumerGroupsStarted: true,
			wantSucceeded:             true,
		},
		{
			name:                      "ResetInitiated Failed",
			markResetInitiated:        false,
			markConsumerGroupsStopped: true,
			markOffsetsUpdated:        true,
			markConsumerGroupsStarted: true,
			wantSucceeded:             false,
		},
		{
			name:                      "ConsumerGroupsStopped Failed",
			markResetInitiated:        true,
			markConsumerGroupsStopped: false,
			markOffsetsUpdated:        true,
			markConsumerGroupsStarted: true,
			wantSucceeded:             false,
		},
		{
			name:                      "OffsetsUpdated Failed",
			markResetInitiated:        true,
			markConsumerGroupsStopped: true,
			markOffsetsUpdated:        false,
			markConsumerGroupsStarted: true,
			wantSucceeded:             false,
		},
		{
			name:                      "ConsumerGroupsStarted Failed",
			markResetInitiated:        true,
			markConsumerGroupsStopped: true,
			markOffsetsUpdated:        true,
			markConsumerGroupsStarted: false,
			wantSucceeded:             false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resetOffsetStatus := &ResetOffsetStatus{}
			resetOffsetStatus.InitializeConditions()
			if test.markResetInitiated {
				resetOffsetStatus.MarkResetInitiatedTrue()
			} else {
				resetOffsetStatus.MarkResetInitiatedFailed("TestingOffsetsResetInitiatedStatus", "TestMessage")
			}
			if test.markConsumerGroupsStopped {
				resetOffsetStatus.MarkConsumerGroupsStoppedTrue()
			} else {
				resetOffsetStatus.MarkConsumerGroupsStoppedFailed("TestingConsumerGroupsStoppedStatus", "TestMessage")
			}
			if test.markOffsetsUpdated {
				resetOffsetStatus.MarkOffsetsUpdatedTrue()
			} else {
				resetOffsetStatus.MarkOffsetsUpdatedFailed("TestingOffsetsUpdatedFailedStatus", "TestMessage")
			}
			if test.markConsumerGroupsStarted {
				resetOffsetStatus.MarkConsumerGroupsStartedTrue()
			} else {
				resetOffsetStatus.MarkConsumerGroupsStartedFailed("TestingConsumerGroupsStartedStatus", "TestMessage")
			}
			gotSucceeded := resetOffsetStatus.IsCompleted()
			if test.wantSucceeded != gotSucceeded {
				t.Errorf("unexpected succeeded status: want %v, got %v", test.wantSucceeded, gotSucceeded)
			}
		})
	}
}

func TestRegisterAlternateResetOffsetConditionSet(t *testing.T) {
	conditionSet := apis.NewLivingConditionSet(apis.ConditionReady, "test")
	RegisterAlternateResetOffsetConditionSet(conditionSet)
	resetOffset := ResetOffset{}
	assert.Equal(t, conditionSet, resetOffset.GetConditionSet())
	assert.Equal(t, conditionSet, resetOffset.Status.GetConditionSet())
}

func TestResetOffsetStatus_Topic(t *testing.T) {
	topic := "test-topic-name"
	resetOffset := ResetOffset{}
	assert.Equal(t, "", resetOffset.Status.GetTopic())
	assert.Equal(t, resetOffset.Status.Topic, resetOffset.Status.GetTopic())
	resetOffset.Status.SetTopic(topic)
	assert.Equal(t, topic, resetOffset.Status.GetTopic())
	assert.Equal(t, resetOffset.Status.Topic, resetOffset.Status.GetTopic())
}

func TestResetOffsetStatus_Group(t *testing.T) {
	group := "test-group-id"
	resetOffset := ResetOffset{}
	assert.Equal(t, "", resetOffset.Status.GetGroup())
	assert.Equal(t, resetOffset.Status.Group, resetOffset.Status.GetGroup())
	resetOffset.Status.SetGroup(group)
	assert.Equal(t, group, resetOffset.Status.GetGroup())
	assert.Equal(t, resetOffset.Status.Group, resetOffset.Status.GetGroup())
}

func TestResetOffsetStatus_Partitions(t *testing.T) {
	partitions := []OffsetMapping{{Partition: 0, OldOffset: 1, NewOffset: 2}, {Partition: 1, OldOffset: 2, NewOffset: 3}}
	resetOffset := ResetOffset{}
	assert.Nil(t, resetOffset.Status.GetPartitions())
	assert.Equal(t, resetOffset.Status.Partitions, resetOffset.Status.GetPartitions())
	resetOffset.Status.SetPartitions(partitions)
	assert.Equal(t, partitions, resetOffset.Status.GetPartitions())
	assert.Equal(t, resetOffset.Status.Partitions, resetOffset.Status.GetPartitions())
}
