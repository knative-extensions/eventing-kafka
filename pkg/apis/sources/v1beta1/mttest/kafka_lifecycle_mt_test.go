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

package mttest

import (
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	corev1 "k8s.io/api/core/v1"
	"knative.dev/eventing-kafka/pkg/apis/sources/v1beta1"
	"knative.dev/pkg/apis"
)

func TestMTKafkaSourceStatusGetCondition(t *testing.T) {
	v1beta1.RegisterAlternateKafkaConditionSet(v1beta1.KafkaMTSourceCondSet)
	tests := []struct {
		name      string
		s         *v1beta1.KafkaSourceStatus
		condQuery apis.ConditionType
		want      *apis.Condition
	}{{
		name:      "uninitialized",
		s:         &v1beta1.KafkaSourceStatus{},
		condQuery: v1beta1.KafkaConditionReady,
		want:      nil,
	}, {
		name: "initialized",
		s: func() *v1beta1.KafkaSourceStatus {
			s := &v1beta1.KafkaSourceStatus{}
			s.InitializeConditions()
			return s
		}(),
		condQuery: v1beta1.KafkaConditionReady,
		want: &apis.Condition{
			Type:   v1beta1.KafkaConditionReady,
			Status: corev1.ConditionUnknown,
		},
	}, {
		name: "mark scheduled",
		s: func() *v1beta1.KafkaSourceStatus {
			s := &v1beta1.KafkaSourceStatus{}
			s.InitializeConditions()
			s.MarkScheduled()
			return s
		}(),
		condQuery: v1beta1.KafkaConditionReady,
		want: &apis.Condition{
			Type:   v1beta1.KafkaConditionReady,
			Status: corev1.ConditionUnknown,
		},
	}, {
		name: "mark sink",
		s: func() *v1beta1.KafkaSourceStatus {
			s := &v1beta1.KafkaSourceStatus{}
			s.InitializeConditions()
			s.MarkSink(apis.HTTP("example"))
			return s
		}(),
		condQuery: v1beta1.KafkaConditionReady,
		want: &apis.Condition{
			Type:   v1beta1.KafkaConditionReady,
			Status: corev1.ConditionUnknown,
		},
	}, {
		name: "mark sink and scheduled",
		s: func() *v1beta1.KafkaSourceStatus {
			s := &v1beta1.KafkaSourceStatus{}
			s.InitializeConditions()
			s.MarkSink(apis.HTTP("uri://example"))
			s.MarkScheduled()
			s.MarkConnectionEstablished()
			s.MarkInitialOffsetCommitted()
			return s
		}(),
		condQuery: v1beta1.KafkaConditionReady,
		want: &apis.Condition{
			Type:   v1beta1.KafkaConditionReady,
			Status: corev1.ConditionTrue,
		},
	}, {
		name: "mark sink and scheduled then no sink",
		s: func() *v1beta1.KafkaSourceStatus {
			s := &v1beta1.KafkaSourceStatus{}
			s.InitializeConditions()
			s.MarkSink(apis.HTTP("example"))
			s.MarkScheduled()
			s.MarkNoSink("Testing", "hi%s", "")
			return s
		}(),
		condQuery: v1beta1.KafkaConditionReady,
		want: &apis.Condition{
			Type:    v1beta1.KafkaConditionReady,
			Status:  corev1.ConditionFalse,
			Reason:  "Testing",
			Message: "hi",
		},
	}, {
		name: "mark sink nil and scheduled",
		s: func() *v1beta1.KafkaSourceStatus {
			s := &v1beta1.KafkaSourceStatus{}
			s.InitializeConditions()
			s.MarkSink(nil)
			s.MarkScheduled()
			return s
		}(),
		condQuery: v1beta1.KafkaConditionReady,
		want: &apis.Condition{
			Type:    v1beta1.KafkaConditionReady,
			Status:  corev1.ConditionUnknown,
			Reason:  "SinkEmpty",
			Message: "Sink has resolved to empty.",
		},
	}, {
		name: "mark sink empty and scheduled then sink",
		s: func() *v1beta1.KafkaSourceStatus {
			s := &v1beta1.KafkaSourceStatus{}
			s.InitializeConditions()
			s.MarkSink(nil)
			s.MarkScheduled()
			s.MarkInitialOffsetCommitted()
			s.MarkConnectionEstablished()
			s.MarkSink(apis.HTTP("example"))
			return s
		}(),
		condQuery: v1beta1.KafkaConditionReady,
		want: &apis.Condition{
			Type:   v1beta1.KafkaConditionReady,
			Status: corev1.ConditionTrue,
		},
	}}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got := test.s.GetCondition(test.condQuery)
			fmt.Println(test.s)
			ignoreTime := cmpopts.IgnoreFields(apis.Condition{},
				"LastTransitionTime", "Severity")
			if diff := cmp.Diff(test.want, got, ignoreTime); diff != "" {

				t.Errorf("unexpected condition (-want, +got) = %v", diff)
			}
		})
	}
}
