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
	"github.com/stretchr/testify/assert"
)

func TestResetOffsetStatus_TopicAnnotation(t *testing.T) {
	topic := "test-topic"
	tests := []struct {
		name  string
		topic string
	}{
		{
			name: "get non-existent",
		},
		{
			name:  "set and get",
			topic: topic,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ros := &ResetOffsetStatus{}
			if len(test.topic) > 0 {
				ros.SetTopicAnnotation(test.topic)
			}
			got := ros.GetTopicAnnotation()
			if diff := cmp.Diff(test.topic, got); diff != "" {
				t.Errorf("unexpected conditions (-want, +got) = %v", diff)
			}
		})
	}
}

func TestResetOffsetStatus_GroupAnnotation(t *testing.T) {
	topic := "test-topic"
	tests := []struct {
		name  string
		group string
	}{
		{
			name: "get non-existent",
		},
		{
			name:  "set and get",
			group: topic,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ros := &ResetOffsetStatus{}
			if len(test.group) > 0 {
				ros.SetGroupAnnotation(test.group)
			}
			got := ros.GetGroupAnnotation()
			if diff := cmp.Diff(test.group, got); diff != "" {
				t.Errorf("unexpected conditions (-want, +got) = %v", diff)
			}
		})
	}
}

func TestResetOffsetStatus_PartitionsAnnotation(t *testing.T) {

	tests := []struct {
		name    string
		offsets []OffsetMapping
	}{
		{
			name: "get non-existent",
		},
		{
			name: "set and get",
			offsets: []OffsetMapping{
				{Partition: 0, OldOffset: 1, NewOffset: 2},
				{Partition: 1, OldOffset: 2, NewOffset: 3},
				{Partition: 2, OldOffset: 3, NewOffset: 4},
				{Partition: 3, OldOffset: 4, NewOffset: 5},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ros := &ResetOffsetStatus{}
			if len(test.offsets) > 0 {
				err := ros.SetPartitionsAnnotation(test.offsets)
				assert.Nil(t, err)
			}
			got, err := ros.GetPartitionsAnnotation()
			assert.Nil(t, err)
			if diff := cmp.Diff(test.offsets, got); diff != "" {
				t.Errorf("unexpected conditions (-want, +got) = %v", diff)
			}
		})
	}
}
