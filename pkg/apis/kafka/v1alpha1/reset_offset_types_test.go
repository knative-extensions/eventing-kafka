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
	"time"

	"github.com/Shopify/sarama"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	duckv1 "knative.dev/pkg/apis/duck/v1"
)

func TestResetOffset_GetGroupVersionKind(t *testing.T) {
	resetOffset := ResetOffset{}
	gvk := resetOffset.GetGroupVersionKind()
	if gvk.Kind != "ResetOffset" {
		t.Errorf("Should be 'ResetOffset'.")
	}
}

func TestResetOffset_GetStatus(t *testing.T) {
	status := &duckv1.Status{}
	resetOffset := ResetOffset{
		Status: ResetOffsetStatus{
			Status: *status,
		},
	}
	if !cmp.Equal(resetOffset.GetStatus(), status) {
		t.Errorf("GetStatus did not retrieve status. Got=%v Want=%v", resetOffset.GetStatus(), status)
	}
}

func TestResetOffsetSpec_IsOffsetEarliest(t *testing.T) {

	tests := []struct {
		name   string
		offset string
		want   bool
	}{
		{
			name:   "earliest",
			offset: OffsetEarliest,
			want:   true,
		},
		{
			name:   "latest",
			offset: OffsetLatest,
			want:   false,
		},
		{
			name:   "time",
			offset: "2021-05-04T05:04:01Z",
			want:   false,
		},
		{
			name:   "invalid",
			offset: "foo",
			want:   false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resetOffsetSpec := &ResetOffsetSpec{Offset: OffsetSpec{Time: test.offset}}
			got := resetOffsetSpec.IsOffsetEarliest()
			if diff := cmp.Diff(test.want, got); diff != "" {
				t.Errorf("unexpected conditions (-want, +got) = %v", diff)
			}
		})
	}
}

func TestResetOffsetSpec_IsOffsetLatest(t *testing.T) {

	tests := []struct {
		name   string
		offset string
		want   bool
	}{
		{
			name:   "earliest",
			offset: OffsetEarliest,
			want:   false,
		},
		{
			name:   "latest",
			offset: OffsetLatest,
			want:   true,
		},
		{
			name:   "time",
			offset: "2021-05-04T05:04:01Z",
			want:   false,
		},
		{
			name:   "invalid",
			offset: "foo",
			want:   false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resetOffsetSpec := &ResetOffsetSpec{Offset: OffsetSpec{Time: test.offset}}
			got := resetOffsetSpec.IsOffsetLatest()
			if diff := cmp.Diff(test.want, got); diff != "" {
				t.Errorf("unexpected conditions (-want, +got) = %v", diff)
			}
		})
	}
}

func TestResetOffsetSpec_ParseOffsetTime(t *testing.T) {

	offsetRFC3339 := time.Now().UTC().Add(-1 * time.Hour).Format(time.RFC3339)
	timeRFC3339, _ := time.Parse(time.RFC3339, offsetRFC3339)

	tests := []struct {
		name       string
		offset     string
		expectTime time.Time
		expectErr  bool
	}{
		{
			name:       "valid",
			offset:     offsetRFC3339,
			expectTime: timeRFC3339,
			expectErr:  false,
		},
		{
			name:       "invalid",
			offset:     "foo",
			expectTime: time.Time{},
			expectErr:  true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resetOffsetSpec := &ResetOffsetSpec{Offset: OffsetSpec{Time: test.offset}}
			offsetTime, err := resetOffsetSpec.ParseOffsetTime()
			if test.expectErr {
				assert.NotNil(t, err)
				assert.Equal(t, time.Time{}, offsetTime)
			} else {
				assert.Nil(t, err)
				assert.Equal(t, test.expectTime, offsetTime)
			}
		})
	}
}

func TestResetOffsetSpec_ParseSaramaOffsetTime(t *testing.T) {

	offsetRFC3339 := time.Now().UTC().Add(-1 * time.Hour).Format(time.RFC3339)
	timeRFC3339, _ := time.Parse(time.RFC3339, offsetRFC3339)

	tests := []struct {
		name       string
		offset     string
		expectTime int64
		expectErr  bool
	}{
		{
			name:       "valid earliest",
			offset:     OffsetEarliest,
			expectTime: sarama.OffsetOldest,
			expectErr:  false,
		},
		{
			name:       "valid latest",
			offset:     OffsetLatest,
			expectTime: sarama.OffsetNewest,
			expectErr:  false,
		},
		{
			name:       "valid time",
			offset:     offsetRFC3339,
			expectTime: timeRFC3339.UnixNano() / 1000000,
			expectErr:  false,
		},
		{
			name:       "invalid time",
			offset:     "foo",
			expectTime: 0,
			expectErr:  true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resetOffsetSpec := &ResetOffsetSpec{Offset: OffsetSpec{Time: test.offset}}
			saramaOffsetTime, err := resetOffsetSpec.ParseSaramaOffsetTime()
			assert.Equal(t, test.expectErr, err != nil)
			assert.Equal(t, test.expectTime, saramaOffsetTime)
		})
	}
}
