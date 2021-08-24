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

package v1beta1

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	eventingduck "knative.dev/eventing/pkg/apis/duck/v1"
	duckv1 "knative.dev/pkg/apis/duck/v1"
)

func TestKafkaChannelGetGroupVersionKind(t *testing.T) {
	ch := KafkaChannel{}
	gvk := ch.GetGroupVersionKind()

	if gvk.Kind != "KafkaChannel" {
		t.Errorf("Should be 'KafkaChannel'.")
	}
}

func TestKafkaChannelGetStatus(t *testing.T) {
	status := &duckv1.Status{}
	config := KafkaChannel{
		Status: KafkaChannelStatus{
			ChannelableStatus: eventingduck.ChannelableStatus{
				Status: *status,
			},
		},
	}

	if !cmp.Equal(config.GetStatus(), status) {
		t.Errorf("GetStatus did not retrieve status. Got=%v Want=%v", config.GetStatus(), status)
	}
}

func TestKafkaChannelSpecParseRetentionDuration(t *testing.T) {

	tests := []struct {
		name             string
		includeRetention bool
		retentionString  string
		expectDuration   time.Duration
		expectErr        bool
	}{
		{
			name:             "valid",
			includeRetention: true,
			retentionString:  "P14D",
			expectDuration:   14 * 24 * time.Hour,
			expectErr:        false,
		},
		{
			name:             "precision",
			includeRetention: true,
			retentionString:  "PT720H", // 30 Days Precise
			expectDuration:   30 * 24 * time.Hour,
			expectErr:        false,
		}, {
			name:             "empty",
			includeRetention: true,
			retentionString:  "",
			expectDuration:   time.Duration(-1),
			expectErr:        true,
		},
		{
			name:             "invalid",
			includeRetention: true,
			retentionString:  "abc123",
			expectDuration:   time.Duration(-1),
			expectErr:        true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			kcSpec := &KafkaChannelSpec{}
			if test.includeRetention {
				kcSpec.RetentionDuration = test.retentionString
			}
			retentionDuration, err := kcSpec.ParseRetentionDuration()
			assert.Equal(t, test.expectErr, err != nil)
			assert.Equal(t, test.expectDuration, retentionDuration)
		})
	}
}
