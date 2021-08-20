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

package main

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
	"go.uber.org/multierr"
	"k8s.io/apimachinery/pkg/types"

	commonconstants "knative.dev/eventing-kafka/pkg/common/constants"
)

func TestFoo(t *testing.T) {

	var multiErr error
	if true {
		multierr.AppendInto(&multiErr, fmt.Errorf("TEST ERROR"))
		assert.NotNil(t, multiErr)
	} else {
		assert.Nil(t, multiErr)
	}
}

func TestGetKafkaChannelNamespacedName(t *testing.T) {

	tests := []struct {
		name     string
		topic    string
		expected *types.NamespacedName
		err      bool
	}{
		{
			name:     "Empty",
			topic:    "",
			expected: nil,
		},
		{
			name:     "Internal",
			topic:    "__consumer_offsets",
			expected: nil,
		},
		{
			name:     "Too Short",
			topic:    "ab",
			expected: nil,
		},
		{
			name:     "No Separator",
			topic:    "Foo",
			expected: nil,
		},
		{
			name:     "Extra Component",
			topic:    "Foo.Bar.Baz",
			expected: nil,
			err:      true,
		},
		{
			name:     "Valid Consolidated",
			topic:    "knative-messaging-kafka.kcnamespace.kcname",
			expected: &types.NamespacedName{Namespace: "kcnamespace", Name: "kcname"},
		},
		{
			name:     "Valid Distributed",
			topic:    "kcnamespace.kcname",
			expected: &types.NamespacedName{Namespace: "kcnamespace", Name: "kcname"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := getKafkaChannelNamespacedName(test.topic)
			assert.Equal(t, test.expected, result)
			assert.Equal(t, test.err, err != nil)
		})
	}
}

func TestGetExpectedRetentionDurationString(t *testing.T) {

	tests := []struct {
		name        string
		retentionMs string
		expect      string
		err         bool
	}{
		{
			name:        "Empty",
			retentionMs: "",
			expect:      "",
			err:         true,
		},
		{
			name:        "Non-Numeric",
			retentionMs: "FOO",
			expect:      "",
			err:         true,
		},
		{
			name:        "1 Second",
			retentionMs: "1000",
			expect:      "PT1S",
		},
		{
			name:        "1.5 Second",
			retentionMs: "1500",
			expect:      "PT1.5S",
		},
		{
			name:        "12 Hours",
			retentionMs: strconv.FormatInt((12 * time.Hour).Milliseconds(), 10),
			expect:      "PT12H",
		},
		{
			name:        "12.5 Hours",
			retentionMs: strconv.FormatInt(((12 * time.Hour) + (30 * time.Minute)).Milliseconds(), 10),
			expect:      "PT12H30M",
		},
		{
			name:        "Full Detail",
			retentionMs: strconv.FormatInt(((12 * time.Hour) + (30 * time.Minute) + (15 * time.Second) + (5 * time.Millisecond)).Milliseconds(), 10),
			expect:      "PT12H30M15S", // Millis Dropped
		},
		{
			name:        "7 Days",
			retentionMs: strconv.FormatInt((7 * 24 * time.Hour).Milliseconds(), 10),
			expect:      "PT168H", // 7 Days
		},
		{
			name:        "30 Days",
			retentionMs: strconv.FormatInt((30 * 24 * time.Hour).Milliseconds(), 10),
			expect:      "PT720H", // 30 Days
		},
		{
			name:        "60 Days",
			retentionMs: strconv.FormatInt((60 * 24 * time.Hour).Milliseconds(), 10),
			expect:      "PT1440H", // 60 Days
		},
		{
			name:        "180 Days",
			retentionMs: strconv.FormatInt((180 * 24 * time.Hour).Milliseconds(), 10),
			expect:      "P180D", // Precision Switch-Over
		},
		{
			name:        "Long Full Detail",
			retentionMs: strconv.FormatInt(((180 * 24 * time.Hour) + (30 * time.Minute) + (15 * time.Second) + (5 * time.Millisecond)).Milliseconds(), 10),
			expect:      "P180DT30M15S", // Precision Switch-Over & Millis Dropped
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {

			topicDetail := sarama.TopicDetail{
				ConfigEntries: map[string]*string{commonconstants.KafkaTopicConfigRetentionMs: &test.retentionMs},
			}

			result, err := getExpectedRetentionDurationString(topicDetail)

			assert.Equal(t, test.expect, result)
			assert.Equal(t, test.err, err != nil)
		})
	}
}
