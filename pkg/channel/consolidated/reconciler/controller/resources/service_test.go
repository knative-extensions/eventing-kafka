/*
Copyright 2022 The Knative Authors

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

package resources

import (
	"fmt"
	"strings"
	"testing"
)

func TestMakeChannelServiceName(t *testing.T) {
	tt := []struct {
		name  string
		given string
		want  string
	}{
		{
			name:  "63 chars",
			given: strings.Repeat("a", 63),
			want:  strings.Repeat("a", 63),
		},
		{
			name:  "64 chars",
			given: strings.Repeat("a", 64),
			want:  strings.Repeat("a", 64),
		},
		{
			name:  fmt.Sprintf("%d chars", 65-len(ChannelSuffix)),
			given: strings.Repeat("a", 65-len(ChannelSuffix)),
			want:  strings.Repeat("a", 65-len(ChannelSuffix)),
		},
		{
			name:  fmt.Sprintf("%d chars", 64-len(ChannelSuffix)),
			given: strings.Repeat("a", 64-len(ChannelSuffix)),
			want:  strings.Repeat("a", 64-len(ChannelSuffix)),
		},
		{
			name:  fmt.Sprintf("%d chars", 63-len(ChannelSuffix)),
			given: strings.Repeat("a", 63-len(ChannelSuffix)),
			want:  strings.Repeat("a", 63-len(ChannelSuffix)) + "-kn-channel",
		},
		{
			name:  fmt.Sprintf("%d chars", 62-len(ChannelSuffix)),
			given: strings.Repeat("a", 62-len(ChannelSuffix)),
			want:  strings.Repeat("a", 62-len(ChannelSuffix)) + "-kn-channel",
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			if got := MakeChannelServiceName(tc.given); got != tc.want {
				t.Errorf("want %s, got %s", tc.want, got)
			}
		})
	}
}
