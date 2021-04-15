package config

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestConfigmapDataCheckSum(t *testing.T) {
	cases := []struct {
		name          string
		configmapData map[string]string
		expected      string
	}{{
		name:          "nil configmap data",
		configmapData: nil,
		expected:      "",
	}, {
		name:          "empty configmap data",
		configmapData: map[string]string{},
		expected:      "3c7e1501", // precomputed manually
	}, {
		name:          "with configmap data",
		configmapData: map[string]string{"foo": "bar"},
		expected:      "f39c9878", // precomputed manually
	}}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			retrieved := ConfigmapDataCheckSum(tc.configmapData)
			if diff := cmp.Diff(tc.expected, retrieved); diff != "" {
				t.Errorf("unexpected Config (-want, +got) = %v", diff)
			}
		})
	}
}
