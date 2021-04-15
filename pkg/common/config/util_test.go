package config

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
)

func TestConfigmapDataCheckSum(t *testing.T) {
	cases := []struct {
		name      string
		configmap *corev1.ConfigMap
		expected  string
	}{{
		name:      "nil configmap",
		configmap: nil,
		expected:  "",
	}, {
		name: "nil configmap data",
		configmap: &corev1.ConfigMap{
			Data: nil,
		},
		expected: "",
	}, {
		name: "with configmap data",
		configmap: &corev1.ConfigMap{
			Data: map[string]string{"foo": "bar"},
		},
		expected: "f39c9878", // precomputed manually
	}}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			retrieved := ConfigmapDataCheckSum(tc.configmap)
			if diff := cmp.Diff(tc.expected, retrieved); diff != "" {
				t.Errorf("unexpected Config (-want, +got) = %v", diff)
			}
		})
	}
}
