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

package kafkachannel

import (
	"context"
	"knative.dev/reconciler-test/pkg/k8s"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"knative.dev/reconciler-test/pkg/feature"
	"knative.dev/reconciler-test/pkg/manifest"
)

type CfgFn func(map[string]interface{})

func GVR() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: "messaging.knative.dev", Version: "v1beta1", Resource: "kafkachannels"}
}

// Install will create a KafkaChannel resource, using the latest version, augmented with the config fn options.
func Install(name string, opts ...CfgFn) feature.StepFn {
	cfg := map[string]interface{}{
		"name":    name,
		"version": GVR().Version,
	}
	for _, fn := range opts {
		fn(cfg)
	}
	return func(ctx context.Context, t feature.T) {
		if _, err := manifest.InstallLocalYaml(ctx, cfg); err != nil {
			t.Fatal(err, cfg)
		}
	}
}

// IsReady tests to see if a KafkaChannel becomes ready within the time given.
func IsReady(name string, timings ...time.Duration) feature.StepFn {
	return k8s.IsReady(GVR(), name, timings...)
}

// WithVersion overrides the default API version
func WithVersion(version string) CfgFn {
	return func(cfg map[string]interface{}) {
		if version != "" {
			cfg["version"] = version
		}
	}
}

// WithNumPartitions adds the numPartitions config to a KafkaChannel spec.
func WithNumPartitions(numPartitions string) CfgFn {
	return func(cfg map[string]interface{}) {
		cfg["numPartitions"] = numPartitions
	}
}

// WithReplicationFactor adds the replicationFactor config to a KafkaChannel spec.
func WithReplicationFactor(replicationFactor string) CfgFn {
	return func(cfg map[string]interface{}) {
		cfg["replicationFactor"] = replicationFactor
	}
}
