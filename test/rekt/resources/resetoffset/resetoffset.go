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

package resetoffset

import (
	"context"
	duckv1 "knative.dev/pkg/apis/duck/v1"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"knative.dev/reconciler-test/pkg/feature"
	"knative.dev/reconciler-test/pkg/manifest"
)

type CfgFn func(map[string]interface{})

func GVR() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: "kafka.eventing.knative.dev", Version: "v1alpha1", Resource: "resetoffsets"}
}

// Install will create a ResetOffset resource, using the latest version, augmented with the config fn options.
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

// WithVersion overrides the default API version
func WithVersion(version string) CfgFn {
	return func(cfg map[string]interface{}) {
		if version != "" {
			cfg["version"] = version
		}
	}
}

// WithOffsetTime adds the offsetTime config to a ResetOffset spec.
func WithOffsetTime(offsetTime string) CfgFn {
	return func(cfg map[string]interface{}) {
		offsetCfg := map[string]interface{}{}
		offsetCfg["time"] = offsetTime
		cfg["offset"] = offsetCfg
	}
}

// WithRef adds the ref config to a ResetOffset spec.
func WithRef(ref *duckv1.KReference) CfgFn {
	return func(cfg map[string]interface{}) {
		refCfg := map[string]interface{}{}
		refCfg["name"] = ref.Name
		refCfg["namespace"] = ref.Namespace
		refCfg["kind"] = ref.Kind
		refCfg["apiVersion"] = ref.APIVersion
		cfg["ref"] = refCfg
	}
}
