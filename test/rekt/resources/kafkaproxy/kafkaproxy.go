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

package kafkaproxy

import (
	"context"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"

	"knative.dev/eventing/test/rekt/resources/addressable"
	"knative.dev/pkg/apis"
	"knative.dev/reconciler-test/pkg/environment"
	"knative.dev/reconciler-test/pkg/feature"
	"knative.dev/reconciler-test/pkg/k8s"
	"knative.dev/reconciler-test/pkg/manifest"
)

type CfgFn func(map[string]interface{})

func init() {
	environment.RegisterPackage(manifest.ImagesLocalYaml()...)
}

// Install starts a new kafkaproxy with the provided name
func Install(name string, opts ...CfgFn) feature.StepFn {
	cfg := map[string]interface{}{
		"name": name,
	}

	for _, fn := range opts {
		fn(cfg)
	}

	return func(ctx context.Context, t feature.T) {
		if _, err := manifest.InstallLocalYaml(ctx, cfg); err != nil {
			t.Fatal(err, cfg)
		}

		k8s.WaitForPodRunningOrFail(ctx, t, name)
	}
}

// WithBootstrapServer adds bootstrapServer to the kafkacat argument list.
func WithBootstrapServer(bootstrapServer string) CfgFn {
	return func(cfg map[string]interface{}) {
		cfg["bootstrapServer"] = bootstrapServer
	}
}

// WithTopic adds the topic to the kafkacat argument list.
func WithTopic(topic string) CfgFn {
	return func(cfg map[string]interface{}) {
		cfg["topic"] = topic
	}
}

// Address returns a proxy's address.
func Address(ctx context.Context, name string, timings ...time.Duration) (*apis.URL, error) {
	gvr := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "services"}
	return addressable.Address(ctx, gvr, name, timings...)
}
