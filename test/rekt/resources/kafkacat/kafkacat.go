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

package kafkacat

import (
	"context"

	"knative.dev/reconciler-test/pkg/feature"
	"knative.dev/reconciler-test/pkg/k8s"
	"knative.dev/reconciler-test/pkg/manifest"
)

type CfgFn func(map[string]interface{})

// Install will create a pod running kafkacat, augmented with the config fn options.
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

// WithPayload adds payload to the kafkacat configmap.
func WithPayload(payload string) CfgFn {
	return func(cfg map[string]interface{}) {
		cfg["payload"] = payload
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

// WithKey adds the key to the kafkacat argument list.
func WithKey(key string) CfgFn {
	return func(cfg map[string]interface{}) {
		if key != "" {
			cfg["key"] = key
		}
	}
}

// WithHeaders adds the headers to the kafkacat argument list.
func WithHeaders(headers map[string]string) CfgFn {
	return func(cfg map[string]interface{}) {
		if len(headers) > 0 {
			cfg["headers"] = headers
		}
	}
}
