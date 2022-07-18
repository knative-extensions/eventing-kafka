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
	"os"

	duckv1 "knative.dev/pkg/apis/duck/v1"
	testlog "knative.dev/reconciler-test/pkg/logging"
	"knative.dev/reconciler-test/pkg/manifest"
)

func Example() {
	ctx := testlog.NewContext()
	images := map[string]string{}

	cfg := map[string]interface{}{
		"name":      "foo",
		"namespace": "bar",
		"version":   "v1alpha1",
	}
	WithOffsetTime("earliest")(cfg)
	WithRef(&duckv1.KReference{
		APIVersion: "messaging.knative.dev/v1",
		Kind:       "Subscription",
		Namespace:  "bar",
		Name:       "baz",
	})(cfg)

	files, err := manifest.ExecuteYAML(ctx, yaml, images, cfg)
	if err != nil {
		panic(err)
	}

	manifest.OutputYAML(os.Stdout, files)

	// Output:
	// apiVersion: kafka.eventing.knative.dev/v1alpha1
	// kind: ResetOffset
	// metadata:
	//   name: foo
	//   namespace: bar
	// spec:
	//   offset:
	//     time: earliest
	//   ref:
	//     apiVersion: messaging.knative.dev/v1
	//     kind: Subscription
	//     namespace: bar
	//     name: baz
}
