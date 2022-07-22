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

package kafkatopic

import (
	"os"

	testlog "knative.dev/reconciler-test/pkg/logging"
	"knative.dev/reconciler-test/pkg/manifest"
)

func Example_min() {
	ctx := testlog.NewContext()
	images := map[string]string{}
	cfg := map[string]interface{}{
		"name":             "foo",
		"partitions":       10,
		"clusterName":      "my-cluster",
		"clusterNamespace": "there",
	}

	files, err := manifest.ExecuteYAML(ctx, yaml, images, cfg)
	if err != nil {
		panic(err)
	}

	manifest.OutputYAML(os.Stdout, files)
	// Output:
	// apiVersion: kafka.strimzi.io/v1beta2
	// kind: KafkaTopic
	// metadata:
	//   name: foo
	//   namespace: there
	//   labels:
	//     strimzi.io/cluster: my-cluster
	// spec:
	//   partitions: 10
	//   replicas: 1
}

func Example_full() {
	ctx := testlog.NewContext()
	images := map[string]string{}
	cfg := map[string]interface{}{
		"name": "foo",
	}

	WithClusterName("other-cluster")(cfg)
	WithClusterNamespace("here")(cfg)
	WithPartitions("100")(cfg)

	files, err := manifest.ExecuteYAML(ctx, yaml, images, cfg)
	if err != nil {
		panic(err)
	}

	manifest.OutputYAML(os.Stdout, files)
	// Output:
	// apiVersion: kafka.strimzi.io/v1beta2
	// kind: KafkaTopic
	// metadata:
	//   name: foo
	//   namespace: here
	//   labels:
	//     strimzi.io/cluster: other-cluster
	// spec:
	//   partitions: 100
	//   replicas: 1
}
