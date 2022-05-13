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
	"os"

	"knative.dev/reconciler-test/pkg/manifest"
)

func Example_min() {
	images := map[string]string{}
	cfg := map[string]interface{}{
		"name":      "foo",
		"namespace": "bar",
	}

	files, err := manifest.ExecuteYAML(yaml, images, cfg)
	if err != nil {
		panic(err)
	}

	manifest.OutputYAML(os.Stdout, files)
	// Output:
	// apiVersion: v1
	// kind: ConfigMap
	// metadata:
	//   name: foo
	//   namespace: bar
	// data:
	//   payload: '<no value>'
	// ---
	// apiVersion: v1
	// kind: Pod
	// metadata:
	//   name: foo
	//   namespace: bar
	// spec:
	//   containers:
	//   - image: docker.io/edenhill/kafkacat:1.6.0
	//     name: producer-container
	//     command:
	//     - kafkacat
	//     args:
	//     - "-P"
	//     - "-T"
	//     - "-b"
	//     - <no value>
	//     - "-t"
	//     - <no value>
	//     - "-l"
	//     - "/etc/mounted/payload"
	//     volumeMounts:
	//       - name: event-payload
	//         mountPath: /etc/mounted
	//   restartPolicy: Never
	//   volumes:
	//     - name: event-payload
	//       configMap:
	//         name: foo
}

func Example_full() {
	images := map[string]string{}
	cfg := map[string]interface{}{
		"name":      "foo",
		"namespace": "bar",
	}

	WithPayload("aplayload")(cfg)
	WithBootstrapServer("baz")(cfg)
	WithTopic("t1")(cfg)
	WithKey("akey")(cfg)
	WithHeaders(map[string]string{"ct": "xml", "other": "head"})(cfg)

	files, err := manifest.ExecuteYAML(yaml, images, cfg)
	if err != nil {
		panic(err)
	}

	manifest.OutputYAML(os.Stdout, files)
	// Output:
	// apiVersion: v1
	// kind: ConfigMap
	// metadata:
	//   name: foo
	//   namespace: bar
	// data:
	//   payload: 'akey=aplayload'
	// ---
	// apiVersion: v1
	// kind: Pod
	// metadata:
	//   name: foo
	//   namespace: bar
	// spec:
	//   containers:
	//   - image: docker.io/edenhill/kafkacat:1.6.0
	//     name: producer-container
	//     command:
	//     - kafkacat
	//     args:
	//     - "-P"
	//     - "-T"
	//     - "-b"
	//     - baz
	//     - "-t"
	//     - t1
	//     - "-K="
	//     - "-H"
	//     - "ct=xml"
	//     - "-H"
	//     - "other=head"
	//     - "-l"
	//     - "/etc/mounted/payload"
	//     volumeMounts:
	//       - name: event-payload
	//         mountPath: /etc/mounted
	//   restartPolicy: Never
	//   volumes:
	//     - name: event-payload
	//       configMap:
	//         name: foo
}
