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

package kafkasource

import (
	"os"

	duckv1 "knative.dev/pkg/apis/duck/v1"
	"knative.dev/reconciler-test/pkg/manifest"
)

// The following examples validate the processing of the With* helper methods
// applied to config and go template parser.

func Example_min() {
	images := map[string]string{}
	cfg := map[string]interface{}{
		"name":      "foo",
		"namespace": "bar",
		"version":   "v1beta1",
	}

	files, err := manifest.ExecuteYAML(yaml, images, cfg)
	if err != nil {
		panic(err)
	}

	manifest.OutputYAML(os.Stdout, files)
	// Output:
	// apiVersion: sources.knative.dev/v1beta1
	// kind: KafkaSource
	// metadata:
	//   name: foo
	//   namespace: bar
	// spec:
}

func Example_full() {
	images := map[string]string{}
	cfg := map[string]interface{}{
		"name":      "foo",
		"namespace": "bar",
	}

	WithVersion("v1")(cfg)
	WithAnnotations(map[string]string{
		"autoscaling.knative.dev/class":    "keda.autoscaling.knative.dev",
		"autoscaling.knative.dev/minScale": "0",
	})(cfg)
	WithBootstrapServers([]string{"baz"})(cfg)
	WithTopics([]string{"t1", "t2"})(cfg)
	WithTLSCert("tlscertname", "tlscertkey")(cfg)
	WithTLSKey("tlskeyname", "tlskeykey")(cfg)
	WithTLSCACert("tlscaCertname", "tlscaCertkey")(cfg)
	WithSASLUser("saslusername", "sasluserkey")(cfg)
	WithSASLPassword("saslpwdname", "saslpwdkey")(cfg)
	WithSASLType("sasltypename", "sasltypekey")(cfg)
	WithExtensions(map[string]string{
		"myextension":      "myvalue",
		"myotherextension": "myothervalue",
	})(cfg)
	WithSink(&duckv1.KReference{Kind: "Service", Name: "name", APIVersion: "v1"}, "")(cfg)

	files, err := manifest.ExecuteLocalYAML(images, cfg)
	if err != nil {
		panic(err)
	}

	manifest.OutputYAML(os.Stdout, files)
	// Output:
	// apiVersion: sources.knative.dev/v1
	// kind: KafkaSource
	// metadata:
	//   name: foo
	//   namespace: bar
	//   annotations:
	//     autoscaling.knative.dev/class: "keda.autoscaling.knative.dev"
	//     autoscaling.knative.dev/minScale: "0"
	// spec:
	//   bootstrapServers:
	//     - "baz"
	//   topics:
	//     - "t1"
	//     - "t2"
	//   net:
	//     tls:
	//       enable: true
	//       cert:
	//         secretKeyRef:
	//           name: "tlscertname"
	//           key: "tlscertkey"
	//       key:
	//         secretKeyRef:
	//           name: "tlskeyname"
	//           key: "tlskeykey"
	//       caCert:
	//         secretKeyRef:
	//           name: "tlscaCertname"
	//           key: "tlscaCertkey"
	//     sasl:
	//       enable: true
	//       user:
	//         secretKeyRef:
	//           name: "saslusername"
	//           key: "sasluserkey"
	//       password:
	//         secretKeyRef:
	//           name: "saslpwdname"
	//           key: "saslpwdkey"
	//       type:
	//         secretKeyRef:
	//           name: "sasltypename"
	//           key: "sasltypekey"
	//   ceOverrides:
	//     extensions:
	//       myextension: "myvalue"
	//       myotherextension: "myothervalue"
	//   sink:
	//     ref:
	//       kind: Service
	//       namespace: bar
	//       name: name
	//       apiVersion: v1
}
