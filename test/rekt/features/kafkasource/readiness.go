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
	"knative.dev/eventing/test/rekt/resources/svc"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	"knative.dev/reconciler-test/pkg/feature"
	"knative.dev/reconciler-test/pkg/manifest"

	"knative.dev/eventing-kafka/test/rekt/resources/kafkasource"
	"knative.dev/eventing-kafka/test/rekt/resources/kafkatopic"
)

// KafkaSourceGoesReady returns a feature testing if a KafkaSource becomes ready.
func KafkaSourceGoesReady(name string, cfg ...manifest.CfgFn) *feature.Feature {
	f := feature.NewFeatureNamed("KafkaSource goes ready.")

	topicName := feature.MakeRandomK8sName("topic") // A k8s name is also a valid topic name.
	f.Setup("install kafka topic", kafkatopic.Install(topicName))

	cfg = append(cfg, kafkasource.WithTopics([]string{topicName}))

	sink := feature.MakeRandomK8sName("sink")
	f.Setup("install a service", svc.Install(sink, "app", "rekt"))

	cfg = append(cfg, kafkasource.WithSink(&duckv1.KReference{
		Kind:       "Service",
		Name:       sink,
		APIVersion: "v1",
	}, ""))

	f.Setup("install a kafkasource", kafkasource.Install(name, cfg...))

	f.Requirement("topic is ready", kafkatopic.IsReady(topicName))
	f.Requirement("kafkasource is ready", kafkasource.IsReady(name))

	return f
}
