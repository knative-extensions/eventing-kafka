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
	"knative.dev/reconciler-test/pkg/feature"

	"knative.dev/eventing-kafka/test/rekt/resources/kafkatopic"
)

// KafkaTopicGoesReady returns a feature testing if a Kafka topic becomes ready.
func KafkaTopicGoesReady(name string, cfg ...kafkatopic.CfgFn) *feature.Feature {
	f := feature.NewFeatureNamed("KafkaTopic goes ready.")

	f.Setup("install a Kafka topic", kafkatopic.Install(name, cfg...))

	// f.Stable("kafkasource").
	// 	Must("be ready", kafkasource.IsReady(name))

	return f
}
