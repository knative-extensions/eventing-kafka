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
	"knative.dev/reconciler-test/pkg/feature"

	"knative.dev/eventing-kafka/test/rekt/resources/kafkachannel"
)

// KafkaChannelGoesReady returns a feature testing if a KafkaChannel becomes ready.
func KafkaChannelGoesReady(name string, cfg ...kafkachannel.CfgFn) *feature.Feature {
	f := feature.NewFeatureNamed("KafkaChannel goes ready")
	f.Setup("Install a KafkaChannel", kafkachannel.Install(name, cfg...))
	f.Requirement("KafkaChannel is ready", kafkachannel.IsReady(name))
	return f
}
