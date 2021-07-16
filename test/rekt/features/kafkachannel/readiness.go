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
	"knative.dev/eventing/test/rekt/resources/subscription"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	"knative.dev/reconciler-test/pkg/feature"

	kafkachannelresources "knative.dev/eventing-kafka/test/rekt/resources/kafkachannel"
)

// UnsubscribedKafkaChannelReadiness returns a feature testing if an unsubscribed KafkaChannel becomes ready.
func UnsubscribedKafkaChannelReadiness(name string, cfg ...kafkachannelresources.CfgFn) *feature.Feature {
	f := feature.NewFeatureNamed("Unsubscribed KafkaChannel goes ready")
	f.Setup("Install a KafkaChannel", kafkachannelresources.Install(name, cfg...))
	f.Assert("KafkaChannel is ready", kafkachannelresources.IsReady(name))
	return f
}

// SubscribedKafkaChannelReadiness returns a feature testing if a subscribed KafkaChannel becomes ready.
func SubscribedKafkaChannelReadiness(name string, cfg ...kafkachannelresources.CfgFn) *feature.Feature {
	f := feature.NewFeatureNamed("Subscribed KafkaChannel goes ready")
	f.Setup("Install a KafkaChannel", kafkachannelresources.Install(name, cfg...))
	f.Setup("Install a Subscription", subscription.Install(name,
		subscription.WithChannel(&duckv1.KReference{
			Kind:       "KafkaChannel",
			Name:       name,
			APIVersion: kafkachannelresources.GVR().GroupVersion().String(),
		}),
		subscription.WithSubscriber(nil, "http://some-service.some-namespace/some-path"),
	))
	f.Assert("KafkaChannel is ready", kafkachannelresources.IsReady(name))
	return f
}
