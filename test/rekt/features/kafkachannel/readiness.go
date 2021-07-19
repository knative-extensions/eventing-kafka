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
)

const kafkaChannelNamePrefix = "kc-rekt-readiness-test-"

// UnsubscribedKafkaChannelReadiness returns a Feature testing if an unsubscribed KafkaChannel becomes ready.
func UnsubscribedKafkaChannelReadiness() *feature.Feature {
	kafkaChannelName := feature.MakeRandomK8sName(kafkaChannelNamePrefix)
	f := feature.NewFeatureNamed("Unsubscribed KafkaChannel goes ready")
	setupKafkaChannel(f, kafkaChannelName)
	assertKafkaChannelReady(f, kafkaChannelName)
	return f
}

// SubscribedKafkaChannelReadiness returns a Feature testing if a subscribed KafkaChannel becomes ready.
func SubscribedKafkaChannelReadiness() *feature.Feature {
	kafkaChannelName := feature.MakeRandomK8sName(kafkaChannelNamePrefix)
	subscriberURI := "http://some-service.some-namespace/some-path"
	f := feature.NewFeatureNamed("Subscribed KafkaChannel goes ready")
	setupKafkaChannel(f, kafkaChannelName)
	setupSubscription(f, kafkaChannelName, subscriberURI)
	assertKafkaChannelReady(f, kafkaChannelName)
	assertSubscriptionReady(f, kafkaChannelName)
	return f
}
