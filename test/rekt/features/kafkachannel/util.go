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
	subscriptionresources "knative.dev/eventing/test/rekt/resources/subscription"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	"knative.dev/reconciler-test/pkg/feature"

	kafkachannelresources "knative.dev/eventing-kafka/test/rekt/resources/kafkachannel"
	resetoffsetresources "knative.dev/eventing-kafka/test/rekt/resources/resetoffset"
)

//
// Setup() Utilities
//

// setupKafkaChannel adds a Setup() to the specified Feature to create a KafkaChannel.
func setupKafkaChannel(f *feature.Feature, name string) {
	kafkaChannelCfgFns := []kafkachannelresources.CfgFn{
		kafkachannelresources.WithNumPartitions("3"),
		kafkachannelresources.WithReplicationFactor("1"),
	}
	f.Setup("Install a KafkaChannel", kafkachannelresources.Install(name, kafkaChannelCfgFns...))
}

// setupSubscription adds a Setup() to the specified Feature to create a Subscription to a KafkaChannel.
func setupSubscription(f *feature.Feature, name string, subscriberURI string) {
	f.Setup("Install a Subscription", subscriptionresources.Install(name,
		subscriptionresources.WithChannel(&duckv1.KReference{
			Kind:       "KafkaChannel",
			Name:       name,
			APIVersion: kafkachannelresources.GVR().GroupVersion().String(),
		}),
		subscriptionresources.WithSubscriber(nil, subscriberURI),
	))
}

// setupResetOffset adds a Setup() to the specified Feature to create a ResetOffset for a KafkaChannel.
func setupResetOffset(f *feature.Feature, name string, offsetTime string) {

	ref := &duckv1.KReference{
		Kind:       "Subscription",
		Namespace:  "",
		Name:       name,
		APIVersion: "",
		Group:      "",
	}

	resetOffsetCfgFns := []resetoffsetresources.CfgFn{
		resetoffsetresources.WithOffsetTime(offsetTime),
		resetoffsetresources.WithRef(ref),
	}
	f.Setup("Install a ResetOffset", resetoffsetresources.Install(name, resetOffsetCfgFns...))
}

//
// Assertion Utilities
//

// assertKafkaChannelReady adds an Assert() to the specified Feature to verify the KafkaChannel is READY.
func assertKafkaChannelReady(f *feature.Feature, name string) {
	f.Assert("KafkaChannel is ready", kafkachannelresources.IsReady(name))
}

// assertSubscriptionReady adds an Assert() to the specified Feature to verify the Subscription is READY.
func assertSubscriptionReady(f *feature.Feature, name string) {
	f.Assert("Subscription is ready", subscriptionresources.IsReady(name))
}
