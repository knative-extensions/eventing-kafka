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
	"context"
	"time"

	cetest "github.com/cloudevents/sdk-go/v2/test"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	"knative.dev/reconciler-test/pkg/eventshub"
	assert "knative.dev/reconciler-test/pkg/eventshub/assert"
	"knative.dev/reconciler-test/pkg/feature"

	"knative.dev/eventing-kafka/test/rekt/resources/kafkacat"
	"knative.dev/eventing-kafka/test/rekt/resources/kafkasource"
	"knative.dev/eventing-kafka/test/rekt/resources/kafkatopic"
)

// DataPlaneDelivery returns a feature testing if the event sent to Kafka is received by the sink
// via KafkaSource installed before sending events to Kafka
func DataPlaneDelivery(name string, ksopts []kafkasource.CfgFn, kcopts []kafkacat.CfgFn, matchers cetest.EventMatcher) *feature.Feature {
	f := feature.NewFeatureNamed("Delivery")

	// Setup sink
	sinkName := feature.MakeRandomK8sName("sink")
	f.Setup("install sink", eventshub.Install(sinkName, eventshub.StartReceiver))

	ksopts = append(ksopts, kafkasource.WithSink(&duckv1.KReference{
		Kind:       "Service",
		Name:       sinkName,
		APIVersion: "v1",
	}, ""))

	// Setup topic
	topicName := feature.MakeRandomK8sName("topic")
	f.Setup("install Kafka topic", kafkatopic.Install(topicName))

	ksopts = append(ksopts, kafkasource.WithTopics([]string{topicName}))
	kcopts = append(kcopts, kafkacat.WithTopic(topicName))

	// Setup source
	f.Setup("install a kafkasource", kafkasource.Install(name, ksopts...))

	f.Requirement("Kafka topic must be ready", kafkatopic.IsReady(topicName))
	f.Requirement("Kafka source must be ready", kafkasource.IsReady(name))

	f.Assert("forward events from topic to sink", sinkReceiveProducedEvent(name, kcopts, sinkName, matchers))

	return f
}

func sinkReceiveProducedEvent(name string, kcopts []kafkacat.CfgFn, storeName string, matchers cetest.EventMatcher) feature.StepFn {
	return func(ctx context.Context, t feature.T) {
		time.Sleep(20 * time.Second)

		// Install and wait for kafkacat to be ready
		kafkacat.Install(name, kcopts...)(ctx, t)

		// Assert events are received and correct
		assert.OnStore(storeName).MatchEvent(matchers).Exact(1)(ctx, t)
	}
}
