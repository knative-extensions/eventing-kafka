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

	. "github.com/cloudevents/sdk-go/v2/test"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/pointer"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	"knative.dev/reconciler-test/pkg/environment"
	"knative.dev/reconciler-test/pkg/eventshub"
	"knative.dev/reconciler-test/pkg/eventshub/assert"
	"knative.dev/reconciler-test/pkg/feature"
	"knative.dev/reconciler-test/pkg/state"

	kafkaclient "knative.dev/eventing-kafka/pkg/client/injection/client"
	"knative.dev/eventing-kafka/test/rekt/resources/kafkaproxy"
	"knative.dev/eventing-kafka/test/rekt/resources/kafkasource"
	"knative.dev/eventing-kafka/test/rekt/resources/kafkatopic"
)

// Scaling returns a feature testing KafkaSource scaling up and down.
func Scaling() *feature.Feature {
	f := feature.NewFeature()

	topic := feature.MakeRandomK8sName("topic")

	// Setup:
	// Sender (eventshub) -> KafkaProxy -> KafkaTopic -> KafkaSource -> Sink (eventshub)
	proxyName := feature.MakeRandomK8sName("proxy")
	f.Setup("set proxy name state", func(ctx context.Context, t feature.T) {
		state.SetOrFail(ctx, t, "proxyName", proxyName)
	})

	f.Setup("install Kafka proxy", kafkaproxy.Install(proxyName,
		kafkaproxy.WithBootstrapServer(kafkaBootstrapUrlPlain),
		kafkaproxy.WithTopic(topic)))

	f.Setup("set topic name state", func(ctx context.Context, t feature.T) {
		state.SetOrFail(ctx, t, "topicName", topic)
	})
	f.Setup("install kafka topic", kafkatopic.Install(topic))

	// Setup sink
	sinkName := feature.MakeRandomK8sName("sink")
	f.Setup("set sink state", func(ctx context.Context, t feature.T) {
		state.SetOrFail(ctx, t, "sinkName", sinkName)
	})
	f.Setup("install sink", eventshub.Install(sinkName, eventshub.StartReceiver))

	// Setup source

	sourceName := feature.MakeRandomK8sName("source")
	f.Setup("set source state", func(ctx context.Context, t feature.T) {
		state.SetOrFail(ctx, t, "sourceName", sourceName)
	})

	// options
	ksopts := []kafkasource.CfgFn{
		kafkasource.WithBootstrapServers([]string{kafkaBootstrapUrlPlain}),
		kafkasource.WithTopics([]string{topic}),
		kafkasource.WithSink(&duckv1.KReference{
			Kind:       "Service",
			Name:       sinkName,
			APIVersion: "v1",
		}, ""),
	}

	// installation

	f.Setup("install a kafka source", kafkasource.Install(sourceName, ksopts...))

	// Check the setup meets some requirements (not what is actually asserted)
	f.Requirement("kafka topic must be ready", kafkatopic.IsReady(topic))
	f.Requirement("kafka source must be ready", kafkasource.IsReady(sourceName))

	f.Assert("sink receive steady stream of events, with no duplicates or missing events for 10s", sinkReceiveSteadyEvents)
	return f
}

func sinkReceiveSteadyEvents(ctx context.Context, t feature.T) {
	proxyName := state.GetStringOrFail(ctx, t, "proxyName")
	sinkName := state.GetStringOrFail(ctx, t, "sinkName")
	sourceName := state.GetStringOrFail(ctx, t, "sourceName")

	url, err := kafkaproxy.Address(ctx, proxyName)
	if err != nil {
		t.Fatalf("failed to get the proxy URL: %v", err)
	}

	sender := feature.MakeRandomK8sName("sender")
	eventshub.Install(sender,
		eventshub.StartSenderURL(url.String()),
		eventshub.EnableIncrementalId,
		eventshub.InputEvent(FullEvent()),
		eventshub.SendMultipleEvents(20, time.Second), // 20 events over 20s
	)(ctx, t)

	time.Sleep(2 * time.Second)

	env := environment.FromContext(ctx)
	source, err := kafkaclient.Get(ctx).SourcesV1beta1().KafkaSources(env.Namespace()).Get(ctx, sourceName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get the KafkaSource object: %v", err)
	}
	source.Spec.Consumers = pointer.Int32Ptr(130)

	_, err = kafkaclient.Get(ctx).SourcesV1beta1().KafkaSources(env.Namespace()).Update(ctx, source, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("failed to update the KafkaSource object: %v", err)
	}

	time.Sleep(20 * time.Second)

	assert.OnStore(sinkName).Exact(20)(ctx, t)
}
