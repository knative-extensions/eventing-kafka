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
	"fmt"
	"os"
	"time"

	"github.com/cloudevents/sdk-go/v2/event"
	. "github.com/cloudevents/sdk-go/v2/test"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	"knative.dev/reconciler-test/pkg/environment"
	"knative.dev/reconciler-test/pkg/eventshub"
	"knative.dev/reconciler-test/pkg/eventshub/assert"
	"knative.dev/reconciler-test/pkg/feature"
	"knative.dev/reconciler-test/pkg/state"

	kafkaclient "knative.dev/eventing-kafka/pkg/client/injection/client"
	"knative.dev/eventing-kafka/test/rekt/resources/kafkacat"
	"knative.dev/eventing-kafka/test/rekt/resources/kafkasource"
	"knative.dev/eventing-kafka/test/rekt/resources/kafkatopic"
)

var test_mt_source = os.Getenv("TEST_MT_SOURCE")

func MtDataPlaneDelivery() *feature.Feature {
	name := "mt-tenant-test-plain"
	f := feature.NewFeatureNamed("Delivery/" + name)
	delivery := delivery{
		message: kafkaMessage{
			Key:     "0",
			Payload: `{"value":711}`,
		},
		matchers: AllOf(
			HasData([]byte(`{"value":711}`)),
			HasExtension("key", "0"),
		),
	}

	delivery.bootstrapServer = auths["plain"].bootstrapServer
	delivery.auth = auths["plain"]

	f.Setup("set kafka message", func(ctx context.Context, t feature.T) {
		state.SetOrFail(ctx, t, "message", delivery.message)
	})
	f.Setup("set bootstrap server", func(ctx context.Context, t feature.T) {
		state.SetOrFail(ctx, t, "bootstrapServer", delivery.bootstrapServer)
	})

	topicName := feature.MakeRandomK8sName("topic") // A k8s name is also a valid topic name.

	f.Setup("set kafka topic name", func(ctx context.Context, t feature.T) {
		state.SetOrFail(ctx, t, "topicName", topicName)
	})

	f.Setup("install kafka topic", kafkatopic.Install(topicName))

	sinkName := feature.MakeRandomK8sName("sink")
	f.Setup("set sink name", func(ctx context.Context, t feature.T) {
		state.SetOrFail(ctx, t, "sinkName", sinkName)
	})

	f.Setup("install sink", eventshub.Install(sinkName, eventshub.StartReceiver))

	ksopts := []kafkasource.CfgFn{
		kafkasource.WithBootstrapServers([]string{delivery.bootstrapServer}),
		kafkasource.WithTopics([]string{topicName}),
		kafkasource.WithSink(&duckv1.KReference{
			Kind:       "Service",
			Name:       sinkName,
			APIVersion: "v1",
		}, ""),
	}

	f.Setup("install a kafka source", kafkasource.Install(name, ksopts...))
	f.Requirement("kafka topic must be ready", kafkatopic.IsReady(topicName))
	f.Requirement("kafka source must be ready", kafkasource.IsReady(name))
	f.Assert("different KafkaSource exists on the same pod", sourcesPlaced(name, delivery.matchers))

	return f
}

func sourcesPlaced(name string, matchers EventMatcher) func(ctx context.Context, t feature.T) {
	return func(ctx context.Context, t feature.T) {
		var message kafkaMessage
		state.GetOrFail(ctx, t, "message", &message)

		topicName := state.GetStringOrFail(ctx, t, "topicName")
		sinkName := state.GetStringOrFail(ctx, t, "sinkName")

		if test_mt_source == "1" {
			time.Sleep(10 * time.Second)
		}

		kcopts := []kafkacat.CfgFn{
			kafkacat.WithBootstrapServer(kafkaBootstrapUrlPlain),
			kafkacat.WithTopic(topicName),
			kafkacat.WithKey(message.Key),
			kafkacat.WithHeaders(message.Headers),
			kafkacat.WithPayload(message.Payload),
		}
		env := environment.FromContext(ctx)
		matchers = AllOf(
			func(have event.Event) error {
				sourceInterface := kafkaclient.Get(ctx).SourcesV1beta1()

				source1, err := sourceInterface.KafkaSources(env.Namespace()).Get(ctx, "mt-tenant-test-plain", metav1.GetOptions{})
				if err != nil {
					return fmt.Errorf("Error when getting Kafka source1")
				}

				source2, err := sourceInterface.KafkaSources(env.Namespace()).Get(ctx, "no-ce-event-plain", metav1.GetOptions{})
				if err != nil {
					return fmt.Errorf("Error when getting Kafka source2")
				}

				if source1.Status.Placement[0].PodName != source2.Status.Placement[0].PodName && len(source2.Status.Placement[0].PodName) != 0 {
					return fmt.Errorf("Two Kafka source do not belong to the same pod")
				}

				return nil
			})

		kafkacat.Install(topicName, kcopts...)(ctx, t)
		assert.OnStore(sinkName).MatchEvent(matchers).Exact(1)(ctx, t)
	}
}
