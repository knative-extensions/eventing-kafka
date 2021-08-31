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
	"encoding/json"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	. "github.com/cloudevents/sdk-go/v2/test"
	cetypes "github.com/cloudevents/sdk-go/v2/types"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	"knative.dev/reconciler-test/pkg/environment"
	"knative.dev/reconciler-test/pkg/eventshub"
	"knative.dev/reconciler-test/pkg/eventshub/assert"
	"knative.dev/reconciler-test/pkg/feature"
	"knative.dev/reconciler-test/pkg/manifest"
	"knative.dev/reconciler-test/pkg/state"

	sourcesv1beta1 "knative.dev/eventing-kafka/pkg/apis/sources/v1beta1"
	"knative.dev/eventing-kafka/test/rekt/resources/kafkacat"
	"knative.dev/eventing-kafka/test/rekt/resources/kafkasource"
	"knative.dev/eventing-kafka/test/rekt/resources/kafkatopic"
)

const (
	kafkaBootstrapUrlPlain = "my-cluster-kafka-bootstrap.kafka.svc:9092"
	kafkaBootstrapUrlTLS   = "my-cluster-kafka-bootstrap.kafka.svc:9093"
	kafkaBootstrapUrlSASL  = "my-cluster-kafka-bootstrap.kafka.svc:9094"

	kafkaSASLSecret = "strimzi-sasl-secret"
	kafkaTLSSecret  = "strimzi-tls-secret"
)

type auth struct {
	bootstrapServer string
	SASLEnabled     bool
	TLSEnabled      bool
}

type kafkaMessage struct {
	Key         string
	Headers     map[string]string
	Payload     string
	ContentType string
	CloudEvent  bool
}
type delivery struct {
	auth
	message         kafkaMessage
	bootstrapServer string
	extensions      map[string]string
	matchers        EventMatcher
}

var (
	// All auth configurations
	auths = map[string]auth{
		"plain": {
			bootstrapServer: kafkaBootstrapUrlPlain,
			SASLEnabled:     false,
			TLSEnabled:      false,
		},
		"s512": {
			bootstrapServer: kafkaBootstrapUrlSASL,
			SASLEnabled:     true,
			TLSEnabled:      false,
		},

		"tls": {
			bootstrapServer: kafkaBootstrapUrlTLS,
			SASLEnabled:     false,
			TLSEnabled:      true,
		},
	}
)

// DataPlaneDelivery returns a feature testing
// given a KafkaSource installed before sending events to Kafka
// when an event is sent to Kafka
// then it is received by the sink
func DataPlaneDelivery() []*feature.Feature {
	time, _ := cetypes.ParseTime("2018-04-05T17:31:00Z")
	meta := map[string]delivery{
		"no-ce-event": {
			message: kafkaMessage{
				Key: "0",
				Headers: map[string]string{
					"content-type": "application/json",
				},
				Payload: `{"value":5}`,
			},
			matchers: AllOf(
				HasDataContentType("application/json"),
				HasData([]byte(`{"value":5}`)),
				HasExtension("key", "0"),
			),
		},
		"no-ce-event-no-content-type": {
			message: kafkaMessage{
				Key:     "0",
				Payload: `{"value":5}`,
			},
			matchers: AllOf(
				HasData([]byte(`{"value":5}`)),
				HasExtension("key", "0"),
			),
		},
		"no-ce-event-content-type-or-key": {
			message: kafkaMessage{
				Payload: `{"value":5}`,
			},
			matchers: AllOf(
				HasData([]byte(`{"value":5}`)),
			),
		},
		"no-ce-event-with-text-plain-body": {
			message: kafkaMessage{
				Key: "0",
				Headers: map[string]string{
					"content-type": "text/plain",
				},
				Payload: `simple 10`,
			},
			matchers: AllOf(
				HasDataContentType("text/plain"),
				HasData([]byte("simple 10")),
				HasExtension("key", "0"),
			),
		},
		"structured": {
			message: kafkaMessage{
				Headers: map[string]string{
					"content-type": "application/cloudevents+json",
				},

				Payload: mustJsonMarshal(map[string]interface{}{
					"specversion":     "1.0",
					"type":            "com.github.pull.create",
					"source":          "https://github.com/cloudevents/spec/pull",
					"subject":         "123",
					"id":              "A234-1234-1234",
					"time":            "2018-04-05T17:31:00Z",
					"datacontenttype": "application/json",
					"data": map[string]string{
						"hello": "Francesco",
					},
					"comexampleextension1": "value",
					"comexampleothervalue": 5,
				}),
				CloudEvent: true,
			},
			matchers: AllOf(
				HasSpecVersion(cloudevents.VersionV1),
				HasType("com.github.pull.create"),
				HasSource("https://github.com/cloudevents/spec/pull"),
				HasSubject("123"),
				HasId("A234-1234-1234"),
				HasTime(time),
				HasDataContentType("application/json"),
				HasData([]byte(`{"hello":"Francesco"}`)),
				HasExtension("comexampleextension1", "value"),
				HasExtension("comexampleothervalue", "5"),
			),
		},
		"binary": {
			message: kafkaMessage{
				Headers: map[string]string{
					"ce_specversion":          "1.0",
					"ce_type":                 "com.github.pull.create",
					"ce_source":               "https://github.com/cloudevents/spec/pull",
					"ce_subject":              "123",
					"ce_id":                   "A234-1234-1234",
					"ce_time":                 "2018-04-05T17:31:00Z",
					"content-type":            "application/json",
					"ce_comexampleextension1": "value",
					"ce_comexampleothervalue": "5",
				},
				Payload: mustJsonMarshal(map[string]string{
					"hello": "Francesco",
				}),
				CloudEvent: true,
			},
			matchers: AllOf(
				HasSpecVersion(cloudevents.VersionV1),
				HasType("com.github.pull.create"),
				HasSource("https://github.com/cloudevents/spec/pull"),
				HasSubject("123"),
				HasId("A234-1234-1234"),
				HasTime(time),
				HasDataContentType("application/json"),
				HasData([]byte(`{"hello":"Francesco"}`)),
				HasExtension("comexampleextension1", "value"),
				HasExtension("comexampleothervalue", "5"),
			),
		},
	}

	var features []*feature.Feature

	for authName, auth := range auths {
		for name, data := range meta {
			name := name + "-" + authName

			// data is a copy.
			data.bootstrapServer = auth.bootstrapServer
			data.auth = auth

			features = append(features, dataPlaneDelivery(name, data))
		}
	}

	return features
}

func dataPlaneDelivery(name string, data delivery) *feature.Feature {
	f := feature.NewFeatureNamed("Delivery/" + name)

	// Setup:
	// (kafkacat) -> KafkaTopic -> KafkaSource -> Sink

	// Setup kafkacat (config only)
	f.Setup("set kafka message", func(ctx context.Context, t feature.T) {
		state.SetOrFail(ctx, t, "message", data.message)
	})
	f.Setup("set bootstrap server", func(ctx context.Context, t feature.T) {
		state.SetOrFail(ctx, t, "bootstrapServer", data.bootstrapServer)
	})

	// Setup topic
	topicName := feature.MakeRandomK8sName("topic") // A k8s name is also a valid topic name.

	f.Setup("set kafka topic name", func(ctx context.Context, t feature.T) {
		state.SetOrFail(ctx, t, "topicName", topicName)
	})

	f.Setup("install kafka topic", kafkatopic.Install(topicName))

	// Setup sink
	sinkName := feature.MakeRandomK8sName("sink")
	f.Setup("set sink name", func(ctx context.Context, t feature.T) {
		state.SetOrFail(ctx, t, "sinkName", sinkName)
	})

	f.Setup("install sink", eventshub.Install(sinkName, eventshub.StartReceiver))

	// Setup source

	// options
	ksopts := []manifest.CfgFn{
		kafkasource.WithBootstrapServers([]string{data.bootstrapServer}),
		kafkasource.WithTopics([]string{topicName}),
		kafkasource.WithExtensions(data.extensions),
		kafkasource.WithSink(&duckv1.KReference{
			Kind:       "Service",
			Name:       sinkName,
			APIVersion: "v1",
		}, ""),
	}

	if data.auth.TLSEnabled {
		ksopts = append(ksopts,
			kafkasource.WithTLSCert(kafkaTLSSecret, "user.crt"),
			kafkasource.WithTLSKey(kafkaTLSSecret, "user.key"),
			kafkasource.WithTLSCACert(kafkaTLSSecret, "ca.crt"))
	}

	if data.auth.SASLEnabled {
		ksopts = append(ksopts,
			kafkasource.WithSASLUser(kafkaSASLSecret, "user"),
			kafkasource.WithSASLPassword(kafkaSASLSecret, "password"),
			kafkasource.WithSASLType(kafkaSASLSecret, "saslType"),
			kafkasource.WithTLSCACert(kafkaTLSSecret, "ca.crt"))
	}

	// installation
	f.Setup("install a kafka source", kafkasource.Install(name, ksopts...))

	// Check the setup meets some requirements (not what is actually asserted)
	f.Requirement("kafka topic must be ready", kafkatopic.IsReady(topicName))
	f.Requirement("kafka source must be ready", kafkasource.IsReady(name))

	f.Assert("forward events from topic to sink", sinkReceiveEvent(name, data.matchers))

	return f
}

func sinkReceiveEvent(name string, matchers EventMatcher) func(ctx context.Context, t feature.T) {
	return func(ctx context.Context, t feature.T) {
		// Restore state
		var message kafkaMessage
		state.GetOrFail(ctx, t, "message", &message)

		topicName := state.GetStringOrFail(ctx, t, "topicName")
		sinkName := state.GetStringOrFail(ctx, t, "sinkName")

		// Install kafkacat
		kcopts := []manifest.CfgFn{
			kafkacat.WithBootstrapServer(kafkaBootstrapUrlPlain),
			kafkacat.WithTopic(topicName),
			kafkacat.WithKey(message.Key),
			kafkacat.WithHeaders(message.Headers),
			kafkacat.WithPayload(message.Payload),
		}

		// If the message is not a CloudEvent,
		// assert ce-source and ce-type is set by the KafkaSource Adapter
		if !message.CloudEvent {
			// Also assert for ce-source and ce-type
			env := environment.FromContext(ctx)
			matchers = AllOf(
				HasType(sourcesv1beta1.KafkaEventType),
				HasSource(sourcesv1beta1.KafkaEventSource(env.Namespace(), name, topicName)),
				matchers)
		}

		// Install and wait for kafkacat to be ready
		kafkacat.Install(topicName, kcopts...)(ctx, t)

		// Assert events are received and correct
		assert.OnStore(sinkName).MatchEvent(matchers).Exact(1)(ctx, t)
	}
}

func mustJsonMarshal(val interface{}) string {
	data, err := json.Marshal(val)
	if err != nil {
		panic(err)
	}
	return string(data)
}
