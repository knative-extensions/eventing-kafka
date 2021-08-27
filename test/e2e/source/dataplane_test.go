// +build e2e_ginkgo

/*
Copyright 2019 The Knative Authors
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
package source

import (
	"context"
	"encoding/json"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	. "github.com/cloudevents/sdk-go/v2/test"
	cetypes "github.com/cloudevents/sdk-go/v2/types"

	"knative.dev/eventing/test/lib/naming"
	"knative.dev/eventing/test/lib/recordevents"
	rrecordevents "knative.dev/eventing/test/lib/resources/recordevents"

	sourcesv1beta1 "knative.dev/eventing-kafka/pkg/apis/sources/v1beta1"
	"knative.dev/eventing-kafka/test/e2e/helpers"
	kafkasource "knative.dev/eventing-kafka/test/lib/resources/kafkasource/v1beta1"
	kafkatopic "knative.dev/eventing-kafka/test/lib/resources/kafkatopic/v1beta2"
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

const (
	kafkaBootstrapUrlPlain = "my-cluster-kafka-bootstrap.kafka.svc:9092"
	kafkaBootstrapUrlTLS   = "my-cluster-kafka-bootstrap.kafka.svc:9093"
	kafkaBootstrapUrlSASL  = "my-cluster-kafka-bootstrap.kafka.svc:9094"

	kafkaClusterName      = "my-cluster"
	kafkaClusterNamespace = "kafka"
)

var (
	ts, _ = cetypes.ParseTime("2018-04-05T17:31:00Z")

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

var _ = Describe("KafkaSource dataplane", func() {
	var topicName string
	var eventStore *recordevents.EventInfoStore

	BeforeEach(func() {
		By("creating a Kafka topic")
		topicName = kafkatopic.Install(client, kafkaClusterName, kafkaClusterNamespace)

		By("creating a record events service")
		eventStore = rrecordevents.Install(context.Background(), client)
	})

	// Generate test cases matrix events * auths
	for authName, auth := range auths {
		authName := authName
		auth := auth

		Context("with auth "+authName, func() {
			var sourceName string

			BeforeEach(func() {
				sourceName = naming.MakeRandomK8sName("kafkasource-" + authName)

				By("creating a KafkaSource")
				ks := kafkasource.New(sourceName, auth.bootstrapServer, topicName, eventStore.AsSinkRef())

				if auth.SASLEnabled {
					By("with SASL")
					kafkasource.WithSASLEnabled(ks, kafkaSASLSecret)
				}
				if auth.TLSEnabled {
					By("with TLS")
					kafkasource.WithTLSEnabled(ks, kafkaTLSSecret)
				}

				kafkasource.Create(client, ks)
				Eventually(kafkasource.IsReady(client, ks)).Should(BeTrue())
			})

			DescribeTable("sending events to kafka",

				func(message kafkaMessage, matcher EventMatcher) {
					By("publishing an event to kafka")
					helpers.MustPublishKafkaMessage(client, kafkaBootstrapUrlPlain, topicName, message.Key, message.Headers, message.Payload)

					// If the message is not a CloudEvent,
					// assert ce-source and ce-type is set by the KafkaSource Adapter
					if !message.CloudEvent {
						// Also assert for ce-source and ce-type
						matcher = AllOf(
							HasType(sourcesv1beta1.KafkaEventType),
							HasSource(sourcesv1beta1.KafkaEventSource(client.Namespace, sourceName, topicName)),
							matcher)
					}

					// Assert events are received and correct
					eventStore.AssertExact(1, recordevents.MatchEvent(matcher))
				},

				Entry("not a CloudEvent",
					kafkaMessage{
						Key: "0",
						Headers: map[string]string{
							"content-type": "application/json",
						},
						Payload: `{"value":5}`,
					},
					AllOf(
						HasDataContentType("application/json"),
						HasData([]byte(`{"value":5}`)),
						HasExtension("key", "0"),
					)),

				Entry("not a CloudEvent, no content-type",
					kafkaMessage{
						Key:     "0",
						Payload: `{"value":5}`,
					},
					AllOf(
						HasData([]byte(`{"value":5}`)),
						HasExtension("key", "0"),
					)),

				Entry("not a CloudEvent, no content-type or key",
					kafkaMessage{
						Payload: `{"value":5}`,
					},
					AllOf(
						HasData([]byte(`{"value":5}`)),
					)),

				Entry("not a CloudEvent, with text/plain body",
					kafkaMessage{
						Key: "0",
						Headers: map[string]string{
							"content-type": "text/plain",
						},
						Payload: `simple 10`,
					},
					AllOf(
						HasDataContentType("text/plain"),
						HasData([]byte("simple 10")),
						HasExtension("key", "0"),
					)),

				Entry("structured CloudEvent",
					kafkaMessage{
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
					AllOf(
						HasSpecVersion(cloudevents.VersionV1),
						HasType("com.github.pull.create"),
						HasSource("https://github.com/cloudevents/spec/pull"),
						HasSubject("123"),
						HasId("A234-1234-1234"),
						HasTime(ts),
						HasDataContentType("application/json"),
						HasData([]byte(`{"hello":"Francesco"}`)),
						HasExtension("comexampleextension1", "value"),
						HasExtension("comexampleothervalue", "5"),
					)),

				Entry("binary CloudEvent",
					kafkaMessage{
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
					AllOf(
						HasSpecVersion(cloudevents.VersionV1),
						HasType("com.github.pull.create"),
						HasSource("https://github.com/cloudevents/spec/pull"),
						HasSubject("123"),
						HasId("A234-1234-1234"),
						HasTime(ts),
						HasDataContentType("application/json"),
						HasData([]byte(`{"hello":"Francesco"}`)),
						HasExtension("comexampleextension1", "value"),
						HasExtension("comexampleothervalue", "5"),
					)))
		})
	}
})

func mustJsonMarshal(val interface{}) string {
	data, err := json.Marshal(val)
	if err != nil {
		panic(err)
	}
	return string(data)
}
