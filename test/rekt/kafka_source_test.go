// +build e2e

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

package rekt

import (
	"encoding/json"
	"os"
	"testing"

	. "github.com/cloudevents/sdk-go/v2/test"

	"knative.dev/eventing-kafka/test/rekt/features/kafkasource"
	"knative.dev/eventing-kafka/test/rekt/resources/kafkacat"
	rks "knative.dev/eventing-kafka/test/rekt/resources/kafkasource"
	"knative.dev/pkg/system"
	"knative.dev/reconciler-test/pkg/k8s"
	"knative.dev/reconciler-test/pkg/knative"
)

const (
	kafkaBootstrapUrlPlain   = "my-cluster-kafka-bootstrap.kafka.svc:9092"
	kafkaBootstrapUrlTLS     = "my-cluster-kafka-bootstrap.kafka.svc:9093"
	kafkaBootstrapUrlSASL    = "my-cluster-kafka-bootstrap.kafka.svc:9094"
	kafkaClusterName         = "my-cluster"
	kafkaClusterNamespace    = "kafka"
	kafkaClusterSASLUsername = "my-sasl-user"
	kafkaClusterTLSUsername  = "my-tls-user"

	kafkaSASLSecret = "strimzi-sasl-secret"
	kafkaTLSSecret  = "strimzi-tls-secret"
)

type authSetup struct {
	bootStrapServer string
	SASLEnabled     bool
	TLSEnabled      bool
}

var (
	test_mt_source = os.Getenv("TEST_MT_SOURCE")
)

func TestKafkaSource(t *testing.T) {
	t.Parallel()

	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithTracingConfig,
		k8s.WithEventListener,
	)

	//	time, _ := cetypes.ParseTime("2018-04-05T17:31:00Z")
	auths := map[string]struct {
		auth authSetup
	}{
		"plain": {
			auth: authSetup{
				bootStrapServer: kafkaBootstrapUrlPlain,
				SASLEnabled:     false,
				TLSEnabled:      false,
			},
		},
		// "s512": {
		// 	auth: authSetup{
		// 		bootStrapServer: kafkaBootstrapUrlSASL,
		// 		SASLEnabled:     true,
		// 		TLSEnabled:      false,
		// 	},
		// },
		// "tls": {
		// 	auth: authSetup{
		// 		bootStrapServer: kafkaBootstrapUrlTLS,
		// 		SASLEnabled:     false,
		// 		TLSEnabled:      true,
		// 	},
		// },
	}
	tests := map[string]struct {
		kafkacatCfg []kafkacat.CfgFn
		matcher     EventMatcher
	}{
		"no-event": {
			kafkacatCfg: []kafkacat.CfgFn{
				kafkacat.WithKey("0"),
				kafkacat.WithHeaders(map[string]string{
					"content-type": "application/json",
				}),
				kafkacat.WithPayload(`{"value":5}`),
			},
			matcher: AllOf(
				//HasSource(cloudEventsSourceName),
				// HasType(cloudEventsEventType),
				HasDataContentType("application/json"),
				HasData([]byte(`{"value":5}`)),
				//HasExtension("key", "0"),
			),
		},
		// "no_event_no_content_type": {
		// 	messageKey:     "0",
		// 	messagePayload: `{"value":5}`,
		// 	matcherGen: func(cloudEventsSourceName, cloudEventsEventType string) EventMatcher {
		// 		return AllOf(
		// 			HasSource(cloudEventsSourceName),
		// 			HasType(cloudEventsEventType),
		// 			HasData([]byte(`{"value":5}`)),
		// 			HasExtension("key", "0"),
		// 		)
		// 	},
		// },
		// "no_event_content_type_or_key": {
		// 	messagePayload: `{"value":5}`,
		// 	matcherGen: func(cloudEventsSourceName, cloudEventsEventType string) EventMatcher {
		// 		return AllOf(
		// 			HasSource(cloudEventsSourceName),
		// 			HasType(cloudEventsEventType),
		// 			HasData([]byte(`{"value":5}`)),
		// 		)
		// 	},
		// },
		// "no_event_with_text_plain_body": {
		// 	messageKey: "0",
		// 	messageHeaders: map[string]string{
		// 		"content-type": "text/plain",
		// 	},
		// 	messagePayload: "simple 10",
		// 	matcherGen: func(cloudEventsSourceName, cloudEventsEventType string) EventMatcher {
		// 		return AllOf(
		// 			HasSource(cloudEventsSourceName),
		// 			HasType(cloudEventsEventType),
		// 			HasDataContentType("text/plain"),
		// 			HasData([]byte("simple 10")),
		// 			HasExtension("key", "0"),
		// 		)
		// 	},
		// },
		// "structured": {
		// 	messageHeaders: map[string]string{
		// 		"content-type": "application/cloudevents+json",
		// 	},
		// 	messagePayload: mustJsonMarshal(t, map[string]interface{}{
		// 		"specversion":     "1.0",
		// 		"type":            "com.github.pull.create",
		// 		"source":          "https://github.com/cloudevents/spec/pull",
		// 		"subject":         "123",
		// 		"id":              "A234-1234-1234",
		// 		"time":            "2018-04-05T17:31:00Z",
		// 		"datacontenttype": "application/json",
		// 		"data": map[string]string{
		// 			"hello": "Francesco",
		// 		},
		// 		"comexampleextension1": "value",
		// 		"comexampleothervalue": 5,
		// 	}),
		// 	matcherGen: func(cloudEventsSourceName, cloudEventsEventType string) EventMatcher {
		// 		return AllOf(
		// 			HasSpecVersion(cloudevents.VersionV1),
		// 			HasType("com.github.pull.create"),
		// 			HasSource("https://github.com/cloudevents/spec/pull"),
		// 			HasSubject("123"),
		// 			HasId("A234-1234-1234"),
		// 			HasTime(time),
		// 			HasDataContentType("application/json"),
		// 			HasData([]byte(`{"hello":"Francesco"}`)),
		// 			HasExtension("comexampleextension1", "value"),
		// 			HasExtension("comexampleothervalue", "5"),
		// 		)
		// 	},
		// },
		// "binary": {
		// 	messageHeaders: map[string]string{
		// 		"ce_specversion":          "1.0",
		// 		"ce_type":                 "com.github.pull.create",
		// 		"ce_source":               "https://github.com/cloudevents/spec/pull",
		// 		"ce_subject":              "123",
		// 		"ce_id":                   "A234-1234-1234",
		// 		"ce_time":                 "2018-04-05T17:31:00Z",
		// 		"content-type":            "application/json",
		// 		"ce_comexampleextension1": "value",
		// 		"ce_comexampleothervalue": "5",
		// 	},
		// 	messagePayload: mustJsonMarshal(t, map[string]string{
		// 		"hello": "Francesco",
		// 	}),
		// 	matcherGen: func(cloudEventsSourceName, cloudEventsEventType string) EventMatcher {
		// 		return AllOf(
		// 			HasSpecVersion(cloudevents.VersionV1),
		// 			HasType("com.github.pull.create"),
		// 			HasSource("https://github.com/cloudevents/spec/pull"),
		// 			HasSubject("123"),
		// 			HasId("A234-1234-1234"),
		// 			HasTime(time),
		// 			HasDataContentType("application/json"),
		// 			HasData([]byte(`{"hello":"Francesco"}`)),
		// 			HasExtension("comexampleextension1", "value"),
		// 			HasExtension("comexampleothervalue", "5"),
		// 		)
		// 	},
		// },
	}
	for authName, auth := range auths {
		for name, test := range tests {
			test := test
			name := name + "-" + authName

			ksopts := []rks.CfgFn{
				rks.WithBootstrapServers([]string{auth.auth.bootStrapServer}),
			}

			kcopts := test.kafkacatCfg
			kcopts = append(kcopts,
				kafkacat.WithBootstrapServer(auth.auth.bootStrapServer))

			env.Test(ctx, t, kafkasource.DataPlaneDelivery(name, ksopts, kcopts, test.matcher))
		}
	}

	env.Finish()
}

func mustJsonMarshal(t *testing.T, val interface{}) string {
	data, err := json.Marshal(val)
	if err != nil {
		t.Errorf("unexpected error, %v", err)
	}
	return string(data)
}
