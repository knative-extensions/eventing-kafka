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
	"testing"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	. "github.com/cloudevents/sdk-go/v2/test"
	cetypes "github.com/cloudevents/sdk-go/v2/types"
	"knative.dev/eventing-kafka/test/rekt/features/kafkasource"
	"knative.dev/eventing-kafka/test/rekt/resources/kafkacat"
	rks "knative.dev/eventing-kafka/test/rekt/resources/kafkasource"
	"knative.dev/eventing/pkg/utils"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/system"
	"knative.dev/reconciler-test/pkg/environment"
	"knative.dev/reconciler-test/pkg/k8s"
	"knative.dev/reconciler-test/pkg/knative"
)

const (
	kafkaBootstrapUrlPlain = "my-cluster-kafka-bootstrap.kafka.svc:9092"
	kafkaBootstrapUrlTLS   = "my-cluster-kafka-bootstrap.kafka.svc:9093"
	kafkaBootstrapUrlSASL  = "my-cluster-kafka-bootstrap.kafka.svc:9094"

	kafkaSASLSecret = "strimzi-sasl-secret"
	kafkaTLSSecret  = "strimzi-tls-secret"
)

type authSetup struct {
	bootStrapServer string
	SASLEnabled     bool
	TLSEnabled      bool
}

func TestKafkaSource(t *testing.T) {
	t.Parallel()

	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithTracingConfig,
		k8s.WithEventListener,
		environment.WithPollTimings(2*time.Second, 30*time.Second),
	)

	kc := kubeclient.Get(ctx)
	_, err := utils.CopySecret(kc.CoreV1(), system.Namespace(), kafkaTLSSecret, env.Namespace(), "default")
	if err != nil {
		t.Fatalf("could not copy secret(%s): %v", kafkaTLSSecret, err)
	}

	_, err = utils.CopySecret(kc.CoreV1(), system.Namespace(), kafkaSASLSecret, env.Namespace(), "default")
	if err != nil {
		t.Fatalf("could not copy secret(%s): %v", kafkaSASLSecret, err)
	}

	time, _ := cetypes.ParseTime("2018-04-05T17:31:00Z")
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
		"s512": {
			auth: authSetup{
				bootStrapServer: kafkaBootstrapUrlSASL,
				SASLEnabled:     true,
				TLSEnabled:      false,
			},
		},
		"tls": {
			auth: authSetup{
				bootStrapServer: kafkaBootstrapUrlTLS,
				SASLEnabled:     false,
				TLSEnabled:      true,
			},
		},
	}
	tests := map[string]struct {
		kafkacatCfg  []kafkacat.CfgFn
		matcher      EventMatcher
		intermediary bool
	}{
		"no-ce-event": {
			kafkacatCfg: []kafkacat.CfgFn{
				kafkacat.WithKey("0"),
				kafkacat.WithHeaders(map[string]string{
					"content-type": "application/json",
				}),
				kafkacat.WithPayload(`{"value":5}`),
			},
			matcher: AllOf(
				HasDataContentType("application/json"),
				HasData([]byte(`{"value":5}`)),
				HasExtension("key", "0"),
			),
		},
		"no-ce-event-no-content-type": {
			kafkacatCfg: []kafkacat.CfgFn{
				kafkacat.WithKey("0"),
				kafkacat.WithPayload(`{"value":5}`),
			},
			matcher: AllOf(
				HasData([]byte(`{"value":5}`)),
				HasExtension("key", "0"),
			),
		},
		"no-ce-event-content-type-or-key": {
			kafkacatCfg: []kafkacat.CfgFn{
				kafkacat.WithPayload(`{"value":5}`),
			},
			matcher: AllOf(
				HasData([]byte(`{"value":5}`)),
			),
		},
		"no-ce-event-with-text-plain-body": {
			kafkacatCfg: []kafkacat.CfgFn{
				kafkacat.WithKey("0"),
				kafkacat.WithHeaders(map[string]string{
					"content-type": "text/plain",
				}),
				kafkacat.WithPayload(`simple 10`),
			},
			matcher: AllOf(
				HasDataContentType("text/plain"),
				HasData([]byte("simple 10")),
				HasExtension("key", "0"),
			),
		},
		"structured": {
			kafkacatCfg: []kafkacat.CfgFn{
				kafkacat.WithHeaders(map[string]string{
					"content-type": "application/cloudevents+json",
				}),
				kafkacat.WithPayload(mustJsonMarshal(t, map[string]interface{}{
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
				})),
			},
			matcher: AllOf(
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
			intermediary: true,
		},
		"binary": {
			kafkacatCfg: []kafkacat.CfgFn{
				kafkacat.WithHeaders(map[string]string{
					"ce_specversion":          "1.0",
					"ce_type":                 "com.github.pull.create",
					"ce_source":               "https://github.com/cloudevents/spec/pull",
					"ce_subject":              "123",
					"ce_id":                   "A234-1234-1234",
					"ce_time":                 "2018-04-05T17:31:00Z",
					"content-type":            "application/json",
					"ce_comexampleextension1": "value",
					"ce_comexampleothervalue": "5",
				}),
				kafkacat.WithPayload(mustJsonMarshal(t, map[string]string{
					"hello": "Francesco",
				})),
			},
			matcher: AllOf(
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
			intermediary: true,
		},
	}
	for authName, auth := range auths {
		for name, test := range tests {
			test := test
			name := name + "-" + authName

			ksopts := []rks.CfgFn{
				rks.WithBootstrapServers([]string{auth.auth.bootStrapServer}),
			}

			if auth.auth.TLSEnabled {
				ksopts = append(ksopts,
					rks.WithTLSCert(kafkaTLSSecret, "user.crt"),
					rks.WithTLSKey(kafkaTLSSecret, "user.key"),
					rks.WithTLSCACert(kafkaTLSSecret, "ca.crt"))
			}

			if auth.auth.SASLEnabled {
				ksopts = append(ksopts,
					rks.WithSASLUser(kafkaSASLSecret, "user"),
					rks.WithSASLPassword(kafkaSASLSecret, "password"),
					rks.WithSASLType(kafkaSASLSecret, "saslType"),
					rks.WithTLSCACert(kafkaTLSSecret, "ca.crt"))
			}

			kcopts := test.kafkacatCfg
			kcopts = append(kcopts,
				kafkacat.WithBootstrapServer(kafkaBootstrapUrlPlain))

			t.Run(name, func(t *testing.T) {
				env.Test(ctx, t, kafkasource.DataPlaneDelivery(name, ksopts, kcopts, test.matcher, test.intermediary))
			})
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
