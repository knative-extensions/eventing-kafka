//+build e2e mtsource
//+build source

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

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	. "github.com/cloudevents/sdk-go/v2/test"
	cetypes "github.com/cloudevents/sdk-go/v2/types"
	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"

	"knative.dev/eventing/pkg/utils"
	testlib "knative.dev/eventing/test/lib"
	"knative.dev/eventing/test/lib/recordevents"
	"knative.dev/eventing/test/lib/resources"

	sourcesv1alpha1 "knative.dev/eventing-kafka/pkg/apis/sources/v1alpha1"
	sourcesv1beta1 "knative.dev/eventing-kafka/pkg/apis/sources/v1beta1"
	"knative.dev/eventing-kafka/test/e2e/helpers"
	contribtestlib "knative.dev/eventing-kafka/test/lib"
	contribresources "knative.dev/eventing-kafka/test/lib/resources"
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

func withAuthEnablementV1Beta1(auth authSetup) contribresources.KafkaSourceV1Beta1Option {
	// We test with sasl512 and enable tls with it, so check tls first
	if auth.TLSEnabled == true {
		return func(ks *sourcesv1beta1.KafkaSource) {
			ks.Spec.KafkaAuthSpec.Net.TLS.Enable = true
			ks.Spec.KafkaAuthSpec.Net.TLS.CACert.SecretKeyRef = &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: kafkaTLSSecret,
				},
				Key: "ca.crt",
			}
			ks.Spec.KafkaAuthSpec.Net.TLS.Cert.SecretKeyRef = &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: kafkaTLSSecret,
				},
				Key: "user.crt",
			}
			ks.Spec.KafkaAuthSpec.Net.TLS.Key.SecretKeyRef = &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: kafkaTLSSecret,
				},
				Key: "user.key",
			}
		}
	}
	if auth.SASLEnabled == true {
		return func(ks *sourcesv1beta1.KafkaSource) {
			ks.Spec.KafkaAuthSpec.Net.SASL.Enable = true
			ks.Spec.KafkaAuthSpec.Net.SASL.User.SecretKeyRef = &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: kafkaSASLSecret,
				},
				Key: "user",
			}
			ks.Spec.KafkaAuthSpec.Net.SASL.Password.SecretKeyRef = &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: kafkaSASLSecret,
				},
				Key: "password",
			}
			ks.Spec.KafkaAuthSpec.Net.SASL.Type.SecretKeyRef = &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: kafkaSASLSecret,
				},
				Key: "saslType",
			}
			ks.Spec.KafkaAuthSpec.Net.TLS.Enable = true
			ks.Spec.KafkaAuthSpec.Net.TLS.CACert.SecretKeyRef = &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: kafkaSASLSecret,
				},
				Key: "ca.crt",
			}
		}
	}
	return func(ks *sourcesv1beta1.KafkaSource) {}
}

func withAuthEnablementV1Alpha1(auth authSetup) contribresources.KafkaSourceV1Alpha1Option {
	// We test with sasl512 and enable tls with it, so check tls first
	if auth.TLSEnabled == true {
		return func(ks *sourcesv1alpha1.KafkaSource) {
			ks.Spec.KafkaAuthSpec.Net.TLS.Enable = true
			ks.Spec.KafkaAuthSpec.Net.TLS.CACert.SecretKeyRef = &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: kafkaTLSSecret,
				},
				Key: "ca.crt",
			}
			ks.Spec.KafkaAuthSpec.Net.TLS.Cert.SecretKeyRef = &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: kafkaTLSSecret,
				},
				Key: "user.crt",
			}
			ks.Spec.KafkaAuthSpec.Net.TLS.Key.SecretKeyRef = &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: kafkaTLSSecret,
				},
				Key: "user.key",
			}
		}
	}
	if auth.SASLEnabled == true {
		return func(ks *sourcesv1alpha1.KafkaSource) {
			ks.Spec.KafkaAuthSpec.Net.SASL.Enable = true
			ks.Spec.KafkaAuthSpec.Net.SASL.User.SecretKeyRef = &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: kafkaSASLSecret,
				},
				Key: "user",
			}
			ks.Spec.KafkaAuthSpec.Net.SASL.Password.SecretKeyRef = &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: kafkaSASLSecret,
				},
				Key: "password",
			}
			ks.Spec.KafkaAuthSpec.Net.SASL.Type.SecretKeyRef = &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: kafkaSASLSecret,
				},
				Key: "saslType",
			}
			ks.Spec.KafkaAuthSpec.Net.TLS.Enable = true
			ks.Spec.KafkaAuthSpec.Net.TLS.CACert.SecretKeyRef = &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: kafkaSASLSecret,
				},
				Key: "ca.crt",
			}
		}
	}
	return func(ks *sourcesv1alpha1.KafkaSource) {}
}
func testKafkaSource(t *testing.T, name string, version string, messageKey string, messageHeaders map[string]string, messagePayload string, matcherGen func(cloudEventsSourceName, cloudEventsEventType string) EventMatcher, auth authSetup) {
	name = fmt.Sprintf("%s-%s", name, version)

	var (
		kafkaTopicName     = uuid.New().String()
		consumerGroup      = uuid.New().String()
		recordEventPodName = "e2e-kafka-r-" + strings.ReplaceAll(name, "_", "-")
		kafkaSourceName    = "e2e-kafka-source-" + strings.ReplaceAll(name, "_", "-")
	)

	client := testlib.Setup(t, true)
	defer testlib.TearDown(client)

	if auth.SASLEnabled {
		_, err := utils.CopySecret(client.Kube.CoreV1(), "knative-eventing", kafkaSASLSecret, client.Namespace, "default")
		if err != nil {
			t.Fatalf("could not copy SASL secret(%s): %v", kafkaSASLSecret, err)
		}
	}
	if auth.TLSEnabled {
		_, err := utils.CopySecret(client.Kube.CoreV1(), "knative-eventing", kafkaTLSSecret, client.Namespace, "default")
		if err != nil {
			t.Fatalf("could not copy secret(%s): %v", kafkaTLSSecret, err)
		}
	}
	helpers.MustCreateTopic(client, kafkaClusterName, kafkaClusterNamespace, kafkaTopicName)
	if len(recordEventPodName) > 63 {
		recordEventPodName = recordEventPodName[:63]
	}
	eventTracker, _ := recordevents.StartEventRecordOrFail(context.Background(), client, recordEventPodName)

	var (
		cloudEventsSourceName string
		cloudEventsEventType  string
	)

	t.Logf("Creating KafkaSource %s", version)
	switch version {
	case "v1alpha1":
		contribtestlib.CreateKafkaSourceV1Alpha1OrFail(client, contribresources.KafkaSourceV1Alpha1(
			auth.bootStrapServer,
			kafkaTopicName,
			resources.ServiceRef(recordEventPodName),
			contribresources.WithNameV1Alpha1(kafkaSourceName),
			contribresources.WithConsumerGroupV1Alpha1(consumerGroup),
			withAuthEnablementV1Alpha1(auth),
		))
		cloudEventsSourceName = sourcesv1alpha1.KafkaEventSource(client.Namespace, kafkaSourceName, kafkaTopicName)
		cloudEventsEventType = sourcesv1alpha1.KafkaEventType
	case "v1beta1":
		contribtestlib.CreateKafkaSourceV1Beta1OrFail(client, contribresources.KafkaSourceV1Beta1(
			auth.bootStrapServer,
			kafkaTopicName,
			resources.ServiceRef(recordEventPodName),
			contribresources.WithNameV1Beta1(kafkaSourceName),
			contribresources.WithConsumerGroupV1Beta1(consumerGroup),
			withAuthEnablementV1Beta1(auth),
		))
		cloudEventsSourceName = sourcesv1beta1.KafkaEventSource(client.Namespace, kafkaSourceName, kafkaTopicName)
		cloudEventsEventType = sourcesv1beta1.KafkaEventType
	default:
		t.Fatalf("Unknown KafkaSource version %s", version)
	}

	client.WaitForAllTestResourcesReadyOrFail(context.Background())

	// See https://github.com/knative-sandbox/eventing-kafka/issues/411
	if test_mt_source == "1" {
		time.Sleep(20 * time.Second)
	}

	helpers.MustPublishKafkaMessage(client, kafkaBootstrapUrlPlain, kafkaTopicName, messageKey, messageHeaders, messagePayload)

	eventTracker.AssertExact(1, recordevents.MatchEvent(matcherGen(cloudEventsSourceName, cloudEventsEventType)))
}

func TestKafkaSource(t *testing.T) {
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
		messageKey     string
		messageHeaders map[string]string
		messagePayload string
		matcherGen     func(cloudEventsSourceName, cloudEventsEventType string) EventMatcher
	}{
		"no_event": {
			messageKey: "0",
			messageHeaders: map[string]string{
				"content-type": "application/json",
			},
			messagePayload: `{"value":5}`,
			matcherGen: func(cloudEventsSourceName, cloudEventsEventType string) EventMatcher {
				return AllOf(
					HasSource(cloudEventsSourceName),
					HasType(cloudEventsEventType),
					HasDataContentType("application/json"),
					HasData([]byte(`{"value":5}`)),
					HasExtension("key", "0"),
				)
			},
		},
		"no_event_no_content_type": {
			messageKey:     "0",
			messagePayload: `{"value":5}`,
			matcherGen: func(cloudEventsSourceName, cloudEventsEventType string) EventMatcher {
				return AllOf(
					HasSource(cloudEventsSourceName),
					HasType(cloudEventsEventType),
					HasData([]byte(`{"value":5}`)),
					HasExtension("key", "0"),
				)
			},
		},
		"no_event_content_type_or_key": {
			messagePayload: `{"value":5}`,
			matcherGen: func(cloudEventsSourceName, cloudEventsEventType string) EventMatcher {
				return AllOf(
					HasSource(cloudEventsSourceName),
					HasType(cloudEventsEventType),
					HasData([]byte(`{"value":5}`)),
				)
			},
		},
		"no_event_with_text_plain_body": {
			messageKey: "0",
			messageHeaders: map[string]string{
				"content-type": "text/plain",
			},
			messagePayload: "simple 10",
			matcherGen: func(cloudEventsSourceName, cloudEventsEventType string) EventMatcher {
				return AllOf(
					HasSource(cloudEventsSourceName),
					HasType(cloudEventsEventType),
					HasDataContentType("text/plain"),
					HasData([]byte("simple 10")),
					HasExtension("key", "0"),
				)
			},
		},
		"structured": {
			messageHeaders: map[string]string{
				"content-type": "application/cloudevents+json",
			},
			messagePayload: mustJsonMarshal(t, map[string]interface{}{
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
			matcherGen: func(cloudEventsSourceName, cloudEventsEventType string) EventMatcher {
				return AllOf(
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
				)
			},
		},
		"binary": {
			messageHeaders: map[string]string{
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
			messagePayload: mustJsonMarshal(t, map[string]string{
				"hello": "Francesco",
			}),
			matcherGen: func(cloudEventsSourceName, cloudEventsEventType string) EventMatcher {
				return AllOf(
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
				)
			},
		},
	}
	for authName, auth := range auths {
		for name, test := range tests {
			test := test
			name := name + "_" + authName
			t.Run(name+"-v1alpha1", func(t *testing.T) {
				testKafkaSource(t, name, "v1alpha1", test.messageKey, test.messageHeaders, test.messagePayload, test.matcherGen, auth.auth)
			})
			t.Run(name+"-v1beta1", func(t *testing.T) {
				testKafkaSource(t, name, "v1beta1", test.messageKey, test.messageHeaders, test.messagePayload, test.matcherGen, auth.auth)
			})
		}
	}
}

func mustJsonMarshal(t *testing.T, val interface{}) string {
	data, err := json.Marshal(val)
	if err != nil {
		t.Errorf("unexpected error, %v", err)
	}
	return string(data)
}

type message struct {
	cloudEventType string
	payload        []byte
	headers        map[string]string
	key            string
}

type updateTest struct {
	auth      authSetup
	topicName string
	sinkName  string
}

var (
	defaultKafkaSource = updateTest{
		auth: authSetup{
			bootStrapServer: kafkaBootstrapUrlTLS,
			SASLEnabled:     false,
			TLSEnabled:      true,
		},
		topicName: "initial-topic",
		sinkName:  "default-event-recorder",
	}
)

func TestKafkaSourceUpdate(t *testing.T) {

	testCases := map[string]updateTest{
		"no-change": defaultKafkaSource,
		"change-sink": {
			auth:      defaultKafkaSource.auth,
			topicName: defaultKafkaSource.topicName,
			sinkName:  "update-event-recorder",
		},
		"change-topic": {
			auth:      defaultKafkaSource.auth,
			topicName: "update-topic",
			sinkName:  defaultKafkaSource.sinkName,
		},
		"change-bootstrap-server": {
			auth: authSetup{
				bootStrapServer: kafkaBootstrapUrlPlain,
				TLSEnabled:      false,
				SASLEnabled:     false,
			},
			topicName: defaultKafkaSource.topicName,
			sinkName:  defaultKafkaSource.sinkName,
		},
	}

	for name, test := range testCases {
		test := test
		t.Run(name, func(t *testing.T) {
			testKafkaSourceUpdate(t, name, test)
		})
	}
}

func testKafkaSourceUpdate(t *testing.T, name string, test updateTest) {

	messagePayload := []byte(`{"value":5}`)
	matcherGen := func(cloudEventsSourceName, originalOrUpdate string) EventMatcher {
		return AllOf(
			HasSource(cloudEventsSourceName),
			HasData(messagePayload),
			HasExtension("kafkaheaderceoriginalorupdate", originalOrUpdate))
	}

	originalMessage := message{
		payload: messagePayload,
		headers: map[string]string{
			"ce_originalorupdate": "original",
		},
		key: "0",
	}
	updateMessage := message{
		payload: messagePayload,
		headers: map[string]string{
			"ce_originalorupdate": "update",
		},
		key: "0",
	}

	client := testlib.Setup(t, true)
	defer testlib.TearDown(client)

	t.Logf("Creating topic: %s\n", defaultKafkaSource.topicName+name)
	helpers.MustCreateTopic(client, kafkaClusterName, kafkaClusterNamespace, defaultKafkaSource.topicName+name)

	t.Logf("Copying secrets: %s\n", defaultKafkaSource.topicName+name)
	_, err := utils.CopySecret(client.Kube.CoreV1(), "knative-eventing", kafkaTLSSecret, client.Namespace, "default")
	if err != nil {
		t.Fatalf("could not copy secret(%s): %v", kafkaTLSSecret, err)
	}

	t.Logf("Creating default eventrecorder pod: %s\n", defaultKafkaSource.sinkName)
	originalEventTracker, _ := recordevents.StartEventRecordOrFail(context.Background(), client, defaultKafkaSource.sinkName)
	client.WaitForAllTestResourcesReadyOrFail(context.Background())

	kafkaSourceName := "e2e-kafka-source-" + name

	t.Logf("Creating kafkasource: %s\n", kafkaSourceName)
	contribtestlib.CreateKafkaSourceV1Beta1OrFail(client, contribresources.KafkaSourceV1Beta1(
		defaultKafkaSource.auth.bootStrapServer,
		defaultKafkaSource.topicName+name,
		resources.ServiceRef(defaultKafkaSource.sinkName),
		contribresources.WithNameV1Beta1(kafkaSourceName),
		withAuthEnablementV1Beta1(defaultKafkaSource.auth),
	))
	client.WaitForAllTestResourcesReadyOrFail(context.Background())

	// See https://github.com/knative-sandbox/eventing-kafka/issues/411
	if test_mt_source == "1" {
		time.Sleep(20 * time.Second)
	}

	t.Logf("Send update event to kafkatopic")
	helpers.MustPublishKafkaMessage(client, kafkaBootstrapUrlPlain,
		defaultKafkaSource.topicName+name,
		originalMessage.key, originalMessage.headers, string(originalMessage.payload))
	eventSourceName := sourcesv1beta1.KafkaEventSource(client.Namespace, kafkaSourceName, defaultKafkaSource.topicName+name)
	originalEventTracker.AssertExact(1, recordevents.MatchEvent(matcherGen(eventSourceName, "original")))
	t.Logf("Properly received original event for %s\n", eventSourceName)

	var (
		ksObj               *sourcesv1beta1.KafkaSource
		newSinkEventTracker *recordevents.EventInfoStore
	)
	ksObj = contribtestlib.GetKafkaSourceV1Beta1OrFail(client, kafkaSourceName)
	if ksObj == nil {
		t.Fatalf("Unabled to Get kafkasource: %s/%s\n", client.Namespace, kafkaSourceName)
	}
	if test.topicName != defaultKafkaSource.topicName {
		helpers.MustCreateTopic(client, kafkaClusterName, kafkaClusterNamespace, test.topicName+name)
		ksObj.Spec.Topics = []string{test.topicName + name}
		eventSourceName = sourcesv1beta1.KafkaEventSource(client.Namespace, kafkaSourceName, test.topicName+name)
	}
	if test.sinkName != defaultKafkaSource.sinkName {
		kSinkRef := resources.ServiceKRef(test.sinkName)
		ksObj.Spec.Sink.Ref = kSinkRef

		newSinkEventTracker, _ = recordevents.StartEventRecordOrFail(context.Background(), client, test.sinkName)
	}
	if test.auth.bootStrapServer != defaultKafkaSource.auth.bootStrapServer {
		ksObj.Spec.KafkaAuthSpec.BootstrapServers = []string{test.auth.bootStrapServer}
		ksObj.Spec.KafkaAuthSpec.Net.TLS.Enable = test.auth.TLSEnabled
		ksObj.Spec.KafkaAuthSpec.Net.SASL.Enable = test.auth.SASLEnabled
	}

	contribtestlib.UpdateKafkaSourceV1Beta1OrFail(client, ksObj)
	client.WaitForAllTestResourcesReadyOrFail(context.Background())

	// See https://github.com/knative-sandbox/eventing-kafka/issues/411
	if test_mt_source == "1" {
		time.Sleep(20 * time.Second)
	}

	t.Logf("Send update event to kafkatopic")
	helpers.MustPublishKafkaMessage(client, kafkaBootstrapUrlPlain,
		test.topicName+name,
		updateMessage.key, updateMessage.headers, string(updateMessage.payload))

	if test.sinkName != defaultKafkaSource.sinkName {
		newSinkEventTracker.AssertExact(1, recordevents.MatchEvent(matcherGen(eventSourceName, "update")))
	} else {
		originalEventTracker.AssertExact(1, recordevents.MatchEvent(matcherGen(eventSourceName, "update")))
	}

}
