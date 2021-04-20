// +build e2e
// +build source mtsource

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
	"testing"
	"time"

	. "github.com/cloudevents/sdk-go/v2/test"
	sourcesv1beta1 "knative.dev/eventing-kafka/pkg/apis/sources/v1beta1"
	"knative.dev/eventing-kafka/test/e2e/helpers"
	contribtestlib "knative.dev/eventing-kafka/test/lib"
	contribresources "knative.dev/eventing-kafka/test/lib/resources"
	"knative.dev/eventing/pkg/utils"
	testlib "knative.dev/eventing/test/lib"
	"knative.dev/eventing/test/lib/recordevents"
	"knative.dev/eventing/test/lib/resources"
)

func TestKafkaSource(t *testing.T) {
	AssureKafkaSourceIsOperational(t, func(auth, testCase, version string) bool {
		return true
	})
}

func TestKafkaSourceUpdate(t *testing.T) {
	t.Skip("Skip these since they're flaky")
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
	helpers.MustCreateTopic(client, kafkaClusterName, kafkaClusterNamespace, defaultKafkaSource.topicName+name, 10)

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
	if testMtSource == "1" {
		time.Sleep(20 * time.Second)
	}

	t.Logf("Send update event to kafkatopic")
	helpers.MustPublishKafkaMessage(client, kafkaBootstrapUrlPlain,
		defaultKafkaSource.topicName+name,
		originalMessage.key, originalMessage.headers, string(originalMessage.payload))
	eventSourceName := sourcesv1beta1.KafkaEventSource(client.Namespace, kafkaSourceName, defaultKafkaSource.topicName+name)
	originalEventTracker.AssertExact(1, recordevents.MatchEvent(matcherGen(eventSourceName, "original")))
	t.Logf("Properly received original event for %s\n", eventSourceName)

	// TODO(slinkydeveloper) Give it 5 secs to the kafka source to reconcile the claims status.
	//  Since claims status is not part of readiness, it could cause a race on writing
	time.Sleep(5 * time.Second)
	var (
		ksObj               *sourcesv1beta1.KafkaSource
		newSinkEventTracker *recordevents.EventInfoStore
	)
	ksObj = contribtestlib.GetKafkaSourceV1Beta1OrFail(client, kafkaSourceName)
	if ksObj == nil {
		t.Fatalf("Unabled to Get kafkasource: %s/%s\n", client.Namespace, kafkaSourceName)
	}
	if test.topicName != defaultKafkaSource.topicName {
		helpers.MustCreateTopic(client, kafkaClusterName, kafkaClusterNamespace, test.topicName+name, 10)
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
	// TODO(slinkydeveloper) Give it 5 secs to the kafka source to reconcile again
	time.Sleep(5 * time.Second)
	client.WaitForAllTestResourcesReadyOrFail(context.Background())

	// See https://github.com/knative-sandbox/eventing-kafka/issues/411
	if testMtSource == "1" {
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
