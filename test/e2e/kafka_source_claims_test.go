//+build e2e !mtsource
//+build source

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

package e2e

import (
	"context"
	"testing"

	"github.com/cloudevents/sdk-go/v2/test"
	"github.com/stretchr/testify/require"
	testlib "knative.dev/eventing/test/lib"
	"knative.dev/eventing/test/lib/recordevents"
	"knative.dev/eventing/test/lib/resources"

	"knative.dev/eventing-kafka/test/e2e/helpers"
	contribtestlib "knative.dev/eventing-kafka/test/lib"
	contribresources "knative.dev/eventing-kafka/test/lib/resources"
)

func TestKafkaSourceClaims(t *testing.T) {
	topic := defaultKafkaSource.topicName + "-test-claims"
	sink := defaultKafkaSource.sinkName

	messageHeaders := map[string]string{
		"content-type": "application/cloudevents+json",
	}
	messagePayload := mustJsonMarshal(t, map[string]interface{}{
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
	})

	client := testlib.Setup(t, true)
	defer testlib.TearDown(client)

	t.Logf("Creating topic: %s\n", topic)
	helpers.MustCreateTopic(client, kafkaClusterName, kafkaClusterNamespace, topic, 10)

	t.Logf("Creating default eventrecorder pod: %s\n", sink)
	eventTracker, _ := recordevents.StartEventRecordOrFail(context.Background(), client, sink)
	client.WaitForAllTestResourcesReadyOrFail(context.Background())

	kafkaSourceName := "e2e-kafka-source-test-claims"

	t.Logf("Creating kafkasource: %s\n", kafkaSourceName)
	contribtestlib.CreateKafkaSourceV1Beta1OrFail(client, contribresources.KafkaSourceV1Beta1(
		kafkaBootstrapUrlPlain,
		topic,
		resources.ServiceRef(sink),
		contribresources.WithNameV1Beta1(kafkaSourceName),
	))
	client.WaitForAllTestResourcesReadyOrFail(context.Background())

	t.Logf("Send update event to kafkatopic")
	helpers.MustPublishKafkaMessage(
		client,
		kafkaBootstrapUrlPlain,
		topic,
		"0",
		messageHeaders,
		messagePayload,
	)

	eventTracker.AssertExact(1, recordevents.MatchEvent(test.HasType("com.github.pull.create")))

	ksObj := contribtestlib.GetKafkaSourceV1Beta1OrFail(client, kafkaSourceName)
	if ksObj == nil {
		t.Fatalf("Unabled to Get kafkasource: %s/%s\n", client.Namespace, kafkaSourceName)
	}

	require.NotEmpty(t, ksObj.Status.Claims)
	t.Logf("Claims value: %s", ksObj.Status.Claims)
}
