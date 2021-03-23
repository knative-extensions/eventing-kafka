//+build e2e
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
	"fmt"
	"testing"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	. "github.com/cloudevents/sdk-go/v2/test"
	"github.com/google/uuid"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	channelsv1beta1 "knative.dev/eventing-kafka/pkg/apis/messaging/v1beta1"
	contribtestlib "knative.dev/eventing-kafka/test/lib"
	testlib "knative.dev/eventing/test/lib"
	"knative.dev/eventing/test/lib/recordevents"
	"knative.dev/eventing/test/lib/resources"
)

const (
	kafkaBootstrapUrlPlain = "my-cluster-kafka-bootstrap.kafka.svc:9092"
	kafkaClusterName       = "my-cluster"
	kafkaClusterNamespace  = "kafka"
	kafkaChannelName       = "kafka-sub-ready-channel"
	kafkaSub0              = "kafka-sub-0"
	kafkaSub1              = "kafka-sub-1"
	recordEventsPodName    = "e2e-channel-sub-ready-recordevents-pod"
	eventSenderName        = "e2e-channel-event-sender-pod"
)

func scaleDispatcherDeployment(t *testing.T, desiredReplicas int32, client *testlib.Client) {
	dispatcherDeployment, err := client.Kube.AppsV1().Deployments("knative-eventing").Get(context.Background(), "kafka-ch-dispatcher", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Unable to get kafka-ch-dispatcher deployment: %v", err)
	}
	if *dispatcherDeployment.Spec.Replicas != desiredReplicas {
		desired := dispatcherDeployment.DeepCopy()
		*desired.Spec.Replicas = desiredReplicas
		_, err := client.Kube.AppsV1().Deployments("knative-eventing").Update(context.Background(), desired, metav1.UpdateOptions{})
		if err != nil {
			t.Fatalf("Unable to update kafka-ch-dispatcher deployment to %d replica: %v", desiredReplicas, err)
		}
		// we actively do NOT wait for deployments to be ready so we can check state below
	}
}

func readyDispatcherPodsCheck(t *testing.T, client *testlib.Client) int32 {
	dispatcherDeployment, err := client.Kube.AppsV1().Deployments("knative-eventing").Get(context.Background(), "kafka-ch-dispatcher", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Unable to get kafka-ch-dispatcher deployment: %v", err)
	}
	return dispatcherDeployment.Status.ReadyReplicas
}

func TestChannelSubscriptionScaleReady(t *testing.T) {

	kafkaChannelMeta := metav1.TypeMeta{
		Kind:       "KafkaChannel",
		APIVersion: "messaging.knative.dev/v1beta1",
	}
	eventSource := fmt.Sprintf("http://%s.svc/", eventSenderName)

	client := testlib.Setup(t, true)
	defer testlib.TearDown(client)

	scaleDispatcherDeployment(t, 1, client)
	kafkaChannelV1Beta1 := &channelsv1beta1.KafkaChannel{
		ObjectMeta: metav1.ObjectMeta{
			Name: kafkaChannelName,
		},
		Spec: channelsv1beta1.KafkaChannelSpec{
			NumPartitions: 3,
		},
	}
	contribtestlib.CreateKafkaChannelV1Beta1OrFail(client, kafkaChannelV1Beta1)
	client.WaitForResourceReadyOrFail(kafkaChannelName, &kafkaChannelMeta)

	eventTracker, _ := recordevents.StartEventRecordOrFail(context.Background(), client, recordEventsPodName)
	client.CreateSubscriptionOrFail(
		kafkaSub0,
		kafkaChannelName,
		&kafkaChannelMeta,
		resources.WithSubscriberForSubscription(recordEventsPodName),
	)
	client.WaitForAllTestResourcesReadyOrFail(context.Background())

	scaleDispatcherDeployment(t, 4, client)
	client.WaitForResourceReadyOrFail(kafkaSub0, testlib.SubscriptionTypeMeta) //this should still be ready

	client.CreateSubscriptionOrFail(
		kafkaSub1,
		kafkaChannelName,
		&kafkaChannelMeta,
		resources.WithSubscriberForSubscription(recordEventsPodName),
	)

	for readyDispatcherPodsCheck(t, client) < 3 {
		subObj, err := client.Eventing.MessagingV1().Subscriptions(client.Namespace).Get(context.Background(), kafkaSub1, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("Could not get subscription object %q: %v", subObj.Name, err)
		}
		if subObj.Status.IsReady() {
			t.Fatalf("Subscription: %s, marked ready before dispatcher pods ready", subObj.Name)
		}
	}
	client.WaitForResourceReadyOrFail(kafkaSub1, testlib.SubscriptionTypeMeta)
	// send CloudEvent to the first channel
	event := cloudevents.NewEvent()
	event.SetID("test")
	event.SetSource(eventSource)
	event.SetType(testlib.DefaultEventType)

	body := fmt.Sprintf(`{"msg":"TestSingleEvent %s"}`, uuid.New().String())
	if err := event.SetData(cloudevents.ApplicationJSON, []byte(body)); err != nil {
		t.Fatalf("Cannot set the payload of the event: %s", err.Error())
	}

	client.SendEventToAddressable(context.Background(), eventSenderName, kafkaChannelName, &kafkaChannelMeta, event)
	// verify the logger service receives the event
	eventTracker.AssertAtLeast(1, recordevents.MatchEvent(
		HasSource(eventSource),
		HasData([]byte(body)),
	))
}
