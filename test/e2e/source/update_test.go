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
	"time"

	. "github.com/cloudevents/sdk-go/v2/test"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"knative.dev/eventing/test/lib/recordevents"
	rrecordevents "knative.dev/eventing/test/lib/resources/recordevents"

	"knative.dev/eventing-kafka/test/e2e/helpers"
	kafkasource "knative.dev/eventing-kafka/test/lib/resources/kafkasource/v1beta1"
	kafkatopic "knative.dev/eventing-kafka/test/lib/resources/kafkatopic/v1beta2"
)

var _ = Describe("KafkaSource updates", func() {
	var topicName string
	var eventStore *recordevents.EventInfoStore
	var sourceName string

	BeforeEach(func() {
		By("creating a Kafka topic")
		topicName = kafkatopic.Install(client, kafkaClusterName, kafkaClusterNamespace)

		By("creating a record events service")
		eventStore = rrecordevents.Install(context.Background(), client)

		By("creating a default Kafka source")
		sourceName = kafkasource.Install(client, kafkaBootstrapUrlPlain, topicName, eventStore.AsSinkRef())
	})

	Context("when the sink is updated", func() {
		var newEventStore *recordevents.EventInfoStore

		BeforeEach(func() {
			By("creating a new record events service")
			newEventStore = rrecordevents.Install(context.Background(), client)

			By("updating the KafkaSource sink")
			source := kafkasource.Get(client, sourceName)
			source.Spec.Sink.Ref = newEventStore.AsSinkRef()
			kafkasource.Update(client, source)

			Eventually(kafkasource.HasGenerationBeenObserved(client, source)).Should(BeTrue())
			Eventually(kafkasource.IsReady(client, source)).Should(BeTrue())

			time.Sleep(10 * time.Second)

			By("sending an event to the Kafka topic")
			helpers.MustPublishKafkaMessage(client, kafkaBootstrapUrlPlain, topicName, "", nil, `{"msg":"hello"}`)
		})

		It("the new sink should receive newly produced events", func() {
			newEventStore.AssertExact(1, recordevents.MatchEvent(HasData([]byte(`{"msg":"hello"}`))))
		})

		It("the old sink should not receive newly produced events", func() {
			// Wait for the event to be delivered
			newEventStore.AssertExact(1, recordevents.MatchEvent(HasData([]byte(`{"msg":"hello"}`))))

			// then assert
			eventStore.AssertNot(recordevents.MatchEvent(HasData([]byte(`{"msg":"hello"}`))))
		})
	})

	Context("when the topic is updated", func() {
		var newTopicName string

		BeforeEach(func() {
			By("creating a new Kafka topic")
			newTopicName = kafkatopic.Install(client, kafkaClusterName, kafkaClusterNamespace)

			By("updating the KafkaSource topic")
			source := kafkasource.Get(client, sourceName)
			source.Spec.Topics = []string{newTopicName}
			kafkasource.Update(client, source)

			Eventually(kafkasource.HasGenerationBeenObserved(client, source)).Should(BeTrue())
			Eventually(kafkasource.IsReady(client, source)).Should(BeTrue())

			time.Sleep(10 * time.Second)
		})

		PIt("should receive produced events when sent to the new topic", func() {
			By("sending an event to the new Kafka topic")
			helpers.MustPublishKafkaMessage(client, kafkaBootstrapUrlPlain, newTopicName, "", nil, `{"msg":"hello"}`)

			eventStore.AssertExact(1, recordevents.MatchEvent(HasData([]byte(`{"msg":"hello"}`))))
		})

		PIt("should not receive produced events when sent to the old topic", func() {
			By("sending an event to the new Kafka topic")
			helpers.MustPublishKafkaMessage(client, kafkaBootstrapUrlPlain, topicName, "", nil, `{"msg":"hello"}`)

			time.Sleep(10 * time.Second)

			eventStore.AssertNot(recordevents.MatchEvent(HasData([]byte(`{"msg":"hello"}`))))
		})
	})

	Context("when the bootstrap server and TLS is updated", func() {
		BeforeEach(func() {

			By("updating the KafkaSource bootstrap servers and TLS secret")
			source := kafkasource.Get(client, sourceName)
			source.Spec.BootstrapServers = []string{kafkaBootstrapUrlTLS}
			kafkasource.WithTLSEnabled(source, kafkaTLSSecret)
			kafkasource.Update(client, source)

			Eventually(kafkasource.HasGenerationBeenObserved(client, source)).Should(BeTrue())
			Eventually(kafkasource.IsReady(client, source)).Should(BeTrue())

			By("sending an event to the Kafka topic")
			helpers.MustPublishKafkaMessage(client, kafkaBootstrapUrlPlain, topicName, "", nil, `{"msg":"hello"}`)
		})

		It("should receive newly produced events", func() {
			eventStore.AssertExact(1, recordevents.MatchEvent(HasData([]byte(`{"msg":"hello"}`))))
		})

	})

})
