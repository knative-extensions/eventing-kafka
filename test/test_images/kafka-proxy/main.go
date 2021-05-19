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

package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/Shopify/sarama"
	protocolkafka "github.com/cloudevents/sdk-go/protocol/kafka_sarama/v2"
	cloudeventhttp "github.com/cloudevents/sdk-go/v2/protocol/http"
	"github.com/kelseyhightower/envconfig"
)

type envConfig struct {
	// KafkaServer URL to connect to the Kafka server.
	KafkaServer string `envconfig:"KAFKA_BOOTSTRAP_SERVER" required:"true"`

	// KafkaTopic where to send events to
	Topic string `envconfig:"KAFKA_TOPIC" required:"true"`
}

type forwarder struct {
	producer sarama.SyncProducer
	topic    string
}

func (f forwarder) forwardEvent(response http.ResponseWriter, request *http.Request) {
	message := cloudeventhttp.NewMessageFromHttpRequest(request)

	kafkaProducerMessage := sarama.ProducerMessage{
		Topic: f.topic,
	}

	ctx := protocolkafka.WithSkipKeyMapping(context.Background())

	err := protocolkafka.WriteProducerMessage(ctx, message, &kafkaProducerMessage)
	if err != nil {
		response.WriteHeader(http.StatusBadRequest)
		response.Write([]byte(err.Error()))
		return
	}

	_, _, err = f.producer.SendMessage(&kafkaProducerMessage)
	if err != nil {
		response.WriteHeader(http.StatusBadGateway)
		response.Write([]byte(err.Error()))
		return
	}

	response.WriteHeader(http.StatusOK)
}

func main() {
	var env envConfig
	if err := envconfig.Process("", &env); err != nil {
		log.Printf("[ERROR] Failed to process environment variables: %v", err)
		os.Exit(1)
	}

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer([]string{env.KafkaServer}, config)
	if err != nil {
		log.Printf("[ERROR] Failed to create Kafka producer: %v", err)
		os.Exit(1)
	}

	proxy := forwarder{
		producer: producer,
		topic:    env.Topic,
	}

	http.HandleFunc("/", proxy.forwardEvent)

	fmt.Println("Starting receiver HTTP requests port 8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatalf("error in ListenAndServe: %v", err)
	}
}
