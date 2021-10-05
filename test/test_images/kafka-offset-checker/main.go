/*
Copyright 2020 The Knative Authors

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
	"log"
	"strings"

	"go.uber.org/zap"
	"k8s.io/apiserver/pkg/storage/names"
	"knative.dev/pkg/logging"

	"knative.dev/eventing-kafka/pkg/common/consumer"
	"knative.dev/eventing-kafka/pkg/common/kafka/offset"

	"github.com/Shopify/sarama"
	"github.com/kelseyhightower/envconfig"
	"knative.dev/pkg/signals"
)

type EnvConfig struct {
	Servers string
}

func main() {
	ctx := signals.NewContext()
	l, err := zap.NewDevelopment(
		zap.WithCaller(true),
	)
	if err != nil {
		log.Fatal("Failed to create zap logger", err)
	}
	ctx = logging.WithLogger(ctx, l.Sugar())

	// Read the configuration out of the environment.
	var s EnvConfig
	err = envconfig.Process("kafka", &s)
	if err != nil {
		log.Fatal(err.Error())
	}
	log.Printf("GOT: %v", s)

	addr := strings.Split(s.Servers, ",")

	cfg := sarama.NewConfig()
	c, err := sarama.NewClient(addr, cfg)
	if err != nil {
		log.Fatal("Failed to create kafka client", err)
	}

	admin, err := sarama.NewClusterAdminFromClient(c)
	if err != nil {
		log.Fatal("Failed to create kafka admin", err)
	}

	topics := []string{
		names.SimpleNameGenerator.GenerateName("tp"),
		names.SimpleNameGenerator.GenerateName("tp"),
	}
	group := names.SimpleNameGenerator.GenerateName("group")
	nPartitions := int32(100)

	for _, t := range topics {
		td := &sarama.TopicDetail{NumPartitions: nPartitions, ReplicationFactor: 3}
		err := admin.CreateTopic(t, td /* validateOnly */, false)
		if err != nil {
			log.Fatalf("Failed to create topic %s: %v\n", t, err)
		}
	}

	log.Println("topics", topics, "group", group)

	tp, err := offset.InitOffsets(ctx, c, admin, topics, group)
	if err != nil {
		log.Fatal("Failed to init offsets", err)
	}

	expectedInitializedTp := nPartitions * int32(len(topics))
	if tp != expectedInitializedTp {
		log.Fatalf("Failed to init the correct number of offsets, got %d expected %d", tp, expectedInitializedTp)
	}

	checker := consumer.KafkaConsumerGroupOffsetsChecker{}
	if err := checker.WaitForOffsetsInitialization(ctx, group, topics, logging.FromContext(ctx), addr, cfg); err != nil {
		log.Fatalf("Failed to wait for offsets initialization, topics %v group %s: %v", topics, group, err)
	}
}
