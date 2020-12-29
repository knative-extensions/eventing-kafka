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
	"context"
	"flag"
	nethttp "net/http"
	"strconv"
	"strings"

	"github.com/cloudevents/sdk-go/v2/binding"
	"github.com/google/uuid"
	"go.uber.org/zap"
	v1 "k8s.io/api/core/v1"
	commonconfig "knative.dev/eventing-kafka/pkg/channel/distributed/common/config"
	commonk8s "knative.dev/eventing-kafka/pkg/channel/distributed/common/k8s"
	"knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/sarama"
	kafkautil "knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/util"
	"knative.dev/eventing-kafka/pkg/channel/distributed/common/metrics"
	"knative.dev/eventing-kafka/pkg/channel/distributed/receiver/channel"
	"knative.dev/eventing-kafka/pkg/channel/distributed/receiver/constants"
	"knative.dev/eventing-kafka/pkg/channel/distributed/receiver/env"
	channelhealth "knative.dev/eventing-kafka/pkg/channel/distributed/receiver/health"
	"knative.dev/eventing-kafka/pkg/channel/distributed/receiver/producer"
	"knative.dev/eventing-kafka/pkg/common/client"
	eventingchannel "knative.dev/eventing/pkg/channel"
	"knative.dev/pkg/kmeta"
	"knative.dev/pkg/logging"
	eventingmetrics "knative.dev/pkg/metrics"
)

// Variables
var (
	logger        *zap.Logger
	serverURL     = flag.String("server", "", "The address of the Kubernetes API server. Overrides any value in kubeconfig. Only required if out-of-cluster.")
	kubeconfig    = flag.String("kubeconfig", "", "Path to a kubeconfig. Only required if out-of-cluster.")
	kafkaProducer *producer.Producer
)

// The Main Function (Go Command)
func main() {

	// Parse The Flags For Local Development Usage
	flag.Parse()

	// Initialize A Knative Injection Lite Context (K8S Client & Logger)
	ctx := commonk8s.LoggingContext(context.Background(), constants.Component, *serverURL, *kubeconfig)

	// Get The Logger From The Context & Defer Flushing Any Buffered Log Entries On Exit
	logger = logging.FromContext(ctx).Desugar()
	defer flush(logger)

	// UnComment To Enable Sarama Logging For Local Debug
	// sarama.EnableSaramaLogging()

	// Load Environment Variables
	environment, err := env.GetEnvironment(logger)
	if err != nil {
		logger.Fatal("Invalid / Missing Environment Variables - Terminating", zap.Error(err))
	}

	// Load The Sarama & Eventing-Kafka Configuration From The ConfigMap
	saramaConfig, ekConfig, err := sarama.LoadSettings(ctx)
	if err != nil {
		logger.Fatal("Failed To Load Sarama Settings", zap.Error(err))
	}

	// Update The Sarama Config - Username/Password Overrides (EnvVars From Secret Take Precedence Over ConfigMap)
	client.UpdateSaramaConfig(saramaConfig, constants.Component, environment.KafkaUsername, environment.KafkaPassword)

	// Enable Sarama Logging If Specified In ConfigMap
	sarama.EnableSaramaLogging(ekConfig.Kafka.EnableSaramaLogging)

	// Initialize Tracing (Watches config-tracing ConfigMap, Assumes Context Came From LoggingContext With Embedded K8S Client Key)
	err = commonconfig.InitializeTracing(logger.Sugar(), ctx, environment.ServiceName, environment.SystemNamespace)
	if err != nil {
		logger.Fatal("Could Not Initialize Tracing - Terminating", zap.Error(err))
	}

	// Initialize Observability (Watches config-observability ConfigMap And Starts Profiling Server)
	err = commonconfig.InitializeObservability(ctx, logger.Sugar(), environment.MetricsDomain, environment.MetricsPort, environment.SystemNamespace)
	if err != nil {
		logger.Fatal("Could Not Initialize Observability - Terminating", zap.Error(err))
	}

	// Start The Liveness And Readiness Servers
	healthServer := channelhealth.NewChannelHealthServer(strconv.Itoa(environment.HealthPort))
	err = healthServer.Start(logger)
	if err != nil {
		logger.Fatal("Failed To Initialize Health Server - Terminating", zap.Error(err))
	}
	// Initialize The KafkaChannel Lister Used To Validate Events
	err = channel.InitializeKafkaChannelLister(ctx, *serverURL, *kubeconfig, healthServer, environment.ResyncPeriod)
	if err != nil {
		logger.Fatal("Failed To Initialize KafkaChannel Lister", zap.Error(err))
	}
	defer channel.Close()

	// Create A New Stats StatsReporter
	statsReporter := metrics.NewStatsReporter(logger)

	// Watch The Settings ConfigMap For Changes
	err = commonconfig.InitializeConfigWatcher(ctx, logger.Sugar(), configMapObserver, environment.SystemNamespace)
	if err != nil {
		logger.Fatal("Failed To Initialize ConfigMap Watcher", zap.Error(err))
	}

	// Initialize The Kafka Producer In Order To Start Processing Status Events
	kafkaProducer, err = producer.NewProducer(logger, saramaConfig, strings.Split(environment.KafkaBrokers, ","), statsReporter, healthServer)
	if err != nil {
		logger.Fatal("Failed To Initialize Kafka Producer", zap.Error(err))
	}
	defer kafkaProducer.Close()

	channelReporter := eventingchannel.NewStatsReporter(environment.ContainerName, kmeta.ChildName(environment.PodName, uuid.New().String()))

	// Create A New Knative Eventing MessageReceiver (Parses The Channel From The Host Header)
	messageReceiver, err := eventingchannel.NewMessageReceiver(handleMessage, logger, channelReporter, eventingchannel.ResolveMessageChannelFromHostHeader(eventingchannel.ParseChannel))
	if err != nil {
		logger.Fatal("Failed To Create MessageReceiver", zap.Error(err))
	}

	// Set The Liveness Flag - Readiness Is Set By Individual Components
	healthServer.SetAlive(true)

	// Start The Message Receiver (Blocking)
	err = messageReceiver.Start(ctx)
	if err != nil {
		logger.Error("Failed To Start MessageReceiver", zap.Error(err))
	}

	// Reset The Liveness and Readiness Flags In Preparation For Shutdown
	healthServer.Shutdown()

	// Stop The Liveness And Readiness Servers
	healthServer.Stop(logger)
}

// Deferred Logger / Metrics Flush
func flush(logger *zap.Logger) {
	_ = logger.Sync()
	eventingmetrics.FlushExporter()
}

// CloudEvent Message Handler - Converts To KafkaMessage And Produces To Channel's Kafka Topic
func handleMessage(ctx context.Context, channelReference eventingchannel.ChannelReference, message binding.Message, transformers []binding.Transformer, _ nethttp.Header) error {

	// Note - The context provided here is a different context from the one created in main() and does not have our logger instance.
	if logger.Core().Enabled(zap.DebugLevel) {
		// Checked Logging Level First To Avoid Calling zap.Any In Production
		logger.Debug("~~~~~~~~~~~~~~~~~~~~  Processing Request  ~~~~~~~~~~~~~~~~~~~~")
		logger.Debug("Received Message", zap.Any("ChannelReference", channelReference))
	}

	// Trim The "-kn-channel" Suffix From The Service Name
	channelReference.Name = kafkautil.TrimKafkaChannelServiceNameSuffix(channelReference.Name)

	// Validate The KafkaChannel Prior To Producing Kafka Message
	err := channel.ValidateKafkaChannel(channelReference)
	if err != nil {
		logger.Warn("Unable To Validate ChannelReference", zap.Any("ChannelReference", channelReference), zap.Error(err))
		return err
	}

	// Produce The CloudEvent Binding Message (Send To The Appropriate Kafka Topic)
	err = kafkaProducer.ProduceKafkaMessage(ctx, channelReference, message, transformers...)
	if err != nil {
		logger.Error("Failed To Produce Kafka Message", zap.Error(err))
		return err
	}

	// Return Success
	return nil
}

// configMapObserver is the callback function that handles changes to our ConfigMap
func configMapObserver(configMap *v1.ConfigMap) {
	if configMap == nil {
		logger.Warn("Nil ConfigMap passed to configMapObserver; ignoring")
		return
	}
	if kafkaProducer == nil {
		// This typically happens during startup
		logger.Debug("Producer is nil during call to configMapObserver; ignoring changes")
		return
	}

	// Toss the new config map to the producer for inspection and action
	newProducer := kafkaProducer.ConfigChanged(configMap)
	if newProducer != nil {
		// The configuration change caused a new producer to be created, so switch to that one
		logger.Info("Producer Reconfigured; Switching To New Producer")
		kafkaProducer = newProducer
	}
}
