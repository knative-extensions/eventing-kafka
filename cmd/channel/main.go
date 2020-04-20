package main

import (
	"context"
	"flag"
	"github.com/cloudevents/sdk-go/v1/cloudevents"
	"github.com/kyma-incubator/knative-kafka/pkg/channel/channel"
	"github.com/kyma-incubator/knative-kafka/pkg/channel/constants"
	"github.com/kyma-incubator/knative-kafka/pkg/channel/env"
	channelhealth "github.com/kyma-incubator/knative-kafka/pkg/channel/health"
	"github.com/kyma-incubator/knative-kafka/pkg/channel/producer"
	commonk8s "github.com/kyma-incubator/knative-kafka/pkg/common/k8s"
	kafkautil "github.com/kyma-incubator/knative-kafka/pkg/common/kafka/util"
	"github.com/kyma-incubator/knative-kafka/pkg/common/prometheus"
	"go.uber.org/zap"
	eventingChannel "knative.dev/eventing/pkg/channel"
	"knative.dev/eventing/pkg/kncloudevents"
	"knative.dev/pkg/logging"
)

// Variables
var (
	logger     *zap.Logger
	masterURL  = flag.String("masterurl", "", "The address of the Kubernetes API server. Overrides any value in kubeconfig. Only required if out-of-cluster.")
	kubeconfig = flag.String("kubeconfig", "", "Path to a kubeconfig. Only required if out-of-cluster.")
)

// The Main Function (Go Command)
func main() {

	// Parse The Flags For Local Development Usage
	flag.Parse()

	// Initialize A Knative Injection Lite Context (K8S Client & Logger)
	ctx := commonk8s.LoggingContext(context.Background(), constants.Component, *masterURL, *kubeconfig)

	// Get The Logger From The Context & Defer Flushing Any Buffered Log Entries On Exit
	logger = logging.FromContext(ctx).Desugar()
	defer func() { _ = logger.Sync() }()

	// Load Environment Variables
	environment, err := env.GetEnvironment(logger)
	if err != nil {
		logger.Fatal("Invalid / Missing Environment Variables - Terminating")
		return // Quiet The Compiler ; )
	}

	// Start The Liveness And Readiness Servers
	healthServer := channelhealth.NewChannelHealthServer(environment.HealthPort)
	healthServer.Start(logger)

	// Start The Prometheus Metrics Server (Prometheus)
	metricsServer := prometheus.NewMetricsServer(logger, environment.MetricsPort, "/metrics")
	metricsServer.Start()

	// Initialize The KafkaChannel Lister Used To Validate Events
	err = channel.InitializeKafkaChannelLister(ctx, *masterURL, *kubeconfig, healthServer)
	if err != nil {
		logger.Fatal("Failed To Initialize KafkaChannel Lister", zap.Error(err))
	}

	// Initialize The Kafka Producer In Order To Start Processing Status Events
	err = producer.InitializeProducer(logger, environment.KafkaBrokers, environment.KafkaUsername, environment.KafkaPassword, metricsServer, healthServer)
	if err != nil {
		logger.Fatal("Failed To Initialize Kafka Producer", zap.Error(err))
	}

	// The EventReceiver is responsible for processing the context (headers and binary/json content) of each request,
	// and then passing the context, channel details, and the constructed CloudEvent event to our handleEvent() function.
	eventReceiver, err := eventingChannel.NewEventReceiver(handleEvent, logger)
	if err != nil {
		logger.Fatal("Failed To Create Knative EventReceiver", zap.Error(err))
	}

	// The Knative CloudEvent Client handles the mux http server setup (middlewares and transport options) and invokes
	// the eventReceiver. Although the NewEventReceiver method above will also invoke kncloudevents.NewDefaultClient
	// internally, that client goes unused when using the ServeHTTP on the eventReceiver.
	//
	// IMPORTANT: Because the kncloudevents package does not allow injecting modified configuration,
	//            we can't override the default port being used (8080).
	knCloudEventClient, err := kncloudevents.NewDefaultClient()
	if err != nil {
		logger.Fatal("Failed To Create Knative CloudEvent Client", zap.Error(err))
	}

	// Set The Liveness Flag - Readiness Is Set By Individual Components
	healthServer.SetAlive(true)

	// Start Receiving Events (Blocking Call :)
	err = knCloudEventClient.StartReceiver(ctx, eventReceiver.ServeHTTP)
	if err != nil {
		logger.Error("Failed To Start Event Receiver", zap.Error(err))
	}

	// Reset The Liveness and Readiness Flags In Preparation For Shutdown
	healthServer.Shutdown()

	// Close The K8S KafkaChannel Lister & The Kafka Producer
	channel.Close()
	producer.Close()

	// Shutdown The Prometheus Metrics Server
	metricsServer.Stop()

	// Stop The Liveness And Readiness Servers
	healthServer.Stop(logger)
}

// Handler For Receiving Cloud Events And Sending The Event To Kafka
func handleEvent(_ context.Context, channelReference eventingChannel.ChannelReference, cloudEvent cloudevents.Event) error {

	// Note - The context provided here is a different context from the one created in main() and does not have our logger instance.

	logger.Debug("~~~~~~~~~~~~~~~~~~~~  Processing Request  ~~~~~~~~~~~~~~~~~~~~")
	logger.Debug("Received Cloud Event", zap.Any("CloudEvent", cloudEvent), zap.Any("ChannelReference", channelReference))

	// Trim The "-kafkachannel" Suffix From The Service Name (Added Only To Workaround Kyma Naming Conflict)
	channelReference.Name = kafkautil.TrimKafkaChannelServiceNameSuffix(channelReference.Name)

	// Validate The KafkaChannel Prior To Producing Kafka Message
	err := channel.ValidateKafkaChannel(channelReference)
	if err != nil {
		logger.Warn("Unable To Validate ChannelReference", zap.Any("ChannelReference", channelReference), zap.Error(err))
		return err
	}

	// Send The Event To The Appropriate Channel/Topic
	err = producer.ProduceKafkaMessage(cloudEvent, channelReference)
	if err != nil {
		logger.Error("Failed To Produce Kafka Message", zap.Error(err))
		return err
	}

	// Return Success
	return nil
}
