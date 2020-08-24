package main

import (
	"context"
	"flag"
	nethttp "net/http"
	"strconv"
	"strings"

	"github.com/cloudevents/sdk-go/v2/binding"
	"go.uber.org/zap"
	v1 "k8s.io/api/core/v1"
	"knative.dev/eventing-kafka/pkg/channel/channel"
	"knative.dev/eventing-kafka/pkg/channel/constants"
	"knative.dev/eventing-kafka/pkg/channel/env"
	channelhealth "knative.dev/eventing-kafka/pkg/channel/health"
	"knative.dev/eventing-kafka/pkg/channel/producer"
	commonconfig "knative.dev/eventing-kafka/pkg/common/config"
	commonk8s "knative.dev/eventing-kafka/pkg/common/k8s"
	"knative.dev/eventing-kafka/pkg/common/kafka/sarama"
	kafkautil "knative.dev/eventing-kafka/pkg/common/kafka/util"
	"knative.dev/eventing-kafka/pkg/common/metrics"
	eventingchannel "knative.dev/eventing/pkg/channel"
	"knative.dev/pkg/logging"
	eventingmetrics "knative.dev/pkg/metrics"
)

// Constants
const (
	Component = "KafkaChannel"
)

// Variables
var (
	logger        *zap.Logger
	masterURL     = flag.String("masterurl", "", "The address of the Kubernetes API server. Overrides any value in kubeconfig. Only required if out-of-cluster.")
	kubeconfig    = flag.String("kubeconfig", "", "Path to a kubeconfig. Only required if out-of-cluster.")
	kafkaProducer *producer.Producer
)

// The Main Function (Go Command)
func main() {

	// Parse The Flags For Local Development Usage
	flag.Parse()

	// Initialize A Knative Injection Lite Context (K8S Client & Logger)
	ctx := commonk8s.LoggingContext(context.Background(), constants.Component, *masterURL, *kubeconfig)

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

	// Load the Sarama SyncProducer (and Eventing-Kafka settings, though the channel doesn't use those at the moment)
	// from our configmap
	saramaConfig, _, err := sarama.LoadSettings(ctx)
	if err != nil {
		logger.Fatal("Failed To Load Sarama Settings", zap.Error(err))
	}

	// Initialize Tracing (Watches config-tracing ConfigMap, Assumes Context Came From LoggingContext With Embedded K8S Client Key)
	err = commonconfig.InitializeTracing(logger.Sugar(), ctx, environment.ServiceName)
	if err != nil {
		logger.Fatal("Could Not Initialize Tracing - Terminating", zap.Error(err))
	}

	// Initialize Observability (Watches config-observability ConfigMap And Starts Profiling Server)
	err = commonconfig.InitializeObservability(logger.Sugar(), ctx, environment.MetricsDomain, environment.MetricsPort)
	if err != nil {
		logger.Fatal("Could Not Initialize Observability - Terminating", zap.Error(err))
	}

	// Start The Liveness And Readiness Servers
	healthServer := channelhealth.NewChannelHealthServer(strconv.Itoa(environment.HealthPort))
	healthServer.Start(logger)

	// Initialize The KafkaChannel Lister Used To Validate Events
	err = channel.InitializeKafkaChannelLister(ctx, *masterURL, *kubeconfig, healthServer)
	if err != nil {
		logger.Fatal("Failed To Initialize KafkaChannel Lister", zap.Error(err))
	}
	defer channel.Close()

	// Create A New Stats StatsReporter
	statsReporter := metrics.NewStatsReporter(logger)

	// Watch The Settings ConfigMap For Changes
	err = commonconfig.InitializeConfigWatcher(logger.Sugar(), ctx, configMapObserver)
	if err != nil {
		logger.Fatal("Failed To Initialize ConfigMap Watcher", zap.Error(err))
	}

	// Create The Sarama SyncProducer Config
	// Add username/password/components overrides to the Sarama config (these take precedence over what's in the configmap)
	sarama.UpdateSaramaConfig(saramaConfig, Component, environment.KafkaUsername, environment.KafkaPassword)

	// Initialize The Kafka Producer In Order To Start Processing Status Events
	kafkaProducer, err = producer.NewProducer(logger, saramaConfig, strings.Split(environment.KafkaBrokers, ","), statsReporter, healthServer)
	if err != nil {
		logger.Fatal("Failed To Initialize Kafka Producer", zap.Error(err))
	}
	defer kafkaProducer.Close()

	// Create A New Knative Eventing MessageReceiver (Parses The Channel From The Host Header)
	messageReceiver, err := eventingchannel.NewMessageReceiver(handleMessage, logger, eventingchannel.ResolveMessageChannelFromHostHeader(eventingchannel.ParseChannel))
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
	logger.Debug("~~~~~~~~~~~~~~~~~~~~  Processing Request  ~~~~~~~~~~~~~~~~~~~~")
	logger.Debug("Received Message", zap.Any("ChannelReference", channelReference))

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
