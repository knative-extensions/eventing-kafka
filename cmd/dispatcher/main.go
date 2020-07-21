package main

import (
	"flag"
	"go.uber.org/zap"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"knative.dev/eventing-contrib/kafka/channel/pkg/client/clientset/versioned"
	"knative.dev/eventing-contrib/kafka/channel/pkg/client/informers/externalversions"
	commonk8s "knative.dev/eventing-kafka/pkg/common/k8s"
	"knative.dev/eventing-kafka/pkg/common/metrics"
	"knative.dev/eventing-kafka/pkg/dispatcher/controller"
	dispatch "knative.dev/eventing-kafka/pkg/dispatcher/dispatcher"
	"knative.dev/eventing-kafka/pkg/dispatcher/env"
	dispatcherhealth "knative.dev/eventing-kafka/pkg/dispatcher/health"
	kncontroller "knative.dev/pkg/controller"
	"knative.dev/pkg/logging"
	eventingmetrics "knative.dev/pkg/metrics"
	"knative.dev/pkg/signals"
	"os"
	"strconv"
	"time"
)

// Constants
const (
	Component                                      = "KafkaDispatcher"
	DefaultKafkaConsumerOffset                     = "latest"
	DefaultKafkaConsumerPollTimeoutMillis          = 500 // Timeout Millis When Polling For Events
	MinimumKafkaConsumerOffsetCommitDurationMillis = 250 // Azure EventHubs Restrict To 250ms Between Offset Commits
)

// Variables
var (
	logger     *zap.Logger
	dispatcher *dispatch.Dispatcher
	masterURL  = flag.String("master", "", "The address of the Kubernetes API server. Overrides any value in kubeconfig. Only required if out-of-cluster.")
	kubeconfig = flag.String("kubeconfig", "", "Path to a kubeconfig. Only required if out-of-cluster.")
)

// The Main Function (Go Command)
func main() {

	// Parse The Flags For Local Development Usage
	flag.Parse()

	// Initialize A Knative Injection Lite Context (K8S Client & Logger)
	ctx := commonk8s.LoggingContext(signals.NewContext(), Component, *masterURL, *kubeconfig)

	// Get The Logger From The Context & Defer Flushing Any Buffered Log Entries On Exit
	sugaredLogger := logging.FromContext(ctx)
	defer flush(sugaredLogger)
	logger = sugaredLogger.Desugar()

	// Load Environment Variables
	environment, err := env.GetEnvironment(logger)
	if err != nil {
		logger.Panic("Failed To Load Environment Variables - Terminating!", zap.Error(err))
		os.Exit(1)
	}

	// Initialize Tracing (Watches config-tracing ConfigMap, Assumes Context Came From LoggingContext With Embedded K8S Client Key)
	commonk8s.InitializeTracing(sugaredLogger, ctx, environment.ServiceName)

	// Initialize Observability (Watches config-observability ConfigMap And Starts Profiling Server)
	commonk8s.InitializeObservability(sugaredLogger, ctx, environment.MetricsDomain, environment.MetricsPort)

	// Start The Liveness And Readiness Servers
	healthServer := dispatcherhealth.NewDispatcherHealthServer(strconv.Itoa(environment.HealthPort))
	healthServer.Start(logger)

	reporter := metrics.NewStatsReporter(logger)

	// Create The Dispatcher With Specified Configuration
	dispatcherConfig := dispatch.DispatcherConfig{
		Logger:                      logger,
		Brokers:                     environment.KafkaBrokers,
		Topic:                       environment.KafkaTopic,
		Offset:                      DefaultKafkaConsumerOffset,
		PollTimeoutMillis:           DefaultKafkaConsumerPollTimeoutMillis,
		OffsetCommitCount:           environment.KafkaOffsetCommitMessageCount,
		OffsetCommitDuration:        time.Duration(environment.KafkaOffsetCommitDurationMillis) * time.Millisecond,
		OffsetCommitDurationMinimum: MinimumKafkaConsumerOffsetCommitDurationMillis * time.Millisecond,
		Username:                    environment.KafkaUsername,
		Password:                    environment.KafkaPassword,
		ChannelKey:                  environment.ChannelKey,
		Reporter:                    reporter,
		ExponentialBackoff:          environment.ExponentialBackoff,
		InitialRetryInterval:        environment.InitialRetryInterval,
		MaxRetryTime:                environment.MaxRetryTime,
	}
	dispatcher = dispatch.NewDispatcher(dispatcherConfig)

	config, err := clientcmd.BuildConfigFromFlags(*masterURL, *kubeconfig)
	if err != nil {
		logger.Fatal("Error building kubeconfig", zap.Error(err))
	}

	const numControllers = 1
	config.QPS = numControllers * rest.DefaultQPS
	config.Burst = numControllers * rest.DefaultBurst
	kafkaClientSet := versioned.NewForConfigOrDie(config)
	kubeClient := kubernetes.NewForConfigOrDie(config)
	kafkaInformerFactory := externalversions.NewSharedInformerFactory(kafkaClientSet, kncontroller.DefaultResyncPeriod)

	// Create KafkaChannel Informer
	kafkaChannelInformer := kafkaInformerFactory.Messaging().V1alpha1().KafkaChannels()

	// Construct Array Of Controllers, In Our Case Just The One
	controllers := [...]*kncontroller.Impl{
		controller.NewController(
			logger,
			dispatcher,
			kafkaChannelInformer,
			kubeClient,
			kafkaClientSet,
			ctx.Done(),
		),
	}

	// Start The Informers
	logger.Info("Starting informers.")
	if err := kncontroller.StartInformers(ctx.Done(), kafkaChannelInformer.Informer()); err != nil {
		logger.Error("Failed to start informers", zap.Error(err))
		return
	}

	// Set The Liveness And Readiness Flags
	logger.Info("Registering dispatcher as alive and ready")
	healthServer.SetAlive(true)
	healthServer.SetDispatcherReady(true)

	// Start The Controllers (Blocking WaitGroup.Wait Call)
	logger.Info("Starting controllers.")
	kncontroller.StartAll(ctx, controllers[:]...)

	// Reset The Liveness and Readiness Flags In Preparation For Shutdown
	healthServer.Shutdown()

	// Close Consumer Connections
	dispatcher.StopConsumers()

	// Stop The Liveness And Readiness Servers
	healthServer.Stop(logger)
}

func flush(logger *zap.SugaredLogger) {
	_ = logger.Sync()
	eventingmetrics.FlushExporter()
}
