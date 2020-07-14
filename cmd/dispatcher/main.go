package main

import (
	"flag"
	"go.uber.org/zap"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"knative.dev/eventing-contrib/kafka/channel/pkg/client/clientset/versioned"
	"knative.dev/eventing-contrib/kafka/channel/pkg/client/informers/externalversions"
	commonenv "knative.dev/eventing-kafka/pkg/common/env"
	commonk8s "knative.dev/eventing-kafka/pkg/common/k8s"
	"knative.dev/eventing-kafka/pkg/common/prometheus"
	"knative.dev/eventing-kafka/pkg/dispatcher/controller"
	dispatch "knative.dev/eventing-kafka/pkg/dispatcher/dispatcher"
	dispatcherhealth "knative.dev/eventing-kafka/pkg/dispatcher/health"
	kncontroller "knative.dev/pkg/controller"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/signals"
	"os"
	"strconv"
	"strings"
	"time"
)

// Constants
const (
	Component                                      = "KafkaDispatcher"
	DefaultKafkaConsumerPollTimeoutMillis          = 500 // Timeout Millis When Polling For Events
	MinimumKafkaConsumerOffsetCommitDurationMillis = 250 // Azure EventHubs Restrict To 250ms Between Offset Commits
)

// Variables
var (
	logger     *zap.Logger
	dispatcher dispatch.Dispatcher
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
	logger = logging.FromContext(ctx).Desugar()
	defer func() { _ = logger.Sync() }()

	// Load Environment Variables
	metricsPort := os.Getenv(commonenv.MetricsPortEnvVarKey)
	healthPort := os.Getenv(commonenv.HealthPortEnvVarKey)
	rawExpBackoff, expBackoffPresent := os.LookupEnv(commonenv.ExponentialBackoffEnvVarKey)
	exponentialBackoff, _ := strconv.ParseBool(rawExpBackoff)
	maxRetryTime, _ := strconv.ParseInt(os.Getenv(commonenv.MaxRetryTimeEnvVarKey), 10, 64)
	initialRetryInterval, _ := strconv.ParseInt(os.Getenv(commonenv.InitialRetryIntervalEnvVarKey), 10, 64)
	kafkaBrokers := os.Getenv(commonenv.KafkaBrokerEnvVarKey)
	kafkaTopic := os.Getenv(commonenv.KafkaTopicEnvVarKey)
	channelKey := os.Getenv(commonenv.ChannelKeyEnvVarKey)
	serviceName := os.Getenv(commonenv.ServiceNameEnvVarKey)
	kafkaUsername := os.Getenv(commonenv.KafkaUsernameEnvVarKey)
	kafkaPassword := os.Getenv(commonenv.KafkaPasswordEnvVarKey)
	kafkaPasswordLog := ""
	kafkaOffsetCommitMessageCount, _ := strconv.ParseInt(os.Getenv(commonenv.KafkaOffsetCommitMessageCountEnvVarKey), 10, 64)
	kafkaOffsetCommitDurationMillis, _ := strconv.ParseInt(os.Getenv(commonenv.KafkaOffsetCommitDurationMillisEnvVarKey), 10, 64)

	if len(kafkaPassword) > 0 {
		kafkaPasswordLog = "*************"
	}

	// Log Environment Variables
	logger.Info("Environment Variables",
		zap.String(commonenv.HealthPortEnvVarKey, healthPort),
		zap.String(commonenv.MetricsPortEnvVarKey, metricsPort),
		zap.Bool(commonenv.ExponentialBackoffEnvVarKey, exponentialBackoff),
		zap.Int64(commonenv.InitialRetryIntervalEnvVarKey, initialRetryInterval),
		zap.Int64(commonenv.MaxRetryTimeEnvVarKey, maxRetryTime),
		zap.String(commonenv.KafkaBrokerEnvVarKey, kafkaBrokers),
		zap.String(commonenv.KafkaTopicEnvVarKey, kafkaTopic),
		zap.Int64(commonenv.KafkaOffsetCommitMessageCountEnvVarKey, kafkaOffsetCommitMessageCount),
		zap.Int64(commonenv.KafkaOffsetCommitDurationMillisEnvVarKey, kafkaOffsetCommitDurationMillis),
		zap.String(commonenv.KafkaUsernameEnvVarKey, kafkaUsername),
		zap.String(commonenv.KafkaPasswordEnvVarKey, kafkaPasswordLog),
		zap.String(commonenv.ChannelKeyEnvVarKey, channelKey),
		zap.String(commonenv.ServiceNameEnvVarKey, serviceName))

	// Validate Required Environment Variables
	if len(metricsPort) == 0 ||
		len(healthPort) == 0 ||
		maxRetryTime <= 0 ||
		initialRetryInterval <= 0 ||
		!expBackoffPresent ||
		len(kafkaBrokers) == 0 ||
		len(kafkaTopic) == 0 ||
		len(channelKey) == 0 ||
		len(serviceName) == 0 ||
		kafkaOffsetCommitMessageCount <= 0 ||
		kafkaOffsetCommitDurationMillis <= 0 {
		logger.Fatal("Invalid / Missing Environment Variables - Terminating")
	}

	// Initialize Tracing (Watching config-tracing ConfigMap, Assumes Context Came From LoggingContext With Embedded K8S Client Key)
	commonk8s.InitializeTracing(logger.Sugar(), ctx, serviceName)

	// Start The Liveness And Readiness Servers
	healthServer := dispatcherhealth.NewDispatcherHealthServer(healthPort)
	healthServer.Start(logger)

	// Start The Prometheus Metrics Server (Prometheus)
	metricsServer := prometheus.NewMetricsServer(logger, metricsPort, "/metrics")
	metricsServer.Start()

	// Create The Dispatcher With Specified Configuration
	dispatcherConfig := dispatch.DispatcherConfig{
		Logger:                      logger,
		ClientId:                    Component,
		Brokers:                     strings.Split(kafkaBrokers, ","),
		Topic:                       kafkaTopic,
		PollTimeoutMillis:           DefaultKafkaConsumerPollTimeoutMillis,
		OffsetCommitCount:           kafkaOffsetCommitMessageCount,
		OffsetCommitDuration:        time.Duration(kafkaOffsetCommitDurationMillis) * time.Millisecond,
		OffsetCommitDurationMinimum: MinimumKafkaConsumerOffsetCommitDurationMillis * time.Millisecond,
		Username:                    kafkaUsername,
		Password:                    kafkaPassword,
		ChannelKey:                  channelKey,
		MetricsServer:               metricsServer,
		ExponentialBackoff:          exponentialBackoff,
		InitialRetryInterval:        initialRetryInterval,
		MaxRetryTime:                maxRetryTime,
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
			channelKey,
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

	// Shutdown The Dispatcher (Close ConsumerGroups)
	dispatcher.Shutdown()

	// Shutdown The Prometheus Metrics Server
	metricsServer.Stop()

	// Stop The Liveness And Readiness Servers
	healthServer.Stop(logger)
}
