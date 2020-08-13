package main

import (
	"flag"
	"strconv"
	"strings"

	"go.uber.org/zap"
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"knative.dev/eventing-contrib/kafka/channel/pkg/client/clientset/versioned"
	"knative.dev/eventing-contrib/kafka/channel/pkg/client/informers/externalversions"
	commonconfig "knative.dev/eventing-kafka/pkg/common/config"
	commonk8s "knative.dev/eventing-kafka/pkg/common/k8s"
	"knative.dev/eventing-kafka/pkg/common/kafka/util"
	"knative.dev/eventing-kafka/pkg/common/metrics"
	"knative.dev/eventing-kafka/pkg/dispatcher/controller"
	dispatch "knative.dev/eventing-kafka/pkg/dispatcher/dispatcher"
	"knative.dev/eventing-kafka/pkg/dispatcher/env"
	dispatcherhealth "knative.dev/eventing-kafka/pkg/dispatcher/health"
	kncontroller "knative.dev/pkg/controller"
	"knative.dev/pkg/logging"
	eventingmetrics "knative.dev/pkg/metrics"
	"knative.dev/pkg/signals"
)

// Constants
const (
	Component = "KafkaDispatcher"
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
	defer flush(logger)

	// UnComment To Enable Sarama Logging For Local Debug
	// kafkautil.EnableSaramaLogging()

	// Load Environment Variables
	environment, err := env.GetEnvironment(logger)
	if err != nil {
		logger.Fatal("Failed To Load Environment Variables - Terminating!", zap.Error(err))
	}

	// Load the sarama and eventing-kafka settings from our settings configmap
	saramaConfig, ekConfig, err := commonconfig.LoadEventingKafkaSettings(ctx)
	if err != nil {
		logger.Fatal("Failed To Load Sarama Settings", zap.Error(err))
	}
	// Overwrite configmap settings with anything provided by the environment
	env.ApplyOverrides(ekConfig, environment)

	// Initialize Tracing (Watches config-tracing ConfigMap, Assumes Context Came From LoggingContext With Embedded K8S Client Key)
	err = commonconfig.InitializeTracing(logger.Sugar(), ctx, ekConfig.Kafka.ServiceName)
	if err != nil {
		logger.Fatal("Could Not Initialize Tracing - Terminating", zap.Error(err))
	}

	// Initialize Observability (Watches config-observability ConfigMap And Starts Profiling Server)
	err = commonconfig.InitializeObservability(logger.Sugar(), ctx, ekConfig.Metrics.Domain, ekConfig.Metrics.Port)
	if err != nil {
		logger.Fatal("Could Not Initialize Observability - Terminating", zap.Error(err))
	}

	// Start The Liveness And Readiness Servers
	healthServer := dispatcherhealth.NewDispatcherHealthServer(strconv.Itoa(ekConfig.Health.Port))
	healthServer.Start(logger)

	statsReporter := metrics.NewStatsReporter(logger)

	// Add username/password/components overrides to the Sarama config (these take precedence over what's in the configmap)
	util.UpdateSaramaConfig(saramaConfig, Component, ekConfig.Kafka.Username, ekConfig.Kafka.Password)

	// Create The Dispatcher With Specified Configuration
	dispatcherConfig := dispatch.DispatcherConfig{
		Logger:               logger,
		ClientId:             Component,
		Brokers:              strings.Split(ekConfig.Kafka.Brokers, ","),
		Topic:                ekConfig.Kafka.Topic,
		Username:             ekConfig.Kafka.Username,
		Password:             ekConfig.Kafka.Password,
		ChannelKey:           ekConfig.Kafka.ChannelKey,
		StatsReporter:        statsReporter,
		ExponentialBackoff:   ekConfig.Dispatcher.RetryExponentialBackoff,
		InitialRetryInterval: ekConfig.Dispatcher.RetryInitialIntervalMillis,
		MaxRetryTime:         ekConfig.Dispatcher.RetryTimeMillis,
		SaramaConfig:         saramaConfig,
	}
	dispatcher = dispatch.NewDispatcher(dispatcherConfig)

	// Watch The Settings ConfigMap For Changes
	err = commonconfig.InitializeConfigWatcher(logger.Sugar(), ctx, configMapObserver)
	if err != nil {
		logger.Fatal("Failed To Initialize ConfigMap Watcher", zap.Error(err))
	}

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
	kafkaChannelInformer := kafkaInformerFactory.Messaging().V1beta1().KafkaChannels()

	// Construct Array Of Controllers, In Our Case Just The One
	controllers := [...]*kncontroller.Impl{
		controller.NewController(
			logger,
			ekConfig.Kafka.ChannelKey,
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

	// Stop The Liveness And Readiness Servers
	healthServer.Stop(logger)
}

func flush(logger *zap.Logger) {
	_ = logger.Sync()
	eventingmetrics.FlushExporter()
}

// configMapObserver is the callback function that handles changes to our ConfigMap
func configMapObserver(configMap *v1.ConfigMap) {
	if configMap == nil {
		logger.Warn("Nil ConfigMap passed to configMapObserver; ignoring")
		return
	}

	if dispatcher == nil {
		// This typically happens during startup
		logger.Info("Dispatcher is nil during call to configMapObserver; ignoring changes")
		return
	}

	// Toss the new config map to the dispatcher for inspection and action
	newDispatcher := dispatcher.ConfigChanged(configMap)
	if newDispatcher != nil {
		// The configuration change caused a new dispatcher to be created, so switch to that one
		dispatcher = newDispatcher
	}
}
