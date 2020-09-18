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
	commonconfig "knative.dev/eventing-kafka/pkg/channel/distributed/common/config"
	commonk8s "knative.dev/eventing-kafka/pkg/channel/distributed/common/k8s"
	"knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/sarama"
	"knative.dev/eventing-kafka/pkg/channel/distributed/common/metrics"
	dispatcherconfig "knative.dev/eventing-kafka/pkg/channel/distributed/dispatcher/config"
	"knative.dev/eventing-kafka/pkg/channel/distributed/dispatcher/controller"
	dispatch "knative.dev/eventing-kafka/pkg/channel/distributed/dispatcher/dispatcher"
	"knative.dev/eventing-kafka/pkg/channel/distributed/dispatcher/env"
	dispatcherhealth "knative.dev/eventing-kafka/pkg/channel/distributed/dispatcher/health"
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
	serverURL  = flag.String("server", "", "The address of the Kubernetes API server. Overrides any value in kubeconfig. Only required if out-of-cluster.")
	kubeconfig = flag.String("kubeconfig", "", "Path to a kubeconfig. Only required if out-of-cluster.")
)

// The Main Function (Go Command)
func main() {

	// Parse The Flags For Local Development Usage
	flag.Parse()

	// Initialize A Knative Injection Lite Context (K8S Client & Logger)
	ctx := commonk8s.LoggingContext(signals.NewContext(), Component, *serverURL, *kubeconfig)

	// Get The Logger From The Context & Defer Flushing Any Buffered Log Entries On Exit
	logger = logging.FromContext(ctx).Desugar()
	defer flush(logger)

	// UnComment To Enable Sarama Logging For Local Debug
	// sarama.EnableSaramaLogging()

	// Load Environment Variables
	environment, err := env.GetEnvironment(logger)
	if err != nil {
		logger.Fatal("Failed To Load Environment Variables - Terminating!", zap.Error(err))
	}

	// Load The Sarama & Eventing-Kafka Configuration From The ConfigMap
	saramaConfig, configuration, err := sarama.LoadSettings(ctx)
	if err != nil {
		logger.Fatal("Failed To Load Sarama Settings", zap.Error(err))
	}

	// Verify that our loaded configuration is valid
	if err = dispatcherconfig.VerifyConfiguration(configuration); err != nil {
		logger.Fatal("Invalid / Missing Settings - Terminating", zap.Error(err))
	}

	// Initialize Tracing (Watches config-tracing ConfigMap, Assumes Context Came From LoggingContext With Embedded K8S Client Key)
	err = commonconfig.InitializeTracing(logger.Sugar(), ctx, environment.ServiceName)
	if err != nil {
		logger.Fatal("Failed To Initialize Tracing - Terminating", zap.Error(err))
	}

	// Initialize Observability (Watches config-observability ConfigMap And Starts Profiling Server)
	err = commonconfig.InitializeObservability(ctx, logger.Sugar(), environment.MetricsDomain, environment.MetricsPort)
	if err != nil {
		logger.Fatal("Failed To Initialize Observability - Terminating", zap.Error(err))
	}

	// Start The Liveness And Readiness Servers
	healthServer := dispatcherhealth.NewDispatcherHealthServer(strconv.Itoa(environment.HealthPort))
	healthServer.Start(logger)

	statsReporter := metrics.NewStatsReporter(logger)

	// Update The Sarama Config - Username/Password Overrides (EnvVars From Secret Take Precedence Over ConfigMap)
	sarama.UpdateSaramaConfig(saramaConfig, Component, environment.KafkaUsername, environment.KafkaPassword)

	// Create The Dispatcher With Specified Configuration
	dispatcherConfig := dispatch.DispatcherConfig{
		Logger:               logger,
		ClientId:             Component,
		Brokers:              strings.Split(environment.KafkaBrokers, ","),
		Topic:                environment.KafkaTopic,
		Username:             environment.KafkaUsername,
		Password:             environment.KafkaPassword,
		ChannelKey:           environment.ChannelKey,
		StatsReporter:        statsReporter,
		ExponentialBackoff:   *configuration.Dispatcher.RetryExponentialBackoff,
		InitialRetryInterval: configuration.Dispatcher.RetryInitialIntervalMillis,
		MaxRetryTime:         configuration.Dispatcher.RetryTimeMillis,
		SaramaConfig:         saramaConfig,
	}
	dispatcher = dispatch.NewDispatcher(dispatcherConfig)

	// Watch The Settings ConfigMap For Changes
	err = commonconfig.InitializeConfigWatcher(ctx, logger.Sugar(), configMapObserver)
	if err != nil {
		logger.Fatal("Failed To Initialize ConfigMap Watcher", zap.Error(err))
	}

	config, err := clientcmd.BuildConfigFromFlags(*serverURL, *kubeconfig)
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
			environment.ChannelKey,
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
