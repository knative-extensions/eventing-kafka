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
	"strconv"
	"strings"

	"knative.dev/eventing/pkg/kncloudevents"

	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	commonconfig "knative.dev/eventing-kafka/pkg/channel/distributed/common/config"
	commonk8s "knative.dev/eventing-kafka/pkg/channel/distributed/common/k8s"
	"knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/sarama"
	"knative.dev/eventing-kafka/pkg/channel/distributed/common/metrics"
	"knative.dev/eventing-kafka/pkg/channel/distributed/dispatcher/constants"
	"knative.dev/eventing-kafka/pkg/channel/distributed/dispatcher/controller"
	dispatch "knative.dev/eventing-kafka/pkg/channel/distributed/dispatcher/dispatcher"
	"knative.dev/eventing-kafka/pkg/channel/distributed/dispatcher/env"
	dispatcherhealth "knative.dev/eventing-kafka/pkg/channel/distributed/dispatcher/health"
	"knative.dev/eventing-kafka/pkg/client/clientset/versioned"
	"knative.dev/eventing-kafka/pkg/client/informers/externalversions"
	injectionclient "knative.dev/pkg/client/injection/kube/client"
	kncontroller "knative.dev/pkg/controller"
	"knative.dev/pkg/logging"
	eventingmetrics "knative.dev/pkg/metrics"
	"knative.dev/pkg/signals"
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

	ctx := signals.NewContext()

	// Get The K8S Client
	k8sClient := commonk8s.K8sClientWrapper(*serverURL, *kubeconfig)

	// Put The Kubernetes Client Into The Context Where The Injection Framework Expects It
	ctx = context.WithValue(ctx, injectionclient.Key{}, k8sClient)

	// Initialize A Knative Injection Lite Context (K8S Client & Logger)
	ctx = commonk8s.LoggingContext(ctx, constants.Component, k8sClient)

	// Get The Logger From The Context & Defer Flushing Any Buffered Log Entries On Exit
	logger = logging.FromContext(ctx).Desugar()
	defer flush(logger)

	// Load Environment Variables
	environment, err := env.GetEnvironment(logger)
	if err != nil {
		logger.Fatal("Failed To Load Environment Variables - Terminating!", zap.Error(err))
	}

	// Update The Sarama Config - Username/Password Overrides (Values From Secret Take Precedence Over ConfigMap)
	kafkaAuthCfg, err := commonconfig.GetAuthConfigFromKubernetes(ctx, environment.KafkaSecretName, environment.KafkaSecretNamespace)
	if err != nil {
		logger.Fatal("Failed To Load Auth Config", zap.Error(err))
	}

	// Load The Sarama & Eventing-Kafka Configuration From The ConfigMap
	saramaConfig, ekConfig, err := sarama.LoadSettings(ctx, constants.Component, kafkaAuthCfg)
	if err != nil {
		logger.Fatal("Failed To Load Sarama Settings", zap.Error(err))
	}

	// Enable Sarama Logging If Specified In ConfigMap
	sarama.EnableSaramaLogging(ekConfig.Kafka.EnableSaramaLogging)

	// Initialize Tracing (Watches config-tracing ConfigMap, Assumes Context Came From LoggingContext With Embedded K8S Client Key)
	err = commonconfig.InitializeTracing(logger.Sugar(), ctx, environment.ServiceName, environment.SystemNamespace)
	if err != nil {
		logger.Fatal("Failed To Initialize Tracing - Terminating", zap.Error(err))
	}

	// Initialize Observability (Watches config-observability ConfigMap And Starts Profiling Server)
	err = commonconfig.InitializeObservability(ctx, logger.Sugar(), environment.MetricsDomain, environment.MetricsPort, environment.SystemNamespace)
	if err != nil {
		logger.Fatal("Failed To Initialize Observability - Terminating", zap.Error(err))
	}

	// Start The Liveness And Readiness Servers
	healthServer := dispatcherhealth.NewDispatcherHealthServer(strconv.Itoa(environment.HealthPort))
	err = healthServer.Start(logger)
	if err != nil {
		logger.Fatal("Failed To Initialize Health Server - Terminating", zap.Error(err))
	}

	statsReporter := metrics.NewStatsReporter(logger)

	// Increase The Idle Connection Limits From Transport Defaults (see net/http/DefaultTransport)
	kncloudevents.ConfigureConnectionArgs(&kncloudevents.ConnectionArgs{
		MaxIdleConns:        constants.DefaultMaxIdleConns,
		MaxIdleConnsPerHost: constants.DefaultMaxIdleConnsPerHost,
	})

	// Create The Dispatcher With Specified Configuration
	dispatcherConfig := dispatch.DispatcherConfig{
		Logger:        logger,
		ClientId:      constants.Component,
		Brokers:       strings.Split(ekConfig.Kafka.Brokers, ","),
		Topic:         environment.KafkaTopic,
		Username:      kafkaAuthCfg.SASL.User,
		Password:      kafkaAuthCfg.SASL.Password,
		ChannelKey:    environment.ChannelKey,
		StatsReporter: statsReporter,
		SaramaConfig:  saramaConfig,
	}
	dispatcher = dispatch.NewDispatcher(dispatcherConfig)

	// Watch The Settings ConfigMap For Changes
	err = commonconfig.InitializeConfigWatcher(ctx, logger.Sugar(), configMapObserver, environment.SystemNamespace)
	if err != nil {
		logger.Fatal("Failed To Initialize ConfigMap Watcher", zap.Error(err))
	}

	// Watch The Secret For Changes
	err = commonconfig.InitializeSecretWatcher(ctx, environment.KafkaSecretNamespace, environment.KafkaSecretName, secretObserver)
	if err != nil {
		logger.Fatal("Failed To Start Secret Watcher", zap.Error(err))
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

	kafkaInformerFactory := externalversions.NewSharedInformerFactory(kafkaClientSet, environment.ResyncPeriod)

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
func configMapObserver(ctx context.Context, configMap *corev1.ConfigMap) {
	logger := logging.FromContext(ctx)

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
	newDispatcher := dispatcher.ConfigChanged(ctx, configMap)
	if newDispatcher != nil {
		// The configuration change caused a new dispatcher to be created, so switch to that one
		logger.Info("Config Changed; Dispatcher Reconfigured")
		dispatcher = newDispatcher
	}
}

// secretObserver is the callback function that handles changes to our Secret
func secretObserver(ctx context.Context, secret *corev1.Secret) {
	logger := logging.FromContext(ctx)

	if secret == nil {
		logger.Warn("Nil Secret passed to secretObserver; ignoring")
		return
	}

	if dispatcher == nil {
		// This typically happens during startup
		logger.Debug("Dispatcher is nil during call to secretObserver; ignoring changes")
		return
	}

	// Toss the new secret to the dispatcher for inspection and action
	newDispatcher := dispatcher.SecretChanged(ctx, secret)
	if newDispatcher != nil {
		// The configuration change caused a new dispatcher to be created, so switch to that one
		logger.Info("Secret Changed; Dispatcher Reconfigured")
		dispatcher = newDispatcher
	}
}
