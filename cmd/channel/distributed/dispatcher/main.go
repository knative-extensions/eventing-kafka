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
	"strconv"
	"strings"

	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	injectionclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/configmap"
	kncontroller "knative.dev/pkg/controller"
	"knative.dev/pkg/injection"
	"knative.dev/pkg/logging"
	eventingmetrics "knative.dev/pkg/metrics"
	"knative.dev/pkg/signals"

	distributedcommonconfig "knative.dev/eventing-kafka/pkg/channel/distributed/common/config"
	commonk8s "knative.dev/eventing-kafka/pkg/channel/distributed/common/k8s"
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/config"
	"knative.dev/eventing-kafka/pkg/channel/distributed/dispatcher/constants"
	"knative.dev/eventing-kafka/pkg/channel/distributed/dispatcher/controller"
	dispatch "knative.dev/eventing-kafka/pkg/channel/distributed/dispatcher/dispatcher"
	"knative.dev/eventing-kafka/pkg/channel/distributed/dispatcher/env"
	dispatcherhealth "knative.dev/eventing-kafka/pkg/channel/distributed/dispatcher/health"
	kafkaclientset "knative.dev/eventing-kafka/pkg/client/clientset/versioned"
	"knative.dev/eventing-kafka/pkg/client/informers/externalversions"
	commonconstants "knative.dev/eventing-kafka/pkg/common/constants"
	"knative.dev/eventing-kafka/pkg/common/kafka/sarama"
	"knative.dev/eventing-kafka/pkg/common/metrics"
	"knative.dev/eventing/pkg/kncloudevents"
)

// Variables
var (
	logger     *zap.Logger
	dispatcher dispatch.Dispatcher
)

// The Main Function (Go Command)
func main() {

	ctx := signals.NewContext()

	// Create The K8S Configuration (In-Cluster By Default / Cmd Line Flags For Out-Of-Cluster Usage)
	k8sConfig := injection.ParseAndGetRESTConfigOrDie()

	// TODO: do we really need these? I moved them up
	const numControllers = 1
	k8sConfig.QPS = numControllers * rest.DefaultQPS
	k8sConfig.Burst = numControllers * rest.DefaultBurst

	// Put The Kubernetes Config Into The Context Where The Injection Framework Expects It
	ctx = injection.WithConfig(ctx, k8sConfig)

	// Create A New Kubernetes Client From The K8S Configuration
	k8sClient := kubernetes.NewForConfigOrDie(k8sConfig)

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

	configMap, err := configmap.Load(commonconstants.SettingsConfigMapMountPath)
	if err != nil {
		logger.Fatal("error loading configuration", zap.Error(err))
	}

	// Load The Sarama & Eventing-Kafka Configuration From The ConfigMap
	ekConfig, err := sarama.LoadSettings(ctx, constants.Component, configMap, sarama.LoadAuthConfig)
	if err != nil {
		logger.Fatal("Failed To Load Configuration Settings", zap.Error(err))
	}
	err = config.VerifyConfiguration(ekConfig)
	if err != nil {
		logger.Fatal("Failed To Verify Configuration Settings", zap.Error(err))
	}

	// Enable Sarama Logging If Specified In ConfigMap
	sarama.EnableSaramaLogging(ekConfig.Sarama.EnableLogging)

	// Initialize Tracing (Watches config-tracing ConfigMap, Assumes Context Came From LoggingContext With Embedded K8S Client Key)
	err = distributedcommonconfig.InitializeTracing(logger.Sugar(), ctx, environment.ServiceName, environment.SystemNamespace)
	if err != nil {
		logger.Fatal("Failed To Initialize Tracing - Terminating", zap.Error(err))
	}

	// Initialize Observability (Watches config-observability ConfigMap And Starts Profiling Server)
	err = distributedcommonconfig.InitializeObservability(ctx, logger.Sugar(), environment.MetricsDomain, environment.MetricsPort, environment.SystemNamespace)
	if err != nil {
		logger.Fatal("Failed To Initialize Observability - Terminating", zap.Error(err))
	}

	// Start The Liveness And Readiness Servers
	healthServer := dispatcherhealth.NewDispatcherHealthServer(strconv.Itoa(environment.HealthPort))
	err = healthServer.Start(logger)
	if err != nil {
		logger.Fatal("Failed To Initialize Health Server - Terminating", zap.Error(err))
	}

	// Start The Metrics Reporter And Defer Shutdown
	statsReporter := metrics.NewStatsReporter(logger)
	defer statsReporter.Shutdown()

	// Change The CloudEvent Connection Args
	kncloudevents.ConfigureConnectionArgs(&kncloudevents.ConnectionArgs{
		MaxIdleConns:        ekConfig.CloudEvents.MaxIdleConns,
		MaxIdleConnsPerHost: ekConfig.CloudEvents.MaxIdleConnsPerHost,
	})

	// Create The Dispatcher With Specified Configuration
	dispatcherConfig := dispatch.DispatcherConfig{
		Logger:          logger,
		ClientId:        constants.Component,
		Brokers:         strings.Split(ekConfig.Kafka.Brokers, ","),
		Topic:           environment.KafkaTopic,
		ChannelKey:      environment.ChannelKey,
		StatsReporter:   statsReporter,
		MetricsRegistry: ekConfig.Sarama.Config.MetricRegistry,
		SaramaConfig:    ekConfig.Sarama.Config,
	}
	dispatcher = dispatch.NewDispatcher(dispatcherConfig)

	// Watch The Secret For Changes
	err = distributedcommonconfig.InitializeSecretWatcher(ctx, environment.KafkaSecretNamespace, environment.KafkaSecretName, environment.ResyncPeriod, secretObserver)
	if err != nil {
		logger.Fatal("Failed To Start Secret Watcher", zap.Error(err))
	}

	kafkaClient := kafkaclientset.NewForConfigOrDie(k8sConfig)

	kafkaInformerFactory := externalversions.NewSharedInformerFactory(kafkaClient, environment.ResyncPeriod)

	// Create KafkaChannel Informer
	kafkaChannelInformer := kafkaInformerFactory.Messaging().V1beta1().KafkaChannels()

	// Construct Array Of Controllers, In Our Case Just The One
	controllers := [...]*kncontroller.Impl{
		controller.NewController(
			logger,
			environment.ChannelKey,
			dispatcher,
			kafkaChannelInformer,
			k8sClient,
			kafkaClient,
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

// secretObserver is the callback function that handles changes to our Secret
func secretObserver(ctx context.Context, secret *corev1.Secret) {
	secretLogger := logging.FromContext(ctx)

	if secret == nil {
		secretLogger.Warn("Nil Secret passed to secretObserver; ignoring")
		return
	}

	if dispatcher == nil {
		// This typically happens during startup
		secretLogger.Debug("Dispatcher is nil during call to secretObserver; ignoring changes")
		return
	}

	// Toss the new secret to the dispatcher for inspection and action
	dispatcher.SecretChanged(ctx, secret)
}
