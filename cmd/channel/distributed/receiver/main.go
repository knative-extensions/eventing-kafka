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
	"net/http"
	"strconv"
	"strings"

	commonconstants "knative.dev/eventing-kafka/pkg/common/constants"
	"knative.dev/pkg/configmap"

	"github.com/cloudevents/sdk-go/v2/binding"
	"github.com/google/uuid"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	distributedcommonconfig "knative.dev/eventing-kafka/pkg/channel/distributed/common/config"
	commonk8s "knative.dev/eventing-kafka/pkg/channel/distributed/common/k8s"
	"knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/sarama"
	kafkautil "knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/util"
	"knative.dev/eventing-kafka/pkg/channel/distributed/receiver/channel"
	"knative.dev/eventing-kafka/pkg/channel/distributed/receiver/constants"
	"knative.dev/eventing-kafka/pkg/channel/distributed/receiver/env"
	channelhealth "knative.dev/eventing-kafka/pkg/channel/distributed/receiver/health"
	"knative.dev/eventing-kafka/pkg/channel/distributed/receiver/producer"
	kafkaclientset "knative.dev/eventing-kafka/pkg/client/clientset/versioned"
	commonconfig "knative.dev/eventing-kafka/pkg/common/config"
	"knative.dev/eventing-kafka/pkg/common/metrics"
	eventingchannel "knative.dev/eventing/pkg/channel"
	injectionclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/injection"
	"knative.dev/pkg/injection/sharedmain"
	"knative.dev/pkg/kmeta"
	"knative.dev/pkg/logging"
	eventingmetrics "knative.dev/pkg/metrics"
	"knative.dev/pkg/signals"
)

// Variables
var (
	logger        *zap.Logger
	kafkaProducer *producer.Producer
)

// The Main Function (Go Command)
func main() {

	ctx := signals.NewContext()

	// Create The K8S Configuration (In-Cluster By Default / Cmd Line Flags For Out-Of-Cluster Usage)
	k8sConfig := injection.ParseAndGetRESTConfigOrDie()

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
		logger.Fatal("Invalid / Missing Environment Variables - Terminating", zap.Error(err))
	}

	// Update The Sarama Config - Username/Password Overrides (Values From Secret Take Precedence Over ConfigMap)
	kafkaAuthCfg, err := distributedcommonconfig.GetAuthConfigFromKubernetes(ctx, environment.KafkaSecretName, environment.KafkaSecretNamespace)
	if err != nil {
		logger.Fatal("Failed To Load Auth Config", zap.Error(err))
	}

	configMap, err := configmap.Load(commonconstants.SettingsConfigMapMountPath)
	if err != nil {
		logger.Fatal("error loading configuration", zap.Error(err))
	}

	// Load The Sarama & Eventing-Kafka Configuration From The ConfigMap
	saramaConfig, ekConfig, err := sarama.LoadSettings(ctx, constants.Component, configMap, kafkaAuthCfg)
	if err != nil {
		logger.Fatal("Failed To Load Sarama Settings", zap.Error(err))
	}

	// Enable Sarama Logging If Specified In ConfigMap
	sarama.EnableSaramaLogging(ekConfig.Kafka.EnableSaramaLogging)

	// Initialize Tracing (Watches config-tracing ConfigMap, Assumes Context Came From LoggingContext With Embedded K8S Client Key)
	err = distributedcommonconfig.InitializeTracing(logger.Sugar(), ctx, environment.ServiceName, environment.SystemNamespace)
	if err != nil {
		logger.Fatal("Could Not Initialize Tracing - Terminating", zap.Error(err))
	}

	// Initialize Observability (Watches config-observability ConfigMap And Starts Profiling Server)
	err = distributedcommonconfig.InitializeObservability(ctx, logger.Sugar(), environment.MetricsDomain, environment.MetricsPort, environment.SystemNamespace)
	if err != nil {
		logger.Fatal("Could Not Initialize Observability - Terminating", zap.Error(err))
	}

	// Start The Liveness And Readiness Servers
	healthServer := channelhealth.NewChannelHealthServer(strconv.Itoa(environment.HealthPort))
	err = healthServer.Start(logger)
	if err != nil {
		logger.Fatal("Failed To Initialize Health Server - Terminating", zap.Error(err))
	}

	// Create A New Kafka Client From The K8S Config
	kafkaClient := kafkaclientset.NewForConfigOrDie(k8sConfig)

	// Initialize The KafkaChannel Lister Used To Validate Events
	err = channel.InitializeKafkaChannelLister(ctx, kafkaClient, healthServer, environment.ResyncPeriod)
	if err != nil {
		logger.Fatal("Failed To Initialize KafkaChannel Lister", zap.Error(err))
	}
	defer channel.Close()

	// Start The Metrics Reporter And Defer Shutdown
	statsReporter := metrics.NewStatsReporter(logger)
	defer statsReporter.Shutdown()

	// Create A Watcher On The Configuration Settings ConfigMap & Dynamically Update Configuration
	// Since this is designed to be called by the main() function, the default KNative package behavior here
	// is a fatal exit if the watch cannot be set up.
	cmw := sharedmain.SetupConfigMapWatchOrDie(ctx, logger.Sugar())

	// Watch The Settings ConfigMap For Changes
	err = commonconfig.InitializeKafkaConfigMapWatcher(ctx, cmw, logger.Sugar(), configMapObserver, environment.SystemNamespace)
	if err != nil {
		logger.Fatal("Failed To Initialize ConfigMap Watcher", zap.Error(err))
	}

	if err := cmw.Start(ctx.Done()); err != nil {
		logger.Fatal("Failed to start configmap watcher", zap.Error(err))
	}

	// Watch The Secret For Changes
	err = distributedcommonconfig.InitializeSecretWatcher(ctx, environment.KafkaSecretNamespace, environment.KafkaSecretName, environment.ResyncPeriod, secretObserver)
	if err != nil {
		logger.Fatal("Failed To Start Secret Watcher", zap.Error(err))
	}

	// Initialize The Kafka Producer In Order To Start Processing Status Events
	kafkaProducer, err = producer.NewProducer(logger, saramaConfig, strings.Split(ekConfig.Kafka.Brokers, ","), statsReporter, healthServer)
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
func handleMessage(ctx context.Context, channelReference eventingchannel.ChannelReference, message binding.Message, transformers []binding.Transformer, _ http.Header) error {

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
func configMapObserver(ctx context.Context, configMap *corev1.ConfigMap) {
	logger := logging.FromContext(ctx)

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
	newProducer := kafkaProducer.ConfigChanged(ctx, configMap)
	if newProducer != nil {
		// The configuration change caused a new producer to be created, so switch to that one
		logger.Info("Config Changed; Receiver Reconfigured")
		kafkaProducer = newProducer
	}
}

// secretObserver is the callback function that handles changes to our Secret
func secretObserver(ctx context.Context, secret *corev1.Secret) {
	logger := logging.FromContext(ctx)

	if secret == nil {
		logger.Warn("Nil Secret passed to secretObserver; ignoring")
		return
	}

	if kafkaProducer == nil {
		// This typically happens during startup
		logger.Debug("Producer is nil during call to secretObserver; ignoring changes")
		return
	}

	// Toss the new secret to the producer for inspection and action
	newProducer := kafkaProducer.SecretChanged(ctx, secret)
	if newProducer != nil {
		// The configuration change caused a new producer to be created, so switch to that one
		logger.Info("Secret Changed; Receiver Reconfigured")
		kafkaProducer = newProducer
	}
}
