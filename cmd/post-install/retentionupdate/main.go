/*
Copyright 2021 The Knative Authors

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
	"strings"

	"github.com/Shopify/sarama"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"k8s.io/client-go/kubernetes"
	injectionclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/injection"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/signals"

	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/config"
	kafkaclientset "knative.dev/eventing-kafka/pkg/client/clientset/versioned"
	commonconstants "knative.dev/eventing-kafka/pkg/common/constants"
	commonsarama "knative.dev/eventing-kafka/pkg/common/kafka/sarama"
)

const (
	Component = "eventing-kafka-v0.26-post-install-job"
)

/* KafkaChannel Retention Updater Post-Install Job (v0.26)
 *
 * This post-install job will update existing KafkaChannel.Spec so that
 * the new RetentionDuration field matches the current configuration of
 * the corresponding Kafka Topic.
 */
func main() {

	// Create A New Context
	ctx := signals.NewContext()

	// Create The K8S Configuration (In-Cluster By Default / Cmd Line Flags For Out-Of-Cluster Usage)
	k8sConfig := injection.ParseAndGetRESTConfigOrDie()

	// Put The Kubernetes Config Into The Context Where The Injection Framework Expects It
	ctx = injection.WithConfig(ctx, k8sConfig)

	// Create A New Kubernetes Client From The K8S Configuration
	k8sClient := kubernetes.NewForConfigOrDie(k8sConfig)

	// Put The Kubernetes Clientset Into The Context Where The Injection Framework Expects It
	ctx = context.WithValue(ctx, injectionclient.Key{}, k8sClient)

	// TODO - REMOVE ?!?!?!
	// Initialize A Knative Injection Lite Context (K8S Client & Logger)
	// ctx = commonk8s.LoggingContext(ctx, Component, k8sClient)

	// Create A New Kafka Client From The K8S Config
	kafkaClient := kafkaclientset.NewForConfigOrDie(k8sConfig)

	// Get The Logger From The Context & Defer Flushing Any Buffered Log Entries On Exit
	logger := logging.FromContext(ctx).Desugar()
	defer flush(logger)

	// Load The Kafka ConfigMap (Specified In Job Container VolumeMount & Expected To Be In Deployed Namespace)
	configMap, err := configmap.Load(commonconstants.SettingsConfigMapMountPath)
	if err != nil {
		logger.Fatal("Failed To Load ConfigMap", zap.String("MountPath", commonconstants.SettingsConfigMapMountPath), zap.Error(err))
	}

	// Load The Sarama & Eventing-Kafka Configuration From The ConfigMap
	ekConfig, err := commonsarama.LoadSettings(ctx, Component, configMap, commonsarama.LoadAuthConfig)
	if err != nil {
		logger.Fatal("Failed To Load Configuration Settings", zap.Error(err))
	}
	err = config.VerifyConfiguration(ekConfig)
	if err != nil {
		logger.Fatal("Failed To Verify Configuration Settings", zap.Error(err))
	}

	// Enable Sarama Logging If Specified In ConfigMap
	commonsarama.EnableSaramaLogging(ekConfig.Sarama.EnableLogging)

	// Extract The Sarama Brokers / Config
	brokers := strings.Split(ekConfig.Kafka.Brokers, ",")
	saramaConfig := ekConfig.Sarama.Config

	// Create A Sarama ClusterAdmin
	clusterAdmin, err := sarama.NewClusterAdmin(brokers, saramaConfig)
	if err != nil {
		logger.Fatal("Failed To Create Sarama ClusterAdmin", zap.Any("Brokers", brokers), zap.Any("Config", saramaConfig), zap.Error(err))
	}

	// Create A KafkachannelUpdater & Perform The Update
	kcUpdater := NewKafkaChannelUpdater(logger, kafkaClient, clusterAdmin)
	err = kcUpdater.Update(ctx)
	if err != nil {
		logger.Fatal("Failed To Update One Or More KafkaChannels", zap.Errors("Update Errors", multierr.Errors(err)))
	}

	// Log Success And Exit
	logger.Info("Successfully Updated KafkaChannels")
}

// Deferred Logger Flush
func flush(logger *zap.Logger) {
	_ = logger.Sync()
}
