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

package controller

import (
	"context"
	"fmt"
	"strings"

	"github.com/Shopify/sarama"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/reconciler"

	kafkav1alpha1 "knative.dev/eventing-kafka/pkg/apis/kafka/v1alpha1"
	kafkasarama "knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/sarama"
	"knative.dev/eventing-kafka/pkg/client/injection/reconciler/kafka/v1alpha1/resetoffset"
	kafkalisters "knative.dev/eventing-kafka/pkg/client/listers/kafka/v1alpha1"
	"knative.dev/eventing-kafka/pkg/common/client"
	"knative.dev/eventing-kafka/pkg/common/commands/resetoffset/refmappers"
	commonconstants "knative.dev/eventing-kafka/pkg/common/constants"
)

var (
	_ resetoffset.Interface = (*Reconciler)(nil) // Verify Reconciler Implements Interface
	_ resetoffset.Finalizer = (*Reconciler)(nil) // Verify Reconciler Implements Finalizer
)

// Reconciler Implements controller.Reconciler for ResetOffset Resources
type Reconciler struct {
	kafkaBrokers        []string
	saramaConfig        *sarama.Config
	resetoffsetInformer cache.SharedIndexInformer
	resetoffsetLister   kafkalisters.ResetOffsetLister
	refMapper           refmappers.ResetOffsetRefMapper
}

// ReconcileKind Implements The Reconciler Interface & Is Responsible For Performing The Reconciliation (Creation)
func (r *Reconciler) ReconcileKind(ctx context.Context, resetOffset *kafkav1alpha1.ResetOffset) reconciler.Event {

	// Get The Logger From Context
	logger := logging.FromContext(ctx).Desugar()

	// TODO - REMOVE
	logger.Info("STATUS 1",
		zap.Bool("IsInitiated", resetOffset.Status.IsInitiated()),
		zap.Bool("IsCompleted", resetOffset.Status.IsCompleted()),
		zap.Bool("IsSucceeded", resetOffset.Status.IsSucceeded()))

	// Ignore Previously Initiated ResetOffset Instances
	if resetOffset == nil || resetOffset.Status.IsInitiated() || resetOffset.Status.IsCompleted() {
		logger.Debug("Skipping reconciliation of previously executed ResetOffset instance")
		return reconciler.NewEvent(corev1.EventTypeNormal, ResetOffsetSkipped.String(), "Skipped \"%s/%s\"", resetOffset.Namespace, resetOffset.Name)
	}
	logger.Debug("<==========  START RESET-OFFSET RECONCILIATION  ==========>")

	// Set Defaults & Verify The ResetOffset Is Valid
	resetOffset.SetDefaults(ctx)
	if err := resetOffset.Validate(ctx); err != nil {
		logger.Error("Invalid ResetOffset", zap.Error(err))
		return err
	}

	// Reset The ResetOffset's Status Conditions To Unknown
	resetOffset.Status.InitializeConditions()

	// TODO - REMOVE
	logger.Info("STATUS 2",
		zap.Bool("IsInitiated", resetOffset.Status.IsInitiated()),
		zap.Bool("IsCompleted", resetOffset.Status.IsCompleted()),
		zap.Bool("IsSucceeded", resetOffset.Status.IsSucceeded()))

	// Initialize A New Sarama Client
	saramaClient, err := sarama.NewClient(r.kafkaBrokers, r.saramaConfig)
	if err != nil {
		logger.Error("Failed to create a new Sarama Client", zap.Error(err))
		return err
	}
	defer r.safeCloseSaramaClient(ctx, saramaClient)

	// Map The ResetOffset's Ref To Kafka Topic Name / ConsumerGroup ID
	topic, group, err := r.refMapper.MapRef(resetOffset)
	if err != nil {
		resetOffset.Status.MarkRefMappedFailed("FailedToMapRef", "TODO") // TODO - message - !!!
		logger.Error("Failed to map ResetOffset.Spec.Ref to Kafka Topic name / ConsumerGroup ID", zap.Error(err))
		return err
	}
	resetOffset.Status.SetTopic(topic)
	resetOffset.Status.SetGroup(group)
	resetOffset.Status.MarkRefMappedTrue()

	// Parse The Sarama Offset Time From ResetOffset Spec
	time, err := resetOffset.Spec.ParseSaramaOffsetTime()
	if err != nil {
		logger.Error("Failed to parse Sarama Offset Time (millis) from ResetOffset Spec", zap.Error(err))
		return err // Should never happen assuming Validation is in place
	}

	// TODO - Map ResetOffset.Spec.Ref to "owning" Dispatcher Deployment/Service (Consolidated = one, Distributed = many)

	// TODO - Send "Initiate" Message to Dispatcher Replicas to lock mutext and start process.
	resetOffset.Status.MarkResetInitiatedTrue()

	// TODO - Send "Stop" Message to Dispatcher Replicas to stop ConsumerGroups (Initiate & Stop might be the same message)
	resetOffset.Status.MarkConsumerGroupsStoppedTrue()

	// Update The Sarama Offsets & Update ResetOffset CRD With OffsetMappings
	offsetMappings, err := r.updateOffsets(ctx, saramaClient, topic, group, time)
	if err != nil {
		logger.Error("Failed to update Offsets", zap.Error(err))
		resetOffset.Status.MarkOffsetsUpdatedFailed("FailedToUpdateOffsets", "TODO") // TODO - message - !!!
		// TODO reconciler.NewEvent(corev1.EventTypeWarning, "Reconciliation", "Failed to reconcile \"%s/%s\"", resetOffset.Namespace, resetOffset.Name)
		return err
	}
	resetOffset.Status.MarkOffsetsUpdatedTrue()
	resetOffset.Status.SetPartitions(offsetMappings)

	// TODO - Send "Start" Message to Dispatcher Replicas to restart ConsumerGroups
	resetOffset.Status.MarkConsumerGroupsStartedFailed("FailedToStartConsumerGroup", "failed to start consumergroup 123")

	// TODO - if error do rollback/restart!!!???

	// Return Reconciled Success Event
	return reconciler.NewEvent(corev1.EventTypeNormal, ResetOffsetReconciled.String(), "Reconciled \"%s/%s\" successfully", resetOffset.Namespace, resetOffset.Name)
}

// FinalizeKind Implements The Finalizer Interface & Is Responsible For Performing The Finalization (Deletion)
func (r *Reconciler) FinalizeKind(ctx context.Context, resetOffset *kafkav1alpha1.ResetOffset) reconciler.Event {

	// Get The Logger From Context
	logger := logging.FromContext(ctx)

	// TODO - this is bad?   won't be able to remove incomplete (succeeded != unknown) resetoffsets ?  is that ok?
	//        trying to prevent removing in-progress ResetOffset?
	// Ignore Previously Initiated ResetOffset Instances
	if resetOffset == nil || (resetOffset.Status.IsInitiated() && !resetOffset.Status.IsCompleted()) {
		logger.Debug("Skipping finalization of non-completed ResetOffset instance")
		return nil
	}
	logger.Debug("<==========  START RESET-OFFSET FINALIZATION  ==========>")

	// TODO - Implement Finalizer
	logger.Info("FinalizeKind() - TODO - IMPLEMENT ME !")

	// Return Finalized Success Event
	return reconciler.NewEvent(corev1.EventTypeNormal, "Finalization", "Finalized \"%s\" successfully", resetOffset.Name)
}

// safeCloseSaramaClient will attempt to close the specified Sarama Client and will reset the Reconciler reference
func (r *Reconciler) safeCloseSaramaClient(ctx context.Context, client sarama.Client) {
	if client != nil {
		err := client.Close()
		if err != nil && ctx != nil {
			logging.FromContext(ctx).Warn("Failed to close Sarama Client", zap.Error(err))
		}
	}
}

// TODO - This function will need to be refactored once the new "common" ConfigMap implementation is in place!
// updateKafkaConfig is the callback function that handles changes to the ConfigMap
func (r *Reconciler) updateKafkaConfig(ctx context.Context, configMap *corev1.ConfigMap) {

	// Get The Logger From Context
	logger := logging.FromContext(ctx)

	// Validate Reconciler Reference (Sometimes nil on startup and can be ignored)
	if r == nil {
		return
	}

	// Validate ConfigMap Reference
	if configMap == nil {
		logger.Warn("Ignoring nil ConfigMap")
		return
	}

	// Enhance Logger With ConfigMap
	logger = logger.With(zap.String("ConfigMap", fmt.Sprintf("%s/%s", configMap.Namespace, configMap.Name)))

	// Validate ConfigMap.Data
	if configMap.Data == nil {
		logger.Warn("Ignoring nil ConfigMap.Data") // TODO - this was Fatal() in other implementations?
		return
	}

	// Load The EventingKafkaConfig From ConfigMap.Data
	ekConfig, err := kafkasarama.LoadEventingKafkaSettings(configMap.Data)
	if err != nil || ekConfig == nil {
		logger.Error("Failed to extract EventingKafkaConfig from ConfigMap", zap.Any("ConfigMap", configMap), zap.Error(err))
		return
	}

	// Update The KafkaBrokers On Reconciler
	r.kafkaBrokers = strings.Split(ekConfig.Kafka.Brokers, ",")

	// Enable/Disable Sarama Logging Based On ConfigMap Setting
	kafkasarama.EnableSaramaLogging(ekConfig.Kafka.EnableSaramaLogging)
	logger.Debug("Set Sarama logging", zap.Bool("Enabled", ekConfig.Kafka.EnableSaramaLogging))

	// Get The Sarama Config Yaml From The ConfigMap
	saramaSettingsYamlString := configMap.Data[commonconstants.SaramaSettingsConfigKey]

	// Create A Kafka Auth Config From Current Credentials (Secret Data Takes Precedence Over ConfigMap)
	var kafkaAuthCfg *client.KafkaAuthConfig // TODO - temporarily using nil for testing locally with Strimzi - will use new configmap structs ; )

	// Build A New Sarama Config With Auth From Secret And YAML Config From ConfigMap
	saramaConfig, err := client.NewConfigBuilder().
		WithDefaults().
		WithAuth(kafkaAuthCfg).
		FromYaml(saramaSettingsYamlString).
		Build(ctx)
	if err != nil {
		logger.Fatal("Failed to create Sarama Config based on ConfigMap", zap.Error(err))
	}

	// Force Consumer Error Handling!
	saramaConfig.Consumer.Return.Errors = true

	// Update Reconciler With New Sarama Config
	r.saramaConfig = saramaConfig
}
