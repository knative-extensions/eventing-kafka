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
	"knative.dev/pkg/logging"
	"knative.dev/pkg/reconciler"

	kafkav1alpha1 "knative.dev/eventing-kafka/pkg/apis/kafka/v1alpha1"
	"knative.dev/eventing-kafka/pkg/client/injection/reconciler/kafka/v1alpha1/resetoffset"
	kafkalisters "knative.dev/eventing-kafka/pkg/client/listers/kafka/v1alpha1"
	"knative.dev/eventing-kafka/pkg/common/commands/resetoffset/refmappers"
	kafkasarama "knative.dev/eventing-kafka/pkg/common/kafka/sarama"
)

// Component For Sarama Config
const Component = "reset-offset-controller"

var (
	_ resetoffset.Interface = (*Reconciler)(nil) // Verify Reconciler Implements Interface
	_ resetoffset.Finalizer = (*Reconciler)(nil) // Verify Reconciler Implements Finalizer
)

// Reconciler Implements controller.Reconciler for ResetOffset Resources
type Reconciler struct {
	kafkaBrokers      []string
	saramaConfig      *sarama.Config
	resetoffsetLister kafkalisters.ResetOffsetLister
	refMapper         refmappers.ResetOffsetRefMapper
}

// ReconcileKind implements the Reconciler Interface and is responsible for performing Offset repositioning.
func (r *Reconciler) ReconcileKind(ctx context.Context, resetOffset *kafkav1alpha1.ResetOffset) reconciler.Event {

	// Get The Logger From Context
	logger := logging.FromContext(ctx).Desugar()
	logger.Debug("<==========  START RESET-OFFSET RECONCILIATION  ==========>")

	// Ignore Previously Initiated ResetOffset Instances
	if resetOffset.Status.IsInitiated() || resetOffset.Status.IsCompleted() {
		logger.Debug("Skipping reconciliation of previously executed ResetOffset instance")
		return reconciler.NewEvent(corev1.EventTypeNormal, ResetOffsetSkipped.String(), "Skipped previously executed ResetOffset")
	}

	// Reset The ResetOffset's Status Conditions To Unknown
	resetOffset.Status.InitializeConditions()

	// Map The ResetOffset's Ref To Kafka Topic Name / ConsumerGroup ID
	topic, group, err := r.refMapper.MapRef(resetOffset)
	if err != nil {
		logger.Error("Failed to map ResetOffset.Spec.Ref to Kafka Topic name and ConsumerGroup ID", zap.Error(err))
		resetOffset.Status.MarkRefMappedFailed("FailedToMapRef", "Failed to map 'ref' to Kafka Topic and Group: %v", err)
		return fmt.Errorf("failed to map 'ref' to Kafka Topic and Group: %v", err)
	}
	logger.Info("Successfully mapped ResetOffset.Spec.Ref to Kafka Topic name and ConsumerGroup ID", zap.String("Topic", topic), zap.String("Group", group))
	resetOffset.Status.SetTopic(topic)
	resetOffset.Status.SetGroup(group)
	resetOffset.Status.MarkRefMappedTrue()

	// Parse The Sarama Offset Time From ResetOffset Spec
	offsetTime, err := resetOffset.Spec.ParseSaramaOffsetTime()
	if err != nil {
		logger.Error("Failed to parse Sarama Offset Time from ResetOffset Spec", zap.Error(err))
		return err // Should never happen assuming Validation is in place
	}
	logger.Info("Successfully parsed Sarama Offset time from ResetOffset Spec", zap.Int64("Time (millis)", offsetTime))

	// TODO - Map ResetOffset.Spec.Ref to "owning" Dispatcher Deployment/Service (Consolidated = one, Distributed = many)

	// TODO - Send "Initiate" Message to Dispatcher Replicas to lock mutext and start process.
	resetOffset.Status.MarkResetInitiatedTrue()

	// TODO - Send "Stop" Message to Dispatcher Replicas to stop ConsumerGroups
	resetOffset.Status.MarkConsumerGroupsStoppedTrue()

	// Update The Sarama Offsets & Update ResetOffset CRD With OffsetMappings
	offsetMappings, err := r.reconcileOffsets(ctx, topic, group, offsetTime)
	if err != nil {
		logger.Error("Failed to update Offsets of one or more partitions", zap.Error(err))
		resetOffset.Status.MarkOffsetsUpdatedFailed("FailedToUpdateOffsets", "Failed to update offsets of one or more partitions: %v", err)
		return fmt.Errorf("failed to update offsets of one or more partitions: %v", err)
	}
	if offsetMappings != nil {
		resetOffset.Status.SetPartitions(offsetMappings)
	}
	logger.Info("Successfully updated Offsets of all partitions")
	resetOffset.Status.MarkOffsetsUpdatedTrue()

	// TODO - Send "Start" Message to Dispatcher Replicas to restart ConsumerGroups
	resetOffset.Status.MarkConsumerGroupsStartedTrue()

	// Return Reconciled Success Event
	return reconciler.NewEvent(corev1.EventTypeNormal, ResetOffsetReconciled.String(), "Reconciled successfully")
}

// FinalizeKind implements the Finalizer Interface and is responsible for performing any necessary cleanup.
func (r *Reconciler) FinalizeKind(ctx context.Context, resetOffset *kafkav1alpha1.ResetOffset) reconciler.Event {

	// Get The Logger From Context
	logger := logging.FromContext(ctx)
	logger.Debug("<==========  START RESET-OFFSET FINALIZATION  ==========>")

	// Do NOT Finalize ResetOffset Instances That Are "Executing"
	if resetOffset.Status.IsInitiated() && !resetOffset.Status.IsCompleted() {
		logger.Debug("Skipping finalization of in-progress ResetOffset instance")
		return fmt.Errorf("skipping finalization of in-progress ResetOffset instance")
	}

	// No-Op Finalization - Nothing To Do
	logger.Info("No-Op Finalization Successful")

	// Return Finalized Success Event
	return reconciler.NewEvent(corev1.EventTypeNormal, ResetOffsetFinalized.String(), "Finalized successfully")
}

// updateKafkaConfig is the callback function that handles changes to the ConfigMap
func (r *Reconciler) updateKafkaConfig(ctx context.Context, configMap *corev1.ConfigMap) {

	// Get The Logger From Context
	logger := logging.FromContext(ctx).Desugar()

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
		logger.Warn("Ignoring nil ConfigMap.Data")
		return
	}

	// Load The EventingKafkaConfig From ConfigMap.Data
	ekConfig, err := kafkasarama.LoadSettings(ctx, Component, configMap.Data, kafkasarama.LoadAuthConfig)
	if err != nil || ekConfig == nil {
		logger.Error("Failed to extract EventingKafkaConfig from ConfigMap", zap.Any("ConfigMap", configMap), zap.Error(err))
		return
	}

	// Update The KafkaBrokers On Reconciler
	r.kafkaBrokers = strings.Split(ekConfig.Kafka.Brokers, ",")

	// Enable/Disable Sarama Logging Based On ConfigMap Setting
	kafkasarama.EnableSaramaLogging(ekConfig.Sarama.EnableLogging)
	logger.Debug("Set Sarama logging", zap.Bool("Enabled", ekConfig.Sarama.EnableLogging))

	// Force Enable Consumer Error Handling
	ekConfig.Sarama.Config.Consumer.Return.Errors = true

	// Force Disable Manual Commits
	ekConfig.Sarama.Config.Consumer.Offsets.AutoCommit.Enable = false

	// Update Reconciler With New Config
	r.saramaConfig = ekConfig.Sarama.Config
}
