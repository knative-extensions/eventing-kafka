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
	corev1listers "k8s.io/client-go/listers/core/v1"
	control "knative.dev/control-protocol/pkg"
	ctrlreconciler "knative.dev/control-protocol/pkg/reconciler"
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
	kafkaBrokers                  []string
	saramaConfig                  *sarama.Config
	podLister                     corev1listers.PodLister
	resetoffsetLister             kafkalisters.ResetOffsetLister
	refMapper                     refmappers.ResetOffsetRefMapper
	connectionPool                *ctrlreconciler.ControlPlaneConnectionPool
	asyncCommandNotificationStore *ctrlreconciler.AsyncCommandNotificationStore
}

// ReconcileKind implements the Reconciler Interface and is responsible for performing Offset repositioning.
func (r *Reconciler) ReconcileKind(ctx context.Context, resetOffset *kafkav1alpha1.ResetOffset) reconciler.Event {

	// Get The Logger From Context
	logger := logging.FromContext(ctx).Desugar()
	logger.Debug("<==========  START RESET-OFFSET RECONCILIATION  ==========>")

	//
	// Ignore Previously Initiated ResetOffset Instances
	//
	// Normally this would be achieved via a cache.FilteringResourceEventHandler{} directly
	// in the controller, but that only has a metav1.Object to deal with which does not
	// expose the ResetOffset.Status we need to make the filtering decision, so we are left
	// to handle it here in the Reconciler.
	//
	if resetOffset.Status.IsInitiated() || resetOffset.Status.IsCompleted() {
		logger.Debug("Skipping reconciliation of previously executed ResetOffset instance")
		return reconciler.NewEvent(corev1.EventTypeNormal, ResetOffsetSkipped.String(), "Skipped previously executed ResetOffset")
	}

	// Reset The ResetOffset's Status Conditions To Unknown
	resetOffset.Status.InitializeConditions()

	// Map The ResetOffset's Ref To Kafka Topic Name / ConsumerGroup ID
	refInfo, err := r.refMapper.MapRef(resetOffset)
	if err != nil {
		logger.Error("Failed to map ResetOffset.Spec.Ref to Kafka Topic name and ConsumerGroup ID", zap.Error(err))
		resetOffset.Status.MarkRefMappedFailed("FailedToMapRef", "Failed to map 'ref' to Kafka Topic and Group: %v", err)
		return fmt.Errorf("failed to map 'ref' to Kafka Topic and Group: %v", err)
	}
	logger.Info("Successfully mapped ResetOffset.Spec.Ref", zap.Any("RefInfo", refInfo))
	resetOffset.Status.SetTopic(refInfo.TopicName)
	resetOffset.Status.SetGroup(refInfo.GroupId)
	resetOffset.Status.MarkRefMappedTrue()

	// Parse The Sarama Offset Time From ResetOffset Spec
	offsetTime, err := resetOffset.Spec.ParseSaramaOffsetTime()
	if err != nil {
		logger.Error("Failed to parse Sarama Offset Time from ResetOffset Spec", zap.Error(err))
		return err // Should never happen assuming Validation is in place
	}
	logger.Info("Successfully parsed Sarama Offset time from ResetOffset Spec", zap.Int64("Time (millis)", offsetTime))

	// Reconcile The DataPlane "Services" From The ConnectionPool For Specified Key
	dataPlaneServices, err := r.reconcileDataPlaneServices(ctx, resetOffset, refInfo)
	if err != nil {
		logger.Error("Failed to reconcile DataPlane services from ConnectionPool", zap.Error(err))
		// TODO - how to handle this - need another status condition and mark as failed?
	}
	logger.Info("Successfully reconciled DataPlane Services", zap.Any("Services", dataPlaneServiceIPs(dataPlaneServices)))

	// TODO - Send "Initiate" Message to Dispatcher Replicas to lock mutext and start process.
	resetOffset.Status.MarkResetInitiatedTrue()

	// Stop The ConsumerGroup In Associated Dispatchers
	err = r.stopConsumerGroups(ctx, resetOffset, dataPlaneServices, refInfo)
	if err != nil {
		logger.Error("Failed to stop ConsumerGroups", zap.Error(err))
		resetOffset.Status.MarkConsumerGroupsStoppedFailed("FailedToStopConsumerGroups", "Failed to stop one or more ConsumerGroups: %v", err)
		// TODO - should we re-reconcile and keep trying (stuck) or rollback here or in stopConsumerGroups() ???
		return nil // TODO - for now stopping without re-queueing
	}
	logger.Info("Successfully stopped ConsumerGroups")
	resetOffset.Status.MarkConsumerGroupsStoppedTrue()

	// Update The Sarama Offsets & Update ResetOffset CRD With OffsetMappings
	offsetMappings, err := r.reconcileOffsets(ctx, refInfo, offsetTime)
	if err != nil {
		logger.Error("Failed to update Offsets of one or more partitions", zap.Error(err))
		resetOffset.Status.MarkOffsetsUpdatedFailed("FailedToUpdateOffsets", "Failed to update Offsets of one or more Partitions: %v", err)
		return fmt.Errorf("failed to update offsets of one or more partitions: %v", err)
	}
	if offsetMappings != nil {
		resetOffset.Status.SetPartitions(offsetMappings)
	}
	logger.Info("Successfully updated Offsets of all partitions")
	resetOffset.Status.MarkOffsetsUpdatedTrue()

	// Start The ConsumerGroup In Associated Dispatchers
	err = r.startConsumerGroups(ctx, resetOffset, dataPlaneServices, refInfo)
	if err != nil {
		logger.Error("Failed to start ConsumerGroups", zap.Error(err))
		resetOffset.Status.MarkConsumerGroupsStartedFailed("FailedToStartConsumerGroups", "Failed to start one or more ConsumerGroups: %v", err)
		// TODO - should we re-reconcile and keep trying (stuck) or rollback here or in stopConsumerGroups() ???
		return nil // TODO - for now stopping without re-queueing
	}
	logger.Info("Successfully started ConsumerGroups")
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

	// TODO - need to r.asyncCommandNotificationStore.CleanPodNotification() in finalize? (see KafkaSource implementation for example?)

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

// dataPlaneServiceIPs returns the control-protocol Service IPs which are the keys in the specified map.
func dataPlaneServiceIPs(services map[string]control.Service) []string {
	i := 0
	ips := make([]string, len(services))
	for ip := range services {
		ips[i] = ip
		i++
	}
	return ips
}
