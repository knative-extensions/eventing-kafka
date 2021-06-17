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

package kafkachannel

import (
	"context"
	"fmt"
	"strconv"

	"github.com/Shopify/sarama"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"

	kafkav1beta1 "knative.dev/eventing-kafka/pkg/apis/messaging/v1beta1"
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/constants"
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/event"
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/util"
	"knative.dev/eventing-kafka/pkg/common/config"
	commonconstants "knative.dev/eventing-kafka/pkg/common/constants"
)

// reconcileKafkaTopic Reconciles The Kafka Topic Associated With The Specified Channel
func (r *Reconciler) reconcileKafkaTopic(ctx context.Context, channel *kafkav1beta1.KafkaChannel) error {

	// Get The TopicName For Specified Channel
	topicName := util.TopicName(channel)

	// Get Channel-Specific Logger (From The Context) & Add Topic Name
	logger := logging.FromContext(ctx).With(zap.String("TopicName", topicName))

	// Get The Topic Configuration (First From Channel With Failover To Environment)
	numPartitions := config.NumPartitions(channel, r.config, logger)
	replicationFactor := config.ReplicationFactor(channel, r.config, logger)

	retentionMillis := config.RetentionMillis(channel, r.config, logger)

	// Create The Topic (Handles Case Where Already Exists)
	err := r.createTopic(ctx, topicName, numPartitions, replicationFactor, retentionMillis)

	// Log Results & Return Status
	if err != nil {
		controller.GetEventRecorder(ctx).Eventf(channel, corev1.EventTypeWarning, event.KafkaTopicReconciliationFailed.String(), "Failed To Reconcile Kafka Topic For Channel: %v", err)
		logger.Error("Failed To Reconcile Kafka Topic", zap.Error(err))
		channel.Status.MarkTopicFailed("TopicFailed", fmt.Sprintf("Channel Kafka Topic Failed: %s", err))
	} else {
		logger.Info("Successfully Reconciled Kafka Topic")
		channel.Status.MarkTopicTrue()
	}
	return err
}

// finalizeKafkaTopic Finalizes The Kafka Topic Associated With The Specified Channel
func (r *Reconciler) finalizeKafkaTopic(ctx context.Context, channel *kafkav1beta1.KafkaChannel) error {

	// Get The TopicName For Specified Channel
	topicName := util.TopicName(channel)

	// Get Channel Specific Logger (Provided Via Context) & Add Topic Name
	logger := logging.FromContext(ctx).Desugar().With(zap.String("TopicName", topicName))

	// Delete The Kafka Topic & Handle Error Response
	err := r.deleteTopic(ctx, topicName)
	if err != nil {
		logger.Error("Failed To Finalize Kafka Topic", zap.Error(err))
		return err
	} else {
		logger.Info("Successfully Finalized Kafka Topic")
		return nil
	}
}

// createTopic Creates The Specified Kafka Topic
func (r *Reconciler) createTopic(ctx context.Context, topicName string, partitions int32, replicationFactor int16, retentionMillis int64) error {

	// Get The Logger From The Context
	logger := logging.FromContext(ctx)

	// Create The TopicDefinition
	retentionMillisString := strconv.FormatInt(retentionMillis, 10)
	topicDetail := &sarama.TopicDetail{
		NumPartitions:     partitions,
		ReplicationFactor: replicationFactor,
		ReplicaAssignment: nil, // Currently Not Assigning Partitions To Replicas
		ConfigEntries: map[string]*string{
			commonconstants.KafkaTopicConfigRetentionMs: &retentionMillisString,
		},
	}

	// Attempt To Create The Topic & Process TopicError Results (Including Success ;)
	err := r.adminClient.CreateTopic(ctx, topicName, topicDetail)
	if err != nil {
		logger := logger.With(zap.Int16("KError", int16(err.Err)))
		switch err.Err {
		case sarama.ErrNoError:
			logger.Info("Successfully Created New Kafka Topic (ErrNoError)")
			return nil
		case sarama.ErrTopicAlreadyExists:
			logger.Info("Kafka Topic Already Exists - No Creation Required")
			return nil
		default:
			logger.Error("Failed To Create Topic")
			return err
		}
	} else {
		logger.Info("Successfully Created New Kafka Topic (Nil TopicError)")
		return nil
	}
}

// deleteTopic Deletes The Specified Kafka Topic
func (r *Reconciler) deleteTopic(ctx context.Context, topicName string) error {

	// Get The Logger From The Context
	logger := logging.FromContext(ctx)

	// Attempt To Delete The Topic & Process Results
	err := r.adminClient.DeleteTopic(ctx, topicName)
	if err != nil {
		logger := logger.With(zap.Int16("KError", int16(err.Err)))
		switch err.Err {
		case sarama.ErrNoError:
			logger.Info("Successfully Deleted Existing Kafka Topic (ErrNoError)")
			return nil
		case sarama.ErrUnknownTopicOrPartition, sarama.ErrInvalidTopic, sarama.ErrInvalidPartitions:
			logger.Info("Kafka Topic or Partition Not Found - No Deletion Required")
			return nil
		case sarama.ErrInvalidConfig:
			if r.config.Channel.AdminType == constants.KafkaAdminTypeValueAzure {
				// While this could be a valid Kafka error, this most likely is coming from our custom EventHub AdminClient
				// implementation and represents the fact that the EventHub Cache does not contain this topic.  This can
				// happen when an EventHub could not be created due to exceeding the number of allowable EventHubs.  The
				// KafkaChannel is then in an "UNKNOWN" state having never been fully reconciled.  We want to swallow this
				// error here so that the deletion of the Topic / EventHub doesn't block the deletion of the KafkaChannel.
				logger.Warn("Unable To Delete Topic Due To Invalid Kafka Topic Config (Likely EventHub Namespace Cache)")
				return nil
			} else {
				logger.Error("Failed To Delete Topic Due To Invalid Config")
				return err
			}
		default:
			logger.Error("Failed To Delete Topic")
			return err
		}
	} else {
		logger.Info("Successfully Deleted Existing Kafka Topic (Nil TopicError)")
		return nil
	}
}
