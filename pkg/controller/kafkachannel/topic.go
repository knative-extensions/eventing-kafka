package kafkachannel

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	kafkav1alpha1 "knative.dev/eventing-contrib/kafka/channel/pkg/apis/messaging/v1alpha1"
	"knative.dev/eventing-kafka/pkg/controller/constants"
	"knative.dev/eventing-kafka/pkg/controller/env"
	"knative.dev/eventing-kafka/pkg/controller/event"
	"knative.dev/eventing-kafka/pkg/controller/util"
	"knative.dev/pkg/controller"
	"strconv"
)

// Reconcile The Kafka Topic Associated With The Specified Channel & Return The Kafka Secret
func (r *Reconciler) reconcileTopic(ctx context.Context, channel *kafkav1alpha1.KafkaChannel) error {

	// Get The TopicName For Specified Channel
	topicName := util.TopicName(channel)

	// Get Channel Specific Logger & Add Topic Name
	logger := util.ChannelLogger(r.logger, channel).With(zap.String("TopicName", topicName))

	// Get The Topic Configuration (First From Channel With Failover To Environment)
	numPartitions := util.NumPartitions(channel, r.environment, r.logger)
	replicationFactor := util.ReplicationFactor(channel, r.environment, r.logger)
	retentionMillis := util.RetentionMillis(channel, r.environment, r.logger)

	// Create The Topic (Handles Case Where Already Exists)
	err := r.createTopic(ctx, topicName, numPartitions, replicationFactor, retentionMillis)

	// Log Results & Return Status
	if err != nil {
		controller.GetEventRecorder(ctx).Eventf(channel, corev1.EventTypeWarning, event.KafkaTopicReconciliationFailed.String(), "Failed To Reconcile Kafka Topic For Channel: %v", err)
		logger.Error("Failed To Reconcile Topic", zap.Error(err))
		channel.Status.MarkTopicFailed("TopicFailed", fmt.Sprintf("Channel Kafka Topic Failed: %s", err))
	} else {
		logger.Info("Successfully Reconciled Topic")
		channel.Status.MarkTopicTrue()
	}
	return err
}

// Create The Specified Kafka Topic
func (r *Reconciler) createTopic(ctx context.Context, topicName string, partitions int32, replicationFactor int16, retentionMillis int64) error {

	// Setup The Logger
	logger := r.logger.With(zap.String("Topic", topicName))

	// Create The TopicDefinition
	retentionMillisString := strconv.FormatInt(retentionMillis, 10)
	topicDetail := &sarama.TopicDetail{
		NumPartitions:     partitions,
		ReplicationFactor: replicationFactor,
		ReplicaAssignment: nil,
		ConfigEntries: map[string]*string{
			constants.KafkaTopicConfigRetentionMs: &retentionMillisString,
		},
	}

	// Attempt To Create The Topic & Process TopicError Results (Including Success ;)
	err := r.adminClient.CreateTopic(ctx, topicName, topicDetail)
	if err != nil {
		switch err.Err {
		case sarama.ErrNoError:
			logger.Info("Successfully Created New Kafka Topic (ErrNoError)")
			return nil
		case sarama.ErrTopicAlreadyExists:
			logger.Info("Kafka Topic Already Exists - No Creation Required")
			return nil
		default:
			logger.Error("Failed To Create Topic", zap.Any("TopicError", err))
			return err
		}
	} else {
		logger.Info("Successfully Created Existing Kafka Topic (Nil TopicError)")
		return nil
	}
}

// Delete The Specified Kafka Topic
func (r *Reconciler) deleteTopic(ctx context.Context, topicName string) error {

	// Setup The Logger
	logger := r.logger.With(zap.String("Topic", topicName))

	// Attempt To Delete The Topic & Process Results
	err := r.adminClient.DeleteTopic(ctx, topicName)
	if err != nil {
		switch err.Err {
		case sarama.ErrNoError:
			logger.Info("Successfully Deleted Existing Kafka Topic (ErrNoError)")
			return nil
		case sarama.ErrUnknownTopicOrPartition, sarama.ErrInvalidTopic, sarama.ErrInvalidPartitions:
			logger.Info("Kafka Topic or Partition Not Found - No Deletion Required")
			return nil
		case sarama.ErrInvalidConfig:
			if r.environment.KafkaProvider == env.KafkaProviderValueAzure {
				// While this could be a valid Kafka error, this most likely is coming from our custom EventHub AdminClient
				// implementation and represents the fact that the EventHub Cache does not contain this topic.  This can
				// happen when an EventHub could not be created due to exceeding the number of allowable EventHubs.  The
				// KafkaChannel is then in an "UNKNOWN" state having never been fully reconciled.  We want to swallow this
				// error here so that the deletion of the Topic / EventHub doesn't block the deletion of the KafkaChannel.
				logger.Warn("Unable To Delete Topic Due To Invalid Kafka Topic Config (Likely EventHub Namespace Cache)", zap.Error(err))
				return nil
			} else {
				logger.Error("Failed To Delete Topic Due To Invalid Config", zap.Any("TopicError", err))
				return err
			}
		default:
			logger.Error("Failed To Delete Topic", zap.Any("TopicError", err))
			return err
		}
	} else {
		logger.Info("Successfully Deleted Existing Kafka Topic (Nil TopicError)")
		return nil
	}
}
