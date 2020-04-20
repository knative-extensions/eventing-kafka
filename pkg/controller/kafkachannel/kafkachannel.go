package kafkachannel

import (
	"context"
	"fmt"
	"github.com/kyma-incubator/knative-kafka/pkg/controller/constants"
	"github.com/kyma-incubator/knative-kafka/pkg/controller/util"
	"go.uber.org/zap"
	kafkav1alpha1 "knative.dev/eventing-contrib/kafka/channel/pkg/apis/messaging/v1alpha1"
)

// Reconcile The KafkaChannel Itself (Add Labels, etc.)
func (r *Reconciler) reconcileKafkaChannel(_ context.Context, channel *kafkav1alpha1.KafkaChannel) error {

	// Get Channel Specific Logger
	logger := util.ChannelLogger(r.logger, channel)

	// Reconcile The KafkaChannel's Labels
	err := r.reconcileLabels(channel)
	if err != nil {
		logger.Error("Failed To Reconcile KafkaChannel Labels", zap.Error(err))
		return fmt.Errorf("failed to reconcile kafkachannel labels")
	} else {
		logger.Info("Successfully Reconciled KafkaChannel Labels")
		return nil // Success
	}
}

// Add Labels To KafkaChannel (Call After Channel Reconciliation !)
func (r *Reconciler) reconcileLabels(channel *kafkav1alpha1.KafkaChannel) error {

	// Get The KafkaChannel's Current Labels
	labels := channel.Labels

	// Initialize The Labels Map If Empty
	if labels == nil {
		labels = make(map[string]string)
	}

	// Track Modified Status
	modified := false

	// Add Kafka Topic Label If Missing
	topicName := util.TopicName(channel)
	if labels[constants.KafkaTopicLabel] != topicName {
		labels[constants.KafkaTopicLabel] = topicName
		modified = true
	}

	// Add Kafka Secret Label If Missing
	secretName := r.kafkaSecretName(channel) // Can Only Be Called AFTER Topic Reconciliation !!!
	if labels[constants.KafkaSecretLabel] != secretName {
		labels[constants.KafkaSecretLabel] = secretName
		modified = true
	}

	// If The KafkaChannel's Labels Were Modified
	if modified {

		// Then Update The KafkaChannel's Labels
		channel.Labels = labels
		_, err := r.kafkaClientSet.MessagingV1alpha1().KafkaChannels(channel.Namespace).Update(channel)
		if err != nil {
			r.logger.Error("Failed To Update KafkaChannel Labels", zap.Error(err))
			return err
		} else {
			r.logger.Info("Successfully Updated KafkaChannel Labels")
			return nil
		}
	} else {

		// Otherwise Nothing To Do
		r.logger.Info("Successfully Verified KafkaChannel Labels")
		return nil
	}
}
