package util

import (
	"fmt"

	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	kafkav1beta1 "knative.dev/eventing-contrib/kafka/channel/pkg/apis/messaging/v1beta1"
	"knative.dev/eventing-kafka/pkg/common/config"
	"knative.dev/eventing-kafka/pkg/controller/constants"
	"knative.dev/eventing/pkg/utils"
)

// Get A Logger With Channel Info
func ChannelLogger(logger *zap.Logger, channel *kafkav1beta1.KafkaChannel) *zap.Logger {
	return logger.With(zap.String("Namespace", channel.Namespace), zap.String("Name", channel.Name))
}

// Create A Knative Reconciler "Key" Formatted Representation Of The Specified Channel
func ChannelKey(channel *kafkav1beta1.KafkaChannel) string {
	return fmt.Sprintf("%s/%s", channel.Namespace, channel.Name)
}

// Create A New OwnerReference For The Specified KafkaChannel (Controller)
func NewChannelOwnerReference(channel *kafkav1beta1.KafkaChannel) metav1.OwnerReference {

	kafkaChannelGroupVersion := schema.GroupVersion{
		Group:   kafkav1beta1.SchemeGroupVersion.Group,
		Version: kafkav1beta1.SchemeGroupVersion.Version,
	}

	blockOwnerDeletion := true
	controller := true

	return metav1.OwnerReference{
		APIVersion:         kafkaChannelGroupVersion.String(),
		Kind:               constants.KafkaChannelKind,
		Name:               channel.GetName(),
		UID:                channel.GetUID(),
		BlockOwnerDeletion: &blockOwnerDeletion,
		Controller:         &controller,
	}
}

//
// Create A DNS Safe Name For The Channel Deployment Using The Specified Kafka Secret
//
// Note - The current implementation creates a single Channel Deployment for each
//        Kafka Authentication (K8S Secrets) instance.
//
func ChannelDnsSafeName(kafkaSecretName string) string {

	// In order for the resulting name to be a valid DNS component it's length must be no more than 63 characters.
	// We are consuming 17 chars for the component separators, hash, and Channel suffix, which reduces the
	// available length to 46. We will allocate 41 characters to the kafka secret name leaving an extra buffer.
	safeSecretName := GenerateValidDnsName(kafkaSecretName, 41, true, false)

	return fmt.Sprintf("%s-%s-channel", safeSecretName, GenerateHash(kafkaSecretName, 8))
}

// Channel Host Naming Utility
func ChannelHostName(channelName, channelNamespace string) string {
	return fmt.Sprintf("%s.%s.channels.%s", channelName, channelNamespace, utils.GetClusterDomainName())
}

// Utility Function To Get The NumPartitions - First From Channel Spec And Then From ConfigMap-Provided Settings
func NumPartitions(channel *kafkav1beta1.KafkaChannel, configuration *config.EventingKafkaConfig, logger *zap.Logger) int32 {
	value := channel.Spec.NumPartitions
	if value <= 0 {
		logger.Debug("Kafka Channel Spec 'NumPartitions' Not Specified - Using Default", zap.Int32("Value", configuration.Kafka.Topic.DefaultNumPartitions))
		value = configuration.Kafka.Topic.DefaultNumPartitions
	}
	return value
}

// Utility Function To Get The ReplicationFactor - First From Channel Spec And Then From ConfigMap-Provided Settings
func ReplicationFactor(channel *kafkav1beta1.KafkaChannel, configuration *config.EventingKafkaConfig, logger *zap.Logger) int16 {
	value := channel.Spec.ReplicationFactor
	if value <= 0 {
		logger.Debug("Kafka Channel Spec 'ReplicationFactor' Not Specified - Using Default", zap.Int16("Value", configuration.Kafka.Topic.DefaultReplicationFactor))
		value = configuration.Kafka.Topic.DefaultReplicationFactor
	}
	return value
}

// Utility Function To Get The RetentionMillis - First From Channel Spec And Then From ConfigMap-Provided Settings
func RetentionMillis(channel *kafkav1beta1.KafkaChannel, configuration *config.EventingKafkaConfig, logger *zap.Logger) int64 {
	//
	// TODO - The eventing-contrib KafkaChannel CRD does not include RetentionMillis so we're
	//        currently just using the default value specified in Controller Environment Variables.
	//
	//value := channel.Spec.RetentionMillis
	//if value <= 0 {
	//	logger.Debug("Kafka Channel Spec 'RetentionMillis' Not Specified - Using Default", zap.Int64("Value", environment.DefaultRetentionMillis))
	//	value = environment.DefaultRetentionMillis
	//}
	//return value
	return configuration.Kafka.Topic.DefaultRetentionMillis
}
