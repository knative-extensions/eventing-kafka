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

package util

import (
	"fmt"

	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	kafkav1beta1 "knative.dev/eventing-kafka/pkg/apis/messaging/v1beta1"
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/constants"
	commonconfig "knative.dev/eventing-kafka/pkg/common/config"
	"knative.dev/pkg/network"
)

// ChannelLogger Gets A Logger With Channel Info
func ChannelLogger(logger *zap.Logger, channel *kafkav1beta1.KafkaChannel) *zap.Logger {
	return logger.With(zap.String("Channel", fmt.Sprintf("%s/%s", channel.Namespace, channel.Name)))
}

// ChannelKey Creates A Knative Reconciler "Key" Formatted Representation Of The Specified Channel
func ChannelKey(channel *kafkav1beta1.KafkaChannel) string {
	return fmt.Sprintf("%s/%s", channel.Namespace, channel.Name)
}

// NewChannelOwnerReference Creates A New OwnerReference For The Specified KafkaChannel (Controller)
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
// ReceiverDnsSafeName Creates A DNS Safe Name For The Receiver Deployment Using The Specified Kafka Secret
//
// Note - The current implementation creates a single Receiver Deployment for each
//        Kafka Authentication (K8S Secrets) instance.
//
func ReceiverDnsSafeName(prefix string) string {

	// In order for the resulting name to be a valid DNS component it's length must be no more than 63 characters.
	// We are consuming 9 chars for the component separator and Receiver suffix, which reduces the
	// available length to 54. We will allocate 49 characters to the prefix leaving an extra buffer.
	return GenerateValidDnsName(prefix, 49, true, false) + "-receiver"
}

// ChannelHostName Creates A Name For The Channel Host
func ChannelHostName(channelName, channelNamespace string) string {
	return fmt.Sprintf("%s.%s.channels.%s", channelName, channelNamespace, network.GetClusterDomainName())
}

// NumPartitions Gets The NumPartitions - First From Channel Spec And Then From ConfigMap-Provided Settings
func NumPartitions(channel *kafkav1beta1.KafkaChannel, configuration *commonconfig.EventingKafkaConfig, logger *zap.Logger) int32 {
	value := channel.Spec.NumPartitions
	if value <= 0 {
		logger.Debug("Kafka Channel Spec 'NumPartitions' Not Specified - Using Default", zap.Int32("Value", configuration.Kafka.Topic.DefaultNumPartitions))
		value = configuration.Kafka.Topic.DefaultNumPartitions
	}
	return value
}

// ReplicationFactor Gets The ReplicationFactor - First From Channel Spec And Then From ConfigMap-Provided Settings
func ReplicationFactor(channel *kafkav1beta1.KafkaChannel, configuration *commonconfig.EventingKafkaConfig, logger *zap.Logger) int16 {
	value := channel.Spec.ReplicationFactor
	if value <= 0 {
		logger.Debug("Kafka Channel Spec 'ReplicationFactor' Not Specified - Using Default", zap.Int16("Value", configuration.Kafka.Topic.DefaultReplicationFactor))
		value = configuration.Kafka.Topic.DefaultReplicationFactor
	}
	return value
}

// RetentionMillis Gets The RetentionMillis - First From Channel Spec And Then From ConfigMap-Provided Settings
func RetentionMillis(channel *kafkav1beta1.KafkaChannel, configuration *commonconfig.EventingKafkaConfig, logger *zap.Logger) int64 {
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
