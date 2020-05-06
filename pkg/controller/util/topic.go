package util

import (
	commonkafkautil "knative.dev/eventing-kafka/pkg/common/kafka/util"
	kafkav1alpha1 "knative.dev/eventing-contrib/kafka/channel/pkg/apis/messaging/v1alpha1"
)

// Get The TopicName For Specified KafkaChannel (ChannelNamespace.ChannelName)
func TopicName(channel *kafkav1alpha1.KafkaChannel) string {
	return commonkafkautil.TopicName(channel.Namespace, channel.Name)
}
