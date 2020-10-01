package util

import (
	kafkav1beta1 "knative.dev/eventing-kafka/pkg/apis/messaging/v1beta1"
	commonkafkautil "knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/util"
)

// Get The TopicName For Specified KafkaChannel (ChannelNamespace.ChannelName)
func TopicName(channel *kafkav1beta1.KafkaChannel) string {
	return commonkafkautil.TopicName(channel.Namespace, channel.Name)
}
