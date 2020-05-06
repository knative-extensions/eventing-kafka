package util

import (
	commonkafkautil "knative.dev/eventing-kafka/pkg/common/kafka/util"
	eventingChannel "knative.dev/eventing/pkg/channel"
)

// Utility Function For Getting The Kafka Topic Name From The Specified ChannelReference
func TopicName(channelReference eventingChannel.ChannelReference) string {
	return commonkafkautil.TopicName(channelReference.Namespace, channelReference.Name)
}
