package util

import (
	commonkafkautil "github.com/kyma-incubator/knative-kafka/pkg/common/kafka/util"
	eventingChannel "knative.dev/eventing/pkg/channel"
)

// Utility Function For Getting The Kafka Topic Name From The Specified ChannelReference
func TopicName(channelReference eventingChannel.ChannelReference) string {
	return commonkafkautil.TopicName(channelReference.Namespace, channelReference.Name)
}
