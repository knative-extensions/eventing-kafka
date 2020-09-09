package util

import (
	"fmt"
	"strings"

	"knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/constants"
)

// Get The Formatted Kafka Topic Name From The Specified Components
func TopicName(namespace string, name string) string {
	return fmt.Sprintf("%s.%s", namespace, name)
}

// Append The KafkaChannel Service Name Suffix To The Specified String
func AppendKafkaChannelServiceNameSuffix(channelName string) string {
	return fmt.Sprintf("%s-%s", channelName, constants.KafkaChannelServiceNameSuffix)
}

// Remove The KafkaChannel Service Name Suffix From The Specified String
func TrimKafkaChannelServiceNameSuffix(serviceName string) string {
	return strings.TrimSuffix(serviceName, "-"+constants.KafkaChannelServiceNameSuffix)
}
