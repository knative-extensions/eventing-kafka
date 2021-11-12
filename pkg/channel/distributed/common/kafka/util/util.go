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
	"strings"

	"k8s.io/apimachinery/pkg/types"

	"knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/constants"
)

const GroupIdPrefix = "kafka"

// TopicName returns a formatted string representing the Kafka Topic name.
func TopicName(namespace string, name string) string {
	return fmt.Sprintf("%s.%s", namespace, name)
}

// GroupId returns a formatted string representing the Kafka ConsumerGroup ID.
func GroupId(uid string) string {
	return fmt.Sprintf("%s.%s", GroupIdPrefix, uid)
}

// Uid returns a UID from the specified GroupId, which is the inverse of GroupId().
func Uid(groupId string) types.UID {
	return types.UID(strings.TrimPrefix(groupId, GroupIdPrefix+"."))
}

// AppendKafkaChannelServiceNameSuffix appends the KafkaChannel Service name suffix to the specified string.
func AppendKafkaChannelServiceNameSuffix(channelName string) string {
	return fmt.Sprintf("%s-%s", channelName, constants.KafkaChannelServiceNameSuffix)
}

// TrimKafkaChannelServiceNameSuffix removes the KafkaChannel Service name suffix from the specified string.
func TrimKafkaChannelServiceNameSuffix(serviceName string) string {
	return strings.TrimSuffix(serviceName, "-"+constants.KafkaChannelServiceNameSuffix)
}
