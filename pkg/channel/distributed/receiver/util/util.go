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
	commonkafkautil "knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/util"
	eventingChannel "knative.dev/eventing/pkg/channel"
)

// Utility Function For Getting The Kafka Topic Name From The Specified ChannelReference
func TopicName(channelReference eventingChannel.ChannelReference) string {
	return commonkafkautil.TopicName(channelReference.Namespace, channelReference.Name)
}
