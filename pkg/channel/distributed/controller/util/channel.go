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
	"knative.dev/pkg/network"

	kafkav1beta1 "knative.dev/eventing-kafka/pkg/apis/messaging/v1beta1"
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/constants"
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
func ReceiverDnsSafeName(kafkaSecretName string) string {

	// In order for the resulting name to be a valid DNS component it's length must be no more than 63 characters.
	// We are consuming 18 chars for the component separators, hash, and Receiver suffix, which reduces the
	// available length to 45. We will allocate 40 characters to the kafka secret name leaving an extra buffer.
	safeSecretName := GenerateValidDnsName(kafkaSecretName, 40, true, false)

	return fmt.Sprintf("%s-%s-receiver", safeSecretName, GenerateHash(kafkaSecretName, 8))
}

// ChannelHostName Creates A Name For The Channel Host
func ChannelHostName(channelName, channelNamespace string) string {
	return fmt.Sprintf("%s.%s.channels.%s", channelName, channelNamespace, network.GetClusterDomainName())
}
