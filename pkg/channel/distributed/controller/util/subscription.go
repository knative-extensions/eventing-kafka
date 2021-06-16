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
	"os"

	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	messagingv1 "knative.dev/eventing/pkg/apis/messaging/v1"
	"knative.dev/pkg/system"

	kafkav1beta1 "knative.dev/eventing-kafka/pkg/apis/messaging/v1beta1"
	commonkafkautil "knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/util"
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/constants"
)

// SubscriptionLogger returns a Logger with Subscription info.
func SubscriptionLogger(logger *zap.Logger, subscription *messagingv1.Subscription) *zap.Logger {
	return logger.With(zap.String("Subscription", fmt.Sprintf("%s/%s", subscription.Namespace, subscription.Name)))
}

// NewSubscriptionControllerRef creates a new Controller OwnerReference for the specified Knative Subscription.
func NewSubscriptionControllerRef(subscription *messagingv1.Subscription) metav1.OwnerReference {
	return *metav1.NewControllerRef(subscription, schema.GroupVersionKind{
		Group:   messagingv1.SchemeGroupVersion.Group,
		Version: messagingv1.SchemeGroupVersion.Version,
		Kind:    constants.KnativeSubscriptionKind,
	})
}

// TopicNameMapper returns a string representing the Kafka Topic name for the specified Knative Subscription.
func TopicNameMapper(subscription *messagingv1.Subscription) (string, error) {
	if subscription == nil {
		return "", fmt.Errorf("unable to format topic name for nil Subscription")
	}
	channelName := subscription.Spec.Channel.Name
	channelNamespace := subscription.Spec.Channel.Namespace
	if len(channelNamespace) <= 0 {
		channelNamespace = subscription.Namespace
	}
	return commonkafkautil.TopicName(channelNamespace, channelName), nil
}

// GroupIdMapper returns a string representing the Kafka ConsumerGroup ID for the specified Knative Subscription.
func GroupIdMapper(subscription *messagingv1.Subscription) (string, error) {
	if subscription == nil {
		return "", fmt.Errorf("unable to format group id for nil Subscription")
	}
	return commonkafkautil.GroupId(string(subscription.UID)), nil
}

// ConnectionPoolKeyMapper returns a string representing the control-protocol ControlPlaneConnectionPool Key for the specified Knative Subscription.
func ConnectionPoolKeyMapper(subscription *messagingv1.Subscription) (string, error) {
	return TopicNameMapper(subscription) // Distributed KafkaChannel is using the TopicName as ConnectionPool Key since it is 1:1 with KafkaChannel.
}

// DataPlaneNamespaceMapper returns the Kubernetes Namespace where the data-plane components
// (e.g. Dispatcher) are created by the distributed KafkaChannel controller.
func DataPlaneNamespaceMapper(_ *messagingv1.Subscription) (string, error) {
	systemNamespace := os.Getenv(system.NamespaceEnvKey)
	if systemNamespace == "" {
		systemNamespace = "knative-eventing"
	}
	return systemNamespace, nil
}

// DataPlaneLabelsMapper returns a map of Kubernetes Labels identifying the distributed
// KafkaChannels' Dispatcher pods.
func DataPlaneLabelsMapper(subscription *messagingv1.Subscription) (map[string]string, error) {

	channelName := subscription.Spec.Channel.Name
	channelNamespace := subscription.Spec.Channel.Namespace
	if channelNamespace == "" {
		channelNamespace = subscription.Namespace
	}

	sparseKafkaChannel := &kafkav1beta1.KafkaChannel{
		ObjectMeta: metav1.ObjectMeta{
			Name:      channelName,
			Namespace: channelNamespace,
		},
	}

	dispatcherAppLabelValue := DispatcherDnsSafeName(sparseKafkaChannel)

	labels := map[string]string{constants.AppLabel: dispatcherAppLabelValue}

	return labels, nil
}
