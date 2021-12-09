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

package testing

import (
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	eventingduck "knative.dev/eventing/pkg/apis/duck/v1"
	"knative.dev/pkg/apis"

	"knative.dev/eventing-kafka/pkg/apis/messaging/v1beta1"
)

// KafkaChannelOption enables further configuration of a KafkaChannel.
type KafkaChannelOption func(*v1beta1.KafkaChannel)

// NewKafkaChannel creates an KafkaChannel with KafkaChannelOptions.
func NewKafkaChannel(name string, namespace string, options ...KafkaChannelOption) *v1beta1.KafkaChannel {
	kafkachannel := &v1beta1.KafkaChannel{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: v1beta1.KafkaChannelSpec{},
	}
	for _, opt := range options {
		opt(kafkachannel)
	}
	return kafkachannel
}

func WithInitKafkaChannelConditions(kafkachannel *v1beta1.KafkaChannel) {
	kafkachannel.Status.InitializeConditions()
}

func WithKafkaChannelReady(kafkachannel *v1beta1.KafkaChannel) {
	kafkachannel.Status.MarkConfigTrue()
	kafkachannel.Status.MarkTopicTrue()
	kafkachannel.Status.MarkChannelServiceTrue()
	kafkachannel.Status.MarkReceiverServiceTrue()
	kafkachannel.Status.MarkReceiverDeploymentTrue()
	kafkachannel.Status.MarkDispatcherServiceTrue()
	kafkachannel.Status.PropagateDispatcherDeploymentStatus(&appsv1.DeploymentStatus{
		Conditions: []appsv1.DeploymentCondition{
			{
				Type:   appsv1.DeploymentAvailable,
				Status: corev1.ConditionTrue,
			},
		},
	})
}

func WithKafkaChannelAddress(a string) KafkaChannelOption {
	return func(kafkachannel *v1beta1.KafkaChannel) {
		kafkachannel.Status.SetAddress(&apis.URL{
			Scheme: "http",
			Host:   a,
		})
	}
}

func WithSubscriber(uid types.UID, uri string) KafkaChannelOption {
	return func(kafkachannel *v1beta1.KafkaChannel) {
		if kafkachannel.Spec.Subscribers == nil {
			kafkachannel.Spec.Subscribers = []eventingduck.SubscriberSpec{}
		}
		kafkachannel.Spec.Subscribers = append(kafkachannel.Spec.Subscribers, eventingduck.SubscriberSpec{
			UID: uid,
			SubscriberURI: &apis.URL{
				Scheme: "http",
				Host:   uri,
			},
		})
	}
}

func WithSubscriberReady(uid types.UID) KafkaChannelOption {
	return func(kafkachannel *v1beta1.KafkaChannel) {
		if kafkachannel.Status.SubscribableStatus.Subscribers == nil {
			kafkachannel.Status.SubscribableStatus.Subscribers = []eventingduck.SubscriberStatus{}
		}
		kafkachannel.Status.SubscribableStatus.Subscribers = append(kafkachannel.Status.SubscribableStatus.Subscribers, eventingduck.SubscriberStatus{
			Ready: corev1.ConditionTrue,
			UID:   uid,
		})
	}
}

func WithSubscriberNotReady(uid types.UID, message string) KafkaChannelOption {
	return func(kafkachannel *v1beta1.KafkaChannel) {
		if kafkachannel.Status.SubscribableStatus.Subscribers == nil {
			kafkachannel.Status.SubscribableStatus.Subscribers = []eventingduck.SubscriberStatus{}
		}
		kafkachannel.Status.SubscribableStatus.Subscribers = append(kafkachannel.Status.SubscribableStatus.Subscribers, eventingduck.SubscriberStatus{
			Ready:   corev1.ConditionFalse,
			UID:     uid,
			Message: message,
		})
	}
}
