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
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	eventingduck "knative.dev/eventing/pkg/apis/duck/v1"
	eventingChannel "knative.dev/eventing/pkg/channel"
	knativeapis "knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"

	kafkav1beta1 "knative.dev/eventing-kafka/pkg/apis/messaging/v1beta1"
)

// Utility Function For Creating A Test ChannelReference (Knative)
func CreateChannelReference(name string, namespace string) eventingChannel.ChannelReference {
	return eventingChannel.ChannelReference{
		Name:      name,
		Namespace: namespace,
	}
}

func CreateKafkaChannel(name string, namespace string, ready corev1.ConditionStatus) *kafkav1beta1.KafkaChannel {
	return &kafkav1beta1.KafkaChannel{
		TypeMeta: v1.TypeMeta{
			Kind:       "KafkaChannel",
			APIVersion: kafkav1beta1.SchemeGroupVersion.String(),
		},
		ObjectMeta: v1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: kafkav1beta1.KafkaChannelSpec{
			NumPartitions:     0,
			ReplicationFactor: 0,
			ChannelableSpec:   eventingduck.ChannelableSpec{},
		},
		Status: kafkav1beta1.KafkaChannelStatus{
			ChannelableStatus: eventingduck.ChannelableStatus{
				Status: duckv1.Status{
					Conditions: []knativeapis.Condition{
						{Type: kafkav1beta1.KafkaChannelConditionTopicReady, Status: ready},
						{Type: kafkav1beta1.KafkaChannelConditionReceiverServiceReady, Status: ready},
						{Type: kafkav1beta1.KafkaChannelConditionReceiverDeploymentReady, Status: ready},
					},
				},
				AddressStatus:      duckv1.AddressStatus{},
				SubscribableStatus: eventingduck.SubscribableStatus{},
			},
		},
	}
}
