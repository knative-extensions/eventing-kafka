package test

import (
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kafkav1alpha1 "knative.dev/eventing-contrib/kafka/channel/pkg/apis/messaging/v1alpha1"
	eventingChannel "knative.dev/eventing/pkg/channel"
	knativeapis "knative.dev/pkg/apis"
	duckv1beta1 "knative.dev/pkg/apis/duck/v1beta1"
)

// Utility Function For Creating A Test ChannelReference (Knative)
func CreateChannelReference(name string, namespace string) eventingChannel.ChannelReference {
	return eventingChannel.ChannelReference{
		Name:      name,
		Namespace: namespace,
	}
}

// Utility Function For Creating A Test KafkaChannel (Knative-Kafka)
func CreateKafkaChannel(name string, namespace string, ready corev1.ConditionStatus) *kafkav1alpha1.KafkaChannel {
	return &kafkav1alpha1.KafkaChannel{
		TypeMeta: v1.TypeMeta{
			Kind:       "KafkaChannel",
			APIVersion: kafkav1alpha1.SchemeGroupVersion.String(),
		},
		ObjectMeta: v1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: kafkav1alpha1.KafkaChannelSpec{
			NumPartitions:     0,
			ReplicationFactor: 0,
			Subscribable:      nil,
		},
		Status: kafkav1alpha1.KafkaChannelStatus{
			Status: duckv1beta1.Status{
				Conditions: []knativeapis.Condition{
					{Type: knativeapis.ConditionReady, Status: ready},
				},
			},
		},
	}
}
