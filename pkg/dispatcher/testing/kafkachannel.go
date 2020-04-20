package testing

import (
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"knative.dev/eventing-contrib/kafka/channel/pkg/apis/messaging/v1alpha1"
	eventingduck "knative.dev/eventing/pkg/apis/duck/v1alpha1"
	"knative.dev/pkg/apis"
)

// KafkaChannelOption enables further configuration of a KafkaChannel.
type KafkaChannelOption func(*v1alpha1.KafkaChannel)

// NewKafkaChannel creates an KafkaChannel with KafkaChannelOptions.
func NewKafkaChannel(name, namespace string, ncopt ...KafkaChannelOption) *v1alpha1.KafkaChannel {
	nc := &v1alpha1.KafkaChannel{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: v1alpha1.KafkaChannelSpec{},
	}
	for _, opt := range ncopt {
		opt(nc)
	}
	return nc
}

func WithInitKafkaChannelConditions(nc *v1alpha1.KafkaChannel) {
	nc.Status.InitializeConditions()
}

func WithKafkaChannelReady(nc *v1alpha1.KafkaChannel) {
	nc.Status.MarkConfigTrue()
	nc.Status.MarkTopicTrue()
	nc.Status.MarkChannelServiceTrue()
	nc.Status.MarkServiceTrue()
	nc.Status.MarkEndpointsTrue()
	nc.Status.PropagateDispatcherStatus(&appsv1.DeploymentStatus{
		Conditions: []appsv1.DeploymentCondition{
			{
				Type:   appsv1.DeploymentAvailable,
				Status: corev1.ConditionTrue,
			},
		},
	})
}

func WithKafkaChannelAddress(a string) KafkaChannelOption {
	return func(nc *v1alpha1.KafkaChannel) {
		nc.Status.SetAddress(&apis.URL{
			Scheme: "http",
			Host:   a,
		})
	}
}

func WithSubscriber(uid types.UID, uri string) KafkaChannelOption {
	return func(nc *v1alpha1.KafkaChannel) {
		if nc.Spec.Subscribable == nil {
			nc.Spec.Subscribable = &eventingduck.Subscribable{}
		}

		nc.Spec.Subscribable.Subscribers = append(nc.Spec.Subscribable.Subscribers, eventingduck.SubscriberSpec{
			UID: uid,
			SubscriberURI: &apis.URL{
				Scheme: "http",
				Host:   uri,
			},
		})
	}
}

func WithSubscriberReady(uid types.UID) KafkaChannelOption {
	return func(nc *v1alpha1.KafkaChannel) {
		if nc.Status.SubscribableStatus == nil {
			nc.Status.SubscribableStatus = &eventingduck.SubscribableStatus{}
		}

		nc.Status.SubscribableStatus.Subscribers = append(nc.Status.SubscribableStatus.Subscribers, eventingduck.SubscriberStatus{
			Ready: corev1.ConditionTrue,
			UID:   uid,
		})
	}
}
