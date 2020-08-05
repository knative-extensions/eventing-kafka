package util

import (
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"knative.dev/eventing-kafka/pkg/controller/constants"
	messagingv1 "knative.dev/eventing/pkg/apis/messaging/v1"
)

// Get A Logger With Subscription Info
func SubscriptionLogger(logger *zap.Logger, subscription *messagingv1.Subscription) *zap.Logger {
	return logger.With(zap.String("Namespace", subscription.Namespace), zap.String("Name", subscription.Name))
}

// Create A New ControllerReference Model For The Specified Subscription
func NewSubscriptionControllerRef(subscription *messagingv1.Subscription) metav1.OwnerReference {
	return *metav1.NewControllerRef(subscription, schema.GroupVersionKind{
		Group:   messagingv1.SchemeGroupVersion.Group,
		Version: messagingv1.SchemeGroupVersion.Version,
		Kind:    constants.KnativeSubscriptionKind,
	})
}
