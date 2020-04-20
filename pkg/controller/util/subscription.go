package util

import (
	"github.com/kyma-incubator/knative-kafka/pkg/controller/constants"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	messagingv1alpha1 "knative.dev/eventing/pkg/apis/messaging/v1alpha1"
)

// Get A Logger With Subscription Info
func SubscriptionLogger(logger *zap.Logger, subscription *messagingv1alpha1.Subscription) *zap.Logger {
	return logger.With(zap.String("Namespace", subscription.Namespace), zap.String("Name", subscription.Name))
}

// Create A New ControllerReference Model For The Specified Subscription
func NewSubscriptionControllerRef(subscription *messagingv1alpha1.Subscription) metav1.OwnerReference {
	return *metav1.NewControllerRef(subscription, schema.GroupVersionKind{
		Group:   messagingv1alpha1.SchemeGroupVersion.Group,
		Version: messagingv1alpha1.SchemeGroupVersion.Version,
		Kind:    constants.KnativeSubscriptionKind,
	})
}
