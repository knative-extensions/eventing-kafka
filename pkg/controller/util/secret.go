package util

import (
	"github.com/kyma-incubator/knative-kafka/pkg/controller/constants"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// Get A Logger With Secret Info
func SecretLogger(logger *zap.Logger, secret *corev1.Secret) *zap.Logger {
	return logger.With(zap.String("Namespace", secret.Namespace), zap.String("Name", secret.Name))
}

// Create A New OwnerReference For The Specified K8S Secret (Controller)
func NewSecretOwnerReference(secret *corev1.Secret) metav1.OwnerReference {

	kafkaChannelGroupVersion := schema.GroupVersion{
		Group:   corev1.SchemeGroupVersion.Group,
		Version: corev1.SchemeGroupVersion.Version,
	}

	blockOwnerDeletion := true
	controller := true

	return metav1.OwnerReference{
		APIVersion:         kafkaChannelGroupVersion.String(),
		Kind:               constants.SecretKind,
		Name:               secret.GetName(),
		UID:                secret.GetUID(),
		BlockOwnerDeletion: &blockOwnerDeletion,
		Controller:         &controller,
	}
}
