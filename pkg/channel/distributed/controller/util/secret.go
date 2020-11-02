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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/constants"
)

// Get A Logger With Secret Info
func SecretLogger(logger *zap.Logger, secret *corev1.Secret) *zap.Logger {
	return logger.With(zap.String("Secret", fmt.Sprintf("%s/%s", secret.Namespace, secret.Name)))
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
