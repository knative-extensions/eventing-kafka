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
	"context"
	"fmt"

	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
	clientconstants "knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/constants"
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

//
// Temporary Kafka Secret Utilities
//
// The following logic used to live in distributed/common/kafka/ and was used by the various
// AdminClientInterface implementations to dynamically load "Kafka" Secrets by label.  That
// code has been refactored and no longer loads Kafka Secrets, instead relying on being passed
// a complete Sarama Config with any necessary auth included.
//
// This functionality has been temporarily placed here for use in the Controller until further
// refactoring takes place to remove the Kafka Secret labels, and instead rely upon the
// ConfigMap for identification of the Secret.
//

// Utility Function For Returning The Labelled Kafka Secret (Error If Not Exactly 1 Kafka Secret)
func GetKafkaSecret(ctx context.Context, k8sClient kubernetes.Interface, k8sNamespace string) (*corev1.Secret, error) {

	// Get A List Of "Kafka" Secrets
	secretList, err := k8sClient.CoreV1().Secrets(k8sNamespace).List(ctx, metav1.ListOptions{LabelSelector: clientconstants.KafkaSecretLabel + "=" + "true"})
	if err != nil {
		return nil, err
	} else if secretList == nil || len(secretList.Items) < 1 {
		return nil, nil
	} else if len(secretList.Items) > 1 {
		return nil, fmt.Errorf("found multiple kafka secrets - only one is allowed")
	} else {
		return &secretList.Items[0], nil
	}
}

// Utility Function For Validating Kafka Secret
func ValidateKafkaSecret(logger *zap.Logger, secret *corev1.Secret) bool {

	// Assume Invalid Until Proven Otherwise
	valid := false

	// Validate The Kafka Secret
	if secret != nil {

		// Extract The Relevant Data From The Kafka Secret
		brokers := string(secret.Data[clientconstants.KafkaSecretKeyBrokers])

		// Validate Kafka Secret Data
		if len(brokers) > 0 {

			// Mark Kafka Secret As Valid
			valid = true

		} else {

			// Invalid Kafka Secret - Log State
			logger.Error("Invalid Kafka Secret - Missing Brokers", zap.String("Secret", fmt.Sprintf("%s/%s", secret.Namespace, secret.Name)))
		}
	}

	// Return Kafka Secret Validity
	return valid
}
