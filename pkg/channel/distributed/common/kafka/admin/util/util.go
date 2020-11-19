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

	"github.com/Shopify/sarama"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/constants"
)

// Constants
const (
	// Note - Update These When Sarama KErrors Change
	minKError = sarama.ErrUnknown
	maxKError = sarama.ErrFencedInstancedId
)

// Utility Function For Getting All (Limit 100) The Kafka Secrets In A K8S Namespace
func GetKafkaSecrets(ctx context.Context, k8sClient kubernetes.Interface, k8sNamespace string) (*corev1.SecretList, error) {
	return k8sClient.CoreV1().Secrets(k8sNamespace).List(ctx, metav1.ListOptions{
		LabelSelector: constants.KafkaSecretLabel + "=" + "true",
		Limit:         constants.MaxEventHubNamespaces,
	})
}

// Utility Function For Validating Kafka Secret
func ValidateKafkaSecret(logger *zap.Logger, secret *corev1.Secret) bool {

	// Assume Invalid Until Proven Otherwise
	valid := false

	// Validate The Kafka Secret
	if secret != nil {

		// Extract The Relevant Data From The Kafka Secret
		brokers := string(secret.Data[constants.KafkaSecretKeyBrokers])
		username := string(secret.Data[constants.KafkaSecretKeyUsername])
		password := string(secret.Data[constants.KafkaSecretKeyPassword])

		// Validate Kafka Secret Data (Allowing for Kafka not having Authentication enabled)
		if len(brokers) > 0 && len(username) >= 0 && len(password) >= 0 {

			// Mark Kafka Secret As Valid
			valid = true

		} else {

			// Invalid Kafka Secret - Log State
			pwdString := ""
			if len(password) > 0 {
				pwdString = "********"
			}
			logger.Error("Kafka Secret Contains Invalid Data",
				zap.String("Name", secret.Name),
				zap.String("Brokers", brokers),
				zap.String("Username", username),
				zap.String("Password", pwdString))
		}
	}

	// Return Kafka Secret Validity
	return valid
}

// Utility Function To Up-Convert Basic Errors Into TopicErrors (With Message Matching For Pertinent Errors)
func PromoteErrorToTopicError(err error) *sarama.TopicError {
	if err == nil {
		return nil
	} else {
		switch err := err.(type) {
		case *sarama.TopicError:
			return err
		default:
			for kError := minKError; kError <= maxKError; kError++ {
				if err.Error() == kError.Error() {
					return NewTopicError(kError, "Promoted To TopicError Based On Error Message Match")
				}
			}
			return NewTopicError(sarama.ErrUnknown, err.Error())
		}
	}
}

// Utility Function For Creating A New ErrUnknownTopicError With Specified Message
func NewUnknownTopicError(message string) *sarama.TopicError {
	return NewTopicError(sarama.ErrUnknown, message)
}

// Utility Function For Creating A Sarama TopicError With Specified Kafka Error Code (ErrNoError == Success)
func NewTopicError(kError sarama.KError, message string) *sarama.TopicError {
	return &sarama.TopicError{
		Err:    kError,
		ErrMsg: &message,
	}
}
