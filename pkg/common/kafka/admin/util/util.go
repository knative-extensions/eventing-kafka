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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"knative.dev/eventing-kafka/pkg/common/kafka/constants"
)

// Utility Function For Getting All (Limit 100) The Kafka Secrets In A K8S Namespace
func GetKafkaSecrets(ctx context.Context, k8sClient kubernetes.Interface, k8sNamespace string) (*corev1.SecretList, error) {
	return k8sClient.CoreV1().Secrets(k8sNamespace).List(ctx, metav1.ListOptions{
		LabelSelector: constants.KafkaSecretLabel + "=" + "true",
		Limit:         constants.MaxEventHubNamespaces,
	})
}

// Utility Function To Up-Convert Any Basic Errors Into TopicErrors
func PromoteErrorToTopicError(err error) *sarama.TopicError {
	if err == nil {
		return nil
	} else {
		switch err := err.(type) {
		case *sarama.TopicError:
			return err
		default:
			return NewUnknownTopicError(err.Error())
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
