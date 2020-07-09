package util

import (
	"github.com/Shopify/sarama"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"knative.dev/eventing-kafka/pkg/common/kafka/constants"
)

// Utility Function For Getting All (Limit 100) The Kafka Secrets In A K8S Namespace
func GetKafkaSecrets(k8sClient kubernetes.Interface, k8sNamespace string) (*corev1.SecretList, error) {
	return k8sClient.CoreV1().Secrets(k8sNamespace).List(metav1.ListOptions{
		LabelSelector: constants.KafkaSecretLabel + "=" + "true",
		Limit:         constants.MaxEventHubNamespaces,
	})
}

// Utility Function To Up-Convert Any Basic Errors Into TopicErrors
func PromoteErrorToTopicError(err error) *sarama.TopicError {
	if err == nil {
		return nil
	} else {
		switch err.(type) {
		case *sarama.TopicError:
			return err.(*sarama.TopicError)
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
