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
