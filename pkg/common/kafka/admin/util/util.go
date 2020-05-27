package util

import (
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
