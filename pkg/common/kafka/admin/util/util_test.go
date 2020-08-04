package util

import (
	"errors"
	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"knative.dev/eventing-kafka/pkg/common/kafka/constants"
	"testing"
)

// Test The GetKafkaSecrets() Functionality
func TestGetKafkaSecrets(t *testing.T) {

	// Test Data
	k8sNamespace1 := "TestK8SNamespace1"
	k8sNamespace2 := "TestK8SNamespace2"

	kafkaSecretName1 := "TestKafkaSecretName1"
	kafkaSecretName2 := "TestKafkaSecretName2"
	kafkaSecretName3 := "TestKafkaSecretName3"

	kafkaSecret1 := createKafkaSecret(kafkaSecretName1, k8sNamespace1)
	kafkaSecret2 := createKafkaSecret(kafkaSecretName2, k8sNamespace1)
	kafkaSecret3 := createKafkaSecret(kafkaSecretName3, k8sNamespace2)

	// Get The Test K8S Client
	k8sClient := fake.NewSimpleClientset(kafkaSecret1, kafkaSecret2, kafkaSecret3)

	// Perform The Test
	kafkaSecretList, err := GetKafkaSecrets(k8sClient, k8sNamespace1)

	// Verify The Results
	assert.Nil(t, err)
	assert.NotNil(t, kafkaSecretList)
	assert.Len(t, kafkaSecretList.Items, 2)
	assert.Contains(t, kafkaSecretList.Items, *kafkaSecret1)
	assert.Contains(t, kafkaSecretList.Items, *kafkaSecret2)
}

// Test The PromoteErrorToTopicError() Functionality
func TestPromoteErrorToTopicError(t *testing.T) {

	// Test Data
	defaultErrMsg := "test default error"
	defaultErr := errors.New(defaultErrMsg)
	topicErrMsg := "test TopicError"
	topicErr := &sarama.TopicError{
		Err:    sarama.ErrInvalidConfig,
		ErrMsg: &topicErrMsg,
	}

	// Perform The Test (Both Cases)
	nilTopicError := PromoteErrorToTopicError(nil)
	defaultTopicError := PromoteErrorToTopicError(defaultErr)
	saramaTopicError := PromoteErrorToTopicError(topicErr)

	// Verify The Results
	assert.Nil(t, nilTopicError)
	assert.NotNil(t, defaultTopicError)
	assert.Equal(t, sarama.ErrUnknown, defaultTopicError.Err)
	assert.Equal(t, defaultErrMsg, *defaultTopicError.ErrMsg)
	assert.NotNil(t, saramaTopicError)
	assert.Equal(t, topicErr.Err, saramaTopicError.Err)
	assert.Equal(t, topicErrMsg, *saramaTopicError.ErrMsg)
}

// Test The NewUnknownTopicError() Functionality
func TestNewUnknownTopicError(t *testing.T) {

	// Test Data
	errMsg := "test error message"

	// Perform The Test
	topicError := NewUnknownTopicError(errMsg)

	// Verify The Results
	assert.NotNil(t, topicError)
	assert.Equal(t, sarama.ErrUnknown, topicError.Err)
	assert.Equal(t, errMsg, *topicError.ErrMsg)
}

// Test The NewTopicError() Functionality
func TestNewTopicError(t *testing.T) {

	// Test Data
	errMsg := "test error message"

	// Perform The Test
	topicError := NewTopicError(sarama.ErrInvalidConfig, errMsg)

	// Verify The Results
	assert.NotNil(t, topicError)
	assert.Equal(t, sarama.ErrInvalidConfig, topicError.Err)
	assert.Equal(t, errMsg, *topicError.ErrMsg)
}

//
// Utilities
//

// Create K8S Kafka Secret With Specified Config
func createKafkaSecret(name string, namespace string) *corev1.Secret {
	return &corev1.Secret{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Secret",
			APIVersion: corev1.SchemeGroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				constants.KafkaSecretLabel: "true",
			},
		},
	}
}
