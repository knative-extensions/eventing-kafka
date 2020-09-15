package util

import (
	"fmt"
	"testing"

	"crypto/md5"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kafkav1beta1 "knative.dev/eventing-contrib/kafka/channel/pkg/apis/messaging/v1beta1"
	messagingv1 "knative.dev/eventing/pkg/apis/messaging/v1"
)

// Test The newControllerRef() Functionality
func TestNewControllerRef(t *testing.T) {

	// Test Data
	subscription := &messagingv1.Subscription{ObjectMeta: metav1.ObjectMeta{Name: channelName}}

	// Perform The Test
	actualControllerRef := NewSubscriptionControllerRef(subscription)

	// Verify The Results
	assert.NotNil(t, actualControllerRef)
	assert.Equal(t, messagingv1.SchemeGroupVersion.Group+"/"+messagingv1.SchemeGroupVersion.Version, actualControllerRef.APIVersion)
	assert.Equal(t, "Subscription", actualControllerRef.Kind)
	assert.Equal(t, channelName, actualControllerRef.Name)
}

// Test The DispatcherDnsSafeName() Functionality
func TestDispatcherDnsSafeName(t *testing.T) {

	// Define The TestCase Struct
	type TestCase struct {
		Name   string
		Namespace string
	}

	// Create The TestCases
	testCases := []TestCase{
		{Name: channelName, Namespace: channelNamespace},
		{Name: "kafkachannel-kne-trigger", Namespace: "test-broker-redelivery-kafka-channel-messaging-knative-devg4l9d"},
		{Name: "short", Namespace: "kubernetes-maximum-length-for-namespace-with-sixty-three-chars"},
		{Name: "kubernetes-maximum-length-of-channel-name-is-sixty-three-chars", Namespace: "short"},
		{Name: "kubernetes-maximum-length-of-channel-name-is-sixty-three-chars", Namespace: "kubernetes-maximum-length-for-namespace-with-sixty-three-chars"},
		{Name: "a", Namespace: "b"},
	}

	// Run The TestCases
	for _, testCase := range testCases {
		// Test Data
		channel := &kafkav1beta1.KafkaChannel{ObjectMeta: metav1.ObjectMeta{Name: testCase.Name, Namespace: testCase.Namespace}}

		// Perform The Test
		actualResult := DispatcherDnsSafeName(channel)
		hashName := GenerateHash(testCase.Name, 4)
		hashNamespace := GenerateHash(testCase.Namespace, 4)
		truncateName := fmt.Sprintf("%.26s", testCase.Name)
		truncateNamespace := fmt.Sprintf("%.16s", testCase.Namespace)
		expectedResult := fmt.Sprintf("%s-%s-%s-dispatcher", truncateName, truncateNamespace, hashName + hashNamespace)

		// Verify The Results
		assert.Equal(t, expectedResult, actualResult)
	}
}

// Test the GenerateHash Functionality
func TestGenerateHash(t *testing.T) {
	// Define The TestCase Struct
	type TestCase struct {
		Name   string
		Length int
	}

	// Create The TestCases
	testCases := []TestCase{
		{Name: "", Length: 32},
		{Name: "short string", Length: 4},
		{Name: "long string, 8-character hash", Length: 8},
		{Name: "odd hash length, 13-characters", Length: 13},
		{Name: "very long string with 16-character hash and more than 64 characters total", Length: 16},
	}

	// Run The TestCases
	for _, testCase := range testCases {
		hash := GenerateHash(testCase.Name, testCase.Length)
		expected := fmt.Sprintf("%x", md5.Sum([]byte(testCase.Name)))[0:testCase.Length]
		assert.Equal(t, expected, hash)
	}

}
