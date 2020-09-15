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
		Name      string
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
		hash := GenerateHash(testCase.Name+testCase.Namespace, 8)
		truncateName := fmt.Sprintf("%.26s", testCase.Name)
		truncateNamespace := fmt.Sprintf("%.16s", testCase.Namespace)
		expectedResult := fmt.Sprintf("%s-%s-%s-dispatcher", truncateName, truncateNamespace, hash)

		// Verify The Results
		assert.Equal(t, expectedResult, actualResult)
	}
}

// Test The DispatcherDnsSafeName() Functionality
func TestDispatcherDnsSafeName_LongNamesDifferent(t *testing.T) {

	// This test ensures that the DnsSafeName for two channels with similar prefixes are, in fact, different

	// Define The TestCase Struct
	type TestCase struct {
		Name1      string
		Namespace1 string
		Name2      string
		Namespace2 string
	}

	// Create The TestCases
	testCases := []TestCase{
		{
			Name1:      channelName,
			Namespace1: channelNamespace,
			Name2:      channelName + "suffix1",
			Namespace2: channelNamespace + "suffix1",
		},
		{
			Name1:      "kafkachannel-kne-trigger-with-long-prefix-one",
			Namespace1: "test-broker-redelivery-kafka-channel-messaging-knative-one",
			Name2:      "kafkachannel-kne-trigger-with-long-prefix-two",
			Namespace2: "test-broker-redelivery-kafka-channel-messaging-knative-two",
		},
		{
			Name1:      "short-1",
			Namespace1: "kubernetes-maximum-length-for-namespace-with-sixty-three-char1",
			Name2:      "short-2",
			Namespace2: "kubernetes-maximum-length-for-namespace-with-sixty-three-char2",
		},
		{
			Name1:      "kubernetes-maximum-length-of-channel-name-is-sixty-three-char1",
			Namespace1: "short-1",
			Name2:      "kubernetes-maximum-length-of-channel-name-is-sixty-three-char2",
			Namespace2: "short-2",
		},
		{
			Name1:      "kubernetes-maximum-length-of-channel-name-is-sixty-three-char1",
			Namespace1: "kubernetes-maximum-length-for-namespace-with-sixty-three-char1",
			Name2:      "kubernetes-maximum-length-of-channel-name-is-sixty-three-char2",
			Namespace2: "kubernetes-maximum-length-for-namespace-with-sixty-three-char2",
		},
		{
			Name1:      "a-first",
			Namespace1: "b-first",
			Name2:      "a-second",
			Namespace2: "b-second",
		},
	}

	// Run The TestCases
	for _, testCase := range testCases {
		// Test Data
		channel1 := &kafkav1beta1.KafkaChannel{ObjectMeta: metav1.ObjectMeta{Name: testCase.Name1, Namespace: testCase.Namespace1}}
		channel2 := &kafkav1beta1.KafkaChannel{ObjectMeta: metav1.ObjectMeta{Name: testCase.Name2, Namespace: testCase.Namespace2}}

		// Perform The Test
		actualResult1 := DispatcherDnsSafeName(channel1)
		hash1 := GenerateHash(testCase.Name1+testCase.Namespace1, 8)
		truncateName1 := fmt.Sprintf("%.26s", testCase.Name1)
		truncateNamespace1 := fmt.Sprintf("%.16s", testCase.Namespace1)
		expectedResult1 := fmt.Sprintf("%s-%s-%s-dispatcher", truncateName1, truncateNamespace1, hash1)

		actualResult2 := DispatcherDnsSafeName(channel2)
		hash2 := GenerateHash(testCase.Name2+testCase.Namespace2, 8)
		truncateName2 := fmt.Sprintf("%.26s", testCase.Name2)
		truncateNamespace2 := fmt.Sprintf("%.16s", testCase.Namespace2)
		expectedResult2 := fmt.Sprintf("%s-%s-%s-dispatcher", truncateName2, truncateNamespace2, hash2)

		// Verify The Results
		assert.Equal(t, expectedResult1, actualResult1)
		assert.Equal(t, expectedResult2, actualResult2)
		assert.NotEqual(t, actualResult1, actualResult2)
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
