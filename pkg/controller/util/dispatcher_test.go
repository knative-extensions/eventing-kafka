package util

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kafkav1alpha1 "knative.dev/eventing-contrib/kafka/channel/pkg/apis/messaging/v1alpha1"
	messagingv1alpha1 "knative.dev/eventing/pkg/apis/messaging/v1alpha1"
	"testing"
)

// Test The newControllerRef() Functionality
func TestNewControllerRef(t *testing.T) {

	// Test Data
	subscription := &messagingv1alpha1.Subscription{ObjectMeta: metav1.ObjectMeta{Name: channelName}}

	// Perform The Test
	actualControllerRef := NewSubscriptionControllerRef(subscription)

	// Verify The Results
	assert.NotNil(t, actualControllerRef)
	assert.Equal(t, messagingv1alpha1.SchemeGroupVersion.Group+"/"+messagingv1alpha1.SchemeGroupVersion.Version, actualControllerRef.APIVersion)
	assert.Equal(t, "Subscription", actualControllerRef.Kind)
	assert.Equal(t, channelName, actualControllerRef.Name)
}

// Test The DispatcherDnsSafeName() Functionality
func TestDispatcherDnsSafeName(t *testing.T) {

	// Test Data
	channel := &kafkav1alpha1.KafkaChannel{ObjectMeta: metav1.ObjectMeta{Name: channelName, Namespace: channelNamespace}}

	// Perform The Test
	actualResult := DispatcherDnsSafeName(channel)

	// Verify The Results
	expectedResult := fmt.Sprintf("%s-%s-dispatcher", channelName, channelNamespace)
	assert.Equal(t, expectedResult, actualResult)
}
