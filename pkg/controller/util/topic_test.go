package util

import (
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kafkav1beta1 "knative.dev/eventing-contrib/kafka/channel/pkg/apis/messaging/v1beta1"
	"testing"
)

// Test The TopicName() Functionality
func TestTopicName(t *testing.T) {

	// Test Constants
	const (
		channelName      = "TestChannelName"
		channelNamespace = "TestChannelNamespace"
	)

	// The KafkaChannel To Test
	channel := &kafkav1beta1.KafkaChannel{
		ObjectMeta: metav1.ObjectMeta{Name: channelName, Namespace: channelNamespace},
	}

	// Perform The Test
	actualTopicName := TopicName(channel)

	// Verify The Results
	expectedTopicName := channelNamespace + "." + channelName
	assert.Equal(t, expectedTopicName, actualTopicName)
}
