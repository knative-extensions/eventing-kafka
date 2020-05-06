package util

import (
	"github.com/stretchr/testify/assert"
	"knative.dev/eventing/pkg/channel"
	"testing"
)

// Test The TopicName() Functionality
func TestTopicName(t *testing.T) {

	// Test Data
	topicName := "TestTopicName"
	topicNamespace := "TestTopicNamespace"

	// The ChannelReference To Test
	channelReference := channel.ChannelReference{
		Name:      topicName,
		Namespace: topicNamespace,
	}

	// Perform The Test
	actualTopicName := TopicName(channelReference)

	// Validate The Results
	expectedTopicName := channelReference.Namespace + "." + channelReference.Name
	assert.Equal(t, expectedTopicName, actualTopicName)
}
