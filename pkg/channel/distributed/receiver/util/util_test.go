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
	"testing"

	"github.com/stretchr/testify/assert"
	"knative.dev/eventing/pkg/channel"
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
