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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kafkav1beta1 "knative.dev/eventing-kafka/pkg/apis/messaging/v1beta1"
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
