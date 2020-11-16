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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"knative.dev/eventing-kafka/pkg/common/kafka/constants"
)

// Test The TopicName() Functionality
func TestTopicName(t *testing.T) {

	// Test Data
	name := "TestName"
	namespace := "TestNamespace"

	// Perform The Test
	actualTopicName := TopicName(namespace, name)

	// Verify The Results
	expectedTopicName := namespace + "." + name
	assert.Equal(t, expectedTopicName, actualTopicName)
}

// Test The AppendChannelServiceNameSuffix() Functionality
func TestAppendChannelServiceNameSuffix(t *testing.T) {

	// Test Data
	channelName := "TestChannelName"

	// Perform The Test
	actualResult := AppendKafkaChannelServiceNameSuffix(channelName)

	// Verify The Results
	expectedResult := fmt.Sprintf("%s-%s", channelName, constants.KafkaChannelServiceNameSuffix)
	assert.Equal(t, expectedResult, actualResult)
}

// Test The TrimKafkaChannelServiceNameSuffix() Functionality
func TestTrimKafkaChannelServiceNameSuffix(t *testing.T) {

	// Test Data
	channelName := "TestChannelName"
	channelServiceName := fmt.Sprintf("%s-%s", channelName, constants.KafkaChannelServiceNameSuffix)

	// Perform The Test
	actualResult := TrimKafkaChannelServiceNameSuffix(channelServiceName)

	// Verify The Results
	expectedResult := channelName
	assert.Equal(t, expectedResult, actualResult)
}
