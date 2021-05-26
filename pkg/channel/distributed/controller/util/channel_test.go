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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	logtesting "knative.dev/pkg/logging/testing"

	kafkav1beta1 "knative.dev/eventing-kafka/pkg/apis/messaging/v1beta1"
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/constants"
)

// Test Data
const (
	testPrefix       = "testkafkaprefix"
	channelName      = "testname"
	channelNamespace = "testnamespace"
)

// Test The ChannelLogger() Functionality
func TestChannelLogger(t *testing.T) {

	// Test Logger
	logger := logtesting.TestLogger(t).Desugar()

	// Test Data
	channel := &kafkav1beta1.KafkaChannel{
		ObjectMeta: metav1.ObjectMeta{Name: "TestChannelName", Namespace: "TestChannelNamespace"},
	}

	// Perform The Test
	channelLogger := ChannelLogger(logger, channel)
	assert.NotNil(t, channelLogger)
	assert.NotEqual(t, logger, channelLogger)
	channelLogger.Info("Testing Channel Logger")
}

// Test The ChannelKey() Functionality
func TestChannelKey(t *testing.T) {

	// Test Data
	channel := &kafkav1beta1.KafkaChannel{ObjectMeta: metav1.ObjectMeta{Name: channelName, Namespace: channelNamespace}}

	// Perform The Test
	actualResult := ChannelKey(channel)

	// Verify The Results
	expectedResult := fmt.Sprintf("%s/%s", channelNamespace, channelName)
	assert.Equal(t, expectedResult, actualResult)
}

// Test The NewChannelOwnerReference() Functionality
func TestNewChannelOwnerReference(t *testing.T) {

	// Test Data
	channel := &kafkav1beta1.KafkaChannel{
		ObjectMeta: metav1.ObjectMeta{Name: channelName},
	}

	// Perform The Test
	controllerRef := NewChannelOwnerReference(channel)

	// Validate Results
	assert.NotNil(t, controllerRef)
	assert.Equal(t, kafkav1beta1.SchemeGroupVersion.String(), controllerRef.APIVersion)
	assert.Equal(t, constants.KafkaChannelKind, controllerRef.Kind)
	assert.Equal(t, channel.ObjectMeta.Name, controllerRef.Name)
	assert.True(t, *controllerRef.BlockOwnerDeletion)
	assert.True(t, *controllerRef.Controller)
}

// Test The ReceiverDnsSafeName() Functionality
func TestReceiverDnsSafeName(t *testing.T) {

	// Perform The Test
	differentPrefix := testPrefix + "-different"
	actualTest1Result := ReceiverDnsSafeName(testPrefix)
	actualTest2Result := ReceiverDnsSafeName(differentPrefix)

	// Verify The Results
	assert.Equal(t, fmt.Sprintf("%s-%s-receiver", strings.ToLower(testPrefix), GenerateHash(testPrefix, 8)), actualTest1Result)
	assert.Equal(t, fmt.Sprintf("%s-%s-receiver", strings.ToLower(differentPrefix), GenerateHash(differentPrefix, 8)), actualTest2Result)
	assert.NotEqual(t, actualTest1Result, actualTest2Result)
}

// Test The Channel Host Name Formatter / Generator
func TestChannelHostName(t *testing.T) {
	testChannelName := "TestChannelName"
	testChannelNamespace := "TestChannelNamespace"
	expectedChannelHostName := testChannelName + "." + testChannelNamespace + ".channels.cluster.local"
	actualChannelHostName := ChannelHostName(testChannelName, testChannelNamespace)
	assert.Equal(t, expectedChannelHostName, actualChannelHostName)
}
