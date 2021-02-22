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

package channel

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	channelhealth "knative.dev/eventing-kafka/pkg/channel/distributed/receiver/health"
	receivertesting "knative.dev/eventing-kafka/pkg/channel/distributed/receiver/testing"
	fakeclientset "knative.dev/eventing-kafka/pkg/client/clientset/versioned/fake"
	"knative.dev/pkg/logging"
	logtesting "knative.dev/pkg/logging/testing"
)

// Test The InitializeKafkaChannelLister() Functionality
func TestInitializeKafkaChannelLister(t *testing.T) {
	// Create A Context With Test Logger
	ctx := logging.WithLogger(context.TODO(), logtesting.TestLogger(t))

	kafkaClient := fakeclientset.NewSimpleClientset()

	// Perform The Test
	healthServer := channelhealth.NewChannelHealthServer("12345")
	err := InitializeKafkaChannelLister(ctx, kafkaClient, healthServer, 600*time.Minute)

	// Verify The Results
	assert.Nil(t, err)
	assert.NotNil(t, kafkaChannelLister)
	assert.Equal(t, true, healthServer.ChannelReady())
}

// Test All The ValidateKafkaChannel() Functionality
func TestValidateKafkaChannel(t *testing.T) {

	// Set The Package Level Logger To A Test Logger
	logger = logtesting.TestLogger(t).Desugar()

	// Test Data
	channelName := "TestChannelName"
	channelNamespace := "TestChannelNamespace"

	// Test All Permutations Of KafkaChannel Validation
	performValidateKafkaChannelTest(t, "", channelNamespace, false, corev1.ConditionFalse, true)
	performValidateKafkaChannelTest(t, channelName, "", false, corev1.ConditionFalse, true)
	performValidateKafkaChannelTest(t, channelName, channelNamespace, true, corev1.ConditionTrue, false)
	performValidateKafkaChannelTest(t, channelName, channelNamespace, true, corev1.ConditionFalse, true)
	performValidateKafkaChannelTest(t, channelName, channelNamespace, true, corev1.ConditionTrue, true)
	performValidateKafkaChannelTest(t, channelName, channelNamespace, false, corev1.ConditionFalse, true)
}

// Utility Function To Perform A Single Instance Of The ValidateKafkaChannel Test
func performValidateKafkaChannelTest(t *testing.T, channelName string, channelNamespace string, exists bool, ready corev1.ConditionStatus, err bool) {

	// Create The Channel Reference To Test
	channelReference := receivertesting.CreateChannelReference(channelName, channelNamespace)

	// Mock The Package Level KafkaChannel Lister For The Specified Use Case
	kafkaChannelLister = receivertesting.NewMockKafkaChannelLister(channelReference.Name, channelReference.Namespace, exists, ready, err)

	// Perform The Test
	validationError := ValidateKafkaChannel(channelReference)

	// Verify The Results
	assert.Equal(t, err, validationError != nil)
}

// Test The Close() Functionality
func TestClose(t *testing.T) {

	// Set The Package Level Logger To A Test Logger
	logger = logtesting.TestLogger(t).Desugar()

	// Test With Nil stopChan Instance
	Close()

	// Initialize The stopChan Instance
	stopChan = make(chan struct{})

	// Close In The Background
	go Close()

	// Block On The stopChan
	_, ok := <-stopChan

	// Verify stopChan Was Closed
	assert.False(t, ok)
}
