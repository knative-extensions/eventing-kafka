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
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	messagingv1 "knative.dev/eventing/pkg/apis/messaging/v1"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	logtesting "knative.dev/pkg/logging/testing"
	"knative.dev/pkg/system"

	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/constants"
)

// Test Data
const (
	subscriptionName      = "test-subscription-name"
	subscriptionNamespace = "test-subscription-namespace"
)

// Test The SubscriptionLogger Functionality
func TestSubscriptionLogger(t *testing.T) {

	// Test Logger
	logger := logtesting.TestLogger(t).Desugar()

	// Test Data
	subscription := &messagingv1.Subscription{
		ObjectMeta: metav1.ObjectMeta{Name: "TestSubscriptionName", Namespace: "TestSubscriptionNamespace"},
	}

	// Perform The Test
	subscriptionLogger := SubscriptionLogger(logger, subscription)
	assert.NotNil(t, subscriptionLogger)
	assert.NotEqual(t, logger, subscriptionLogger)
	subscriptionLogger.Info("Testing Subscription Logger")
}

// Test The NewSubscriptionControllerRef Functionality
func TestNewSubscriptionControllerRef(t *testing.T) {

	// Test Data
	subscription := &messagingv1.Subscription{
		ObjectMeta: metav1.ObjectMeta{Name: "TestName"},
	}

	// Perform The Test
	controllerRef := NewSubscriptionControllerRef(subscription)

	// Validate Results
	assert.NotNil(t, controllerRef)
	assert.Equal(t, messagingv1.SchemeGroupVersion.String(), controllerRef.APIVersion)
	assert.Equal(t, constants.KnativeSubscriptionKind, controllerRef.Kind)
	assert.Equal(t, subscription.ObjectMeta.Name, controllerRef.Name)
	assert.True(t, *controllerRef.Controller)
}

// Test The TopicNameMapper Functionality
func TestTopicNameMapper(t *testing.T) {

	// Test Data
	kafkaChannelGroupVersion := schema.GroupVersion{
		Group:   messagingv1.SchemeGroupVersion.Group,
		Version: messagingv1.SchemeGroupVersion.Version,
	}

	// Define The TestCases
	tests := []struct {
		name         string
		subscription *messagingv1.Subscription
		expected     string
		err          bool
	}{
		{
			name: "fully populated subscription",
			subscription: &messagingv1.Subscription{
				ObjectMeta: metav1.ObjectMeta{
					Name:      subscriptionName,
					Namespace: subscriptionNamespace,
				},
				Spec: messagingv1.SubscriptionSpec{
					Channel: duckv1.KReference{
						Kind:       constants.KafkaChannelKind,
						Namespace:  channelNamespace,
						Name:       channelName,
						APIVersion: kafkaChannelGroupVersion.String(),
					},
				},
			},
			expected: fmt.Sprintf("%s.%s", channelNamespace, channelName),
			err:      false,
		},
		{
			name: "sparsely populated subscription",
			subscription: &messagingv1.Subscription{
				ObjectMeta: metav1.ObjectMeta{
					Name:      subscriptionName,
					Namespace: subscriptionNamespace,
				},
				Spec: messagingv1.SubscriptionSpec{
					Channel: duckv1.KReference{
						Kind:       constants.KafkaChannelKind,
						Name:       channelName,
						APIVersion: kafkaChannelGroupVersion.String(),
					},
				},
			},
			expected: fmt.Sprintf("%s.%s", subscriptionNamespace, channelName),
			err:      false,
		},
		{
			name:         "nil subscription",
			subscription: nil,
			expected:     "",
			err:          true,
		},
	}

	// Execute The Tests
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {

			// Perform The Test
			actual, err := TopicNameMapper(test.subscription)

			// Verify Results
			assert.Equal(t, test.err, err != nil)
			assert.Equal(t, test.expected, actual)
		})
	}
}

// Test The GroupIdMapper Functionality
func TestGroupIdMapper(t *testing.T) {

	// Test Data
	subscriptionUID := "TestSubscriptionUID"

	// Define The TestCases
	tests := []struct {
		name         string
		subscription *messagingv1.Subscription
		expected     string
		err          bool
	}{
		{
			name: "valid subscription",
			subscription: &messagingv1.Subscription{
				ObjectMeta: metav1.ObjectMeta{
					UID: types.UID(subscriptionUID),
				},
			},
			expected: fmt.Sprintf("kafka.%s", subscriptionUID),
			err:      false,
		},
		{
			name:         "nil subscription",
			subscription: nil,
			expected:     "",
			err:          true,
		},
	}

	// Execute The Tests
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {

			// Perform The Test
			actual, err := GroupIdMapper(test.subscription)

			// Verify Results
			assert.Equal(t, test.err, err != nil)
			assert.Equal(t, test.expected, actual)
		})
	}
}

func TestConnectionPoolKeyMapper(t *testing.T) {

	// Test Data
	kafkaChannelGroupVersion := schema.GroupVersion{
		Group:   messagingv1.SchemeGroupVersion.Group,
		Version: messagingv1.SchemeGroupVersion.Version,
	}

	// Define The TestCases
	tests := []struct {
		name         string
		subscription *messagingv1.Subscription
		expected     string
		err          bool
	}{
		{
			name: "fully populated subscription",
			subscription: &messagingv1.Subscription{
				ObjectMeta: metav1.ObjectMeta{
					Name:      subscriptionName,
					Namespace: subscriptionNamespace,
				},
				Spec: messagingv1.SubscriptionSpec{
					Channel: duckv1.KReference{
						Kind:       constants.KafkaChannelKind,
						Namespace:  channelNamespace,
						Name:       channelName,
						APIVersion: kafkaChannelGroupVersion.String(),
					},
				},
			},
			expected: fmt.Sprintf("%s.%s", channelNamespace, channelName),
			err:      false,
		},
		{
			name: "sparsely populated subscription",
			subscription: &messagingv1.Subscription{
				ObjectMeta: metav1.ObjectMeta{
					Name:      subscriptionName,
					Namespace: subscriptionNamespace,
				},
				Spec: messagingv1.SubscriptionSpec{
					Channel: duckv1.KReference{
						Kind:       constants.KafkaChannelKind,
						Name:       channelName,
						APIVersion: kafkaChannelGroupVersion.String(),
					},
				},
			},
			expected: fmt.Sprintf("%s.%s", subscriptionNamespace, channelName),
			err:      false,
		},
		{
			name:         "nil subscription",
			subscription: nil,
			expected:     "",
			err:          true,
		},
	}

	// Execute The Tests
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {

			// Perform The Test
			actual, err := ConnectionPoolKeyMapper(test.subscription)

			// Verify Results
			assert.Equal(t, test.err, err != nil)
			assert.Equal(t, test.expected, actual)
		})
	}
}

func TestDataPlaneNamespaceMapper(t *testing.T) {

	// Define The TestCases
	tests := []struct {
		name      string
		namespace string
		expected  string
	}{
		{
			name:      "OS Value",
			namespace: "test-namespace",
			expected:  "test-namespace"},
		{
			name:      "Default Value",
			namespace: "",
			expected:  "knative-eventing",
		},
	}

	// Execute The Tests
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {

			// Setup The Environment
			os.Clearenv()
			if test.namespace != "" {
				assert.Nil(t, os.Setenv(system.NamespaceEnvKey, test.namespace))
			}

			// Perform The Test
			result, err := DataPlaneNamespaceMapper(nil)

			// Verify Results
			assert.Nil(t, err)
			assert.Equal(t, test.expected, result)
		})
	}
}

func TestDataPlaneLabelsMapper(t *testing.T) {

	// Define The TestCases
	tests := []struct {
		name         string
		subscription *messagingv1.Subscription
		expected     map[string]string
	}{
		{
			name: "Complete Channel Reference",
			subscription: &messagingv1.Subscription{
				Spec: messagingv1.SubscriptionSpec{
					Channel: duckv1.KReference{
						Namespace: channelNamespace,
						Name:      channelName,
					},
				},
			},
			expected: map[string]string{
				constants.KafkaChannelDispatcherLabel: "true",
				constants.KafkaChannelNameLabel:       channelName,
				constants.KafkaChannelNamespaceLabel:  channelNamespace,
			},
		},
		{
			name: "Sparse Channel Reference",
			subscription: &messagingv1.Subscription{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: channelNamespace,
				},
				Spec: messagingv1.SubscriptionSpec{
					Channel: duckv1.KReference{
						Name: channelName,
					},
				},
			},
			expected: map[string]string{
				constants.KafkaChannelDispatcherLabel: "true",
				constants.KafkaChannelNameLabel:       channelName,
				constants.KafkaChannelNamespaceLabel:  channelNamespace,
			},
		},
	}

	// Execute The Tests
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {

			// Perform The Test
			result, err := DataPlaneLabelsMapper(test.subscription)

			// Verify Results
			assert.Nil(t, err)
			assert.Equal(t, test.expected, result)
		})
	}
}
