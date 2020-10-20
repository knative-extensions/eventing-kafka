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
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/constants"
	messagingv1 "knative.dev/eventing/pkg/apis/messaging/v1"
	logtesting "knative.dev/pkg/logging/testing"
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
