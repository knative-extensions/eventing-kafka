/*
Copyright 2021 The Knative Authors

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

package refmappers

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/rest"
	messagingv1 "knative.dev/eventing/pkg/apis/messaging/v1"
	_ "knative.dev/eventing/pkg/client/injection/informers/messaging/v1/subscription/fake" // Knative Fake Informer Injection
	messaginglisters "knative.dev/eventing/pkg/client/listers/messaging/v1"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	"knative.dev/pkg/injection"
	"knative.dev/pkg/logging"
	logtesting "knative.dev/pkg/logging/testing"

	kafkav1alpha1 "knative.dev/eventing-kafka/pkg/apis/kafka/v1alpha1"
	controllertesting "knative.dev/eventing-kafka/pkg/common/commands/resetoffset/controller/testing"
)

const (
	SubscriptionNamespace = "subscription-namespace"
	SubscriptionName      = "subscription-name"

	TopicName = "TestTopicName"
	GroupId   = "TestGroupId"
)

func TestNewSubscriptionRefMapperFactory(t *testing.T) {

	// Create A Context With Test Logger
	logger := logtesting.TestLogger(t)
	ctx := logging.WithLogger(context.Background(), logger)

	// Register Fake Informers (See Injection "_" Imports Above!)
	ctx, fakeInformers := injection.Fake.SetupInformers(ctx, &rest.Config{})
	assert.NotNil(t, fakeInformers)

	// Create Test Mappers
	topicNameMapper := newMockSubscriptionTopicNameMapper(t, nil, TopicName, nil)
	groupIdMapper := newMockSubscriptionConsumerGroupIdMapper(t, nil, GroupId, nil)

	// Perform The Test - Create New Subscription RefMapper Factory
	factory := NewSubscriptionRefMapperFactory(topicNameMapper, groupIdMapper)
	assert.NotNil(t, factory)

	// Test The Factory Create()
	refMapper := factory.Create(ctx)
	assert.NotNil(t, refMapper)
}

func TestNewResetOffsetSubscriptionRefMapper(t *testing.T) {

	// Create A Context With Test Logger
	logger := logtesting.TestLogger(t)
	ctx := logging.WithLogger(context.Background(), logger)

	// Register Fake Informers (See Injection "_" Imports Above!)
	ctx, fakeInformers := injection.Fake.SetupInformers(ctx, &rest.Config{})
	assert.NotNil(t, fakeInformers)

	// Create Test Mappers
	topicNameMapper := newMockSubscriptionTopicNameMapper(t, nil, TopicName, nil)
	groupIdMapper := newMockSubscriptionConsumerGroupIdMapper(t, nil, GroupId, nil)

	// Perform The Test - Create A New SubscriptionRefMapper
	resetOffsetSubscriptionRefMapper := NewSubscriptionRefMapper(ctx, topicNameMapper, groupIdMapper)

	// Verify The Results
	assert.NotNil(t, resetOffsetSubscriptionRefMapper)
	assert.Equal(t, logger.Desugar(), resetOffsetSubscriptionRefMapper.logger)
	assert.NotNil(t, resetOffsetSubscriptionRefMapper.subscriptionLister)
	assert.NotNil(t, resetOffsetSubscriptionRefMapper.topicNameMapper) // Testify / DeepEqual Cannot Compare func Types
	assert.NotNil(t, resetOffsetSubscriptionRefMapper.groupIdMapper)
}

func TestResetOffsetSubscriptionRefMapper_MapRef(t *testing.T) {

	// Test Data
	logger := logtesting.TestLogger(t).Desugar()
	testErr := fmt.Errorf("test-error")

	// Create A Test Subscription
	subscription := &messagingv1.Subscription{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Subscription",
			APIVersion: messagingv1.SchemeGroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: SubscriptionNamespace,
			Name:      SubscriptionName,
		},
	}

	//
	subscriptionRef := &duckv1.KReference{
		Kind:       "Subscription",
		APIVersion: messagingv1.SchemeGroupVersion.String(),
		Namespace:  SubscriptionNamespace,
		Name:       SubscriptionName,
	}

	// Define The Test Cases
	tests := []struct {
		name            string
		subscription    *messagingv1.Subscription
		subscriptionErr error
		resetOffset     *kafkav1alpha1.ResetOffset
		topicNameMapper SubscriptionTopicNameMapper
		groupIdMapper   SubscriptionConsumerGroupIdMapper
		wantTopic       string
		wantGroup       string
		wantErr         bool
	}{
		{
			name:            "Success",
			subscription:    subscription,
			resetOffset:     controllertesting.NewResetOffset(controllertesting.WithSpecRef(subscriptionRef)),
			topicNameMapper: newMockSubscriptionTopicNameMapper(t, subscription, TopicName, nil),
			groupIdMapper:   newMockSubscriptionConsumerGroupIdMapper(t, subscription, GroupId, nil),
			wantTopic:       TopicName,
			wantGroup:       GroupId,
		},
		{
			name:        "Nil ResetOffset",
			resetOffset: nil,
			wantErr:     true,
		},
		{
			name: "Invalid ResetOffset.Spec.Ref",
			resetOffset: controllertesting.NewResetOffset(controllertesting.WithSpecRef(&duckv1.KReference{
				Kind:       "foo",
				Namespace:  "bar",
				Name:       "baz",
				APIVersion: "bing",
			})),
			wantErr: true,
		},
		{
			name: "Sparse ResetOffset.Spec.Ref",
			resetOffset: controllertesting.NewResetOffset(controllertesting.WithSpecRef(&duckv1.KReference{
				Kind:       "Subscription",
				APIVersion: messagingv1.SchemeGroupVersion.String(),
				Name:       SubscriptionName,
			})),
			wantErr: true,
		},
		{
			name:            "Subscription Get Error",
			subscriptionErr: testErr,
			resetOffset:     controllertesting.NewResetOffset(controllertesting.WithSpecRef(subscriptionRef)),
			wantErr:         true,
		},
		{
			name:            "TopicName Mapper Error",
			subscription:    subscription,
			resetOffset:     controllertesting.NewResetOffset(controllertesting.WithSpecRef(subscriptionRef)),
			topicNameMapper: newMockSubscriptionTopicNameMapper(t, subscription, TopicName, testErr),
			groupIdMapper:   newMockSubscriptionConsumerGroupIdMapper(t, subscription, GroupId, nil),
			wantErr:         true,
		},
		{
			name:            "GroupId Mapper Error",
			subscription:    subscription,
			resetOffset:     controllertesting.NewResetOffset(controllertesting.WithSpecRef(subscriptionRef)),
			topicNameMapper: newMockSubscriptionTopicNameMapper(t, subscription, TopicName, nil),
			groupIdMapper:   newMockSubscriptionConsumerGroupIdMapper(t, subscription, GroupId, testErr),
			wantErr:         true,
		},
	}

	// Execute The Test Cases
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {

			// Create A Mock SubscriptionLister To Return The Test Subscription
			mockSubscriptionNamespaceLister := &MockSubscriptionNamespaceLister{}
			mockSubscriptionNamespaceLister.On("Get", SubscriptionName).Return(test.subscription, test.subscriptionErr)
			mockSubscriptionLister := &MockSubscriptionLister{}
			mockSubscriptionLister.On("Subscriptions", SubscriptionNamespace).Return(mockSubscriptionNamespaceLister)
			if test.resetOffset != nil {
				mockSubscriptionLister.On("Subscriptions", test.resetOffset.Namespace).Return(mockSubscriptionNamespaceLister)
			}

			// Create A New SubscriptionRefMapper To Test
			subscriptionRefMapper := &SubscriptionRefMapper{
				logger:             logger,
				subscriptionLister: mockSubscriptionLister,
				topicNameMapper:    test.topicNameMapper,
				groupIdMapper:      test.groupIdMapper,
			}

			// Perform The Test - Map A Subscription To Kafka Topic Name & ConsumerGroup ID
			topicName, groupId, err := subscriptionRefMapper.MapRef(test.resetOffset)

			// Validate The Results
			assert.Equal(t, test.wantErr, err != nil)
			assert.Equal(t, test.wantTopic, topicName)
			assert.Equal(t, test.wantGroup, groupId)
			//tODO - make dynamic
			//mockSubscriptionLister.AssertExpectations(t)
		})
	}
}

//
// Mock SubscriptionLister
//

var _ messaginglisters.SubscriptionLister = &MockSubscriptionLister{}

type MockSubscriptionLister struct {
	mock.Mock
}

func (l *MockSubscriptionLister) List(selector labels.Selector) (ret []*messagingv1.Subscription, err error) {
	args := l.Called(selector)
	return args.Get(0).([]*messagingv1.Subscription), args.Error(1)
}

func (l *MockSubscriptionLister) Subscriptions(namespace string) messaginglisters.SubscriptionNamespaceLister {
	args := l.Called(namespace)
	return args.Get(0).(messaginglisters.SubscriptionNamespaceLister)
}

//
// Mock SubscriptionNamespaceLister
//

var _ messaginglisters.SubscriptionNamespaceLister = &MockSubscriptionNamespaceLister{}

type MockSubscriptionNamespaceLister struct {
	mock.Mock
}

func (nl *MockSubscriptionNamespaceLister) List(selector labels.Selector) (ret []*messagingv1.Subscription, err error) {
	args := nl.Called(selector)
	return args.Get(0).([]*messagingv1.Subscription), args.Error(1)
}

func (nl *MockSubscriptionNamespaceLister) Get(name string) (*messagingv1.Subscription, error) {
	args := nl.Called(name)
	return args.Get(0).(*messagingv1.Subscription), args.Error(1)
}

//
// Mock Mappers
//

func newMockSubscriptionTopicNameMapper(t *testing.T, expectedSubscription *messagingv1.Subscription, topicName string, err error) SubscriptionTopicNameMapper {
	return func(subscription *messagingv1.Subscription) (string, error) {
		assert.Equal(t, expectedSubscription, subscription)
		return topicName, err
	}
}

func newMockSubscriptionConsumerGroupIdMapper(t *testing.T, expectedSubscription *messagingv1.Subscription, groupId string, err error) SubscriptionConsumerGroupIdMapper {
	return func(subscription *messagingv1.Subscription) (string, error) {
		assert.Equal(t, expectedSubscription, subscription)
		return groupId, err
	}
}
