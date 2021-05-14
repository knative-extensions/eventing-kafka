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
	"strings"

	"go.uber.org/zap"
	"knative.dev/eventing/pkg/apis/messaging"
	messagingv1 "knative.dev/eventing/pkg/apis/messaging/v1"
	subscriptioninformers "knative.dev/eventing/pkg/client/injection/informers/messaging/v1/subscription"
	messaginglisters "knative.dev/eventing/pkg/client/listers/messaging/v1"
	"knative.dev/pkg/logging"

	kafkav1alpha1 "knative.dev/eventing-kafka/pkg/apis/kafka/v1alpha1"
)

//
// SubscriptionRefMapperFactory
//

// Verify The Subscription ResetOffsetRefMapperFactory Implements The Interface
var _ ResetOffsetRefMapperFactory = &SubscriptionRefMapperFactory{}

// SubscriptionRefMapperFactory implements the ResetOffsetRefMapperFactory for Knative Subscriptions
type SubscriptionRefMapperFactory struct {
	TopicNameMapper SubscriptionTopicNameMapper
	GroupIdMapper   SubscriptionConsumerGroupIdMapper
}

// NewSubscriptionRefMapperFactory returns an initialized SubscriptionRefMapperFactory
func NewSubscriptionRefMapperFactory(topicNameMapper SubscriptionTopicNameMapper, groupIdMapper SubscriptionConsumerGroupIdMapper) *SubscriptionRefMapperFactory {
	return &SubscriptionRefMapperFactory{TopicNameMapper: topicNameMapper, GroupIdMapper: groupIdMapper}
}

// Create implements the ResetOffsetRefMapperFactory interface for Subscription references.  It will return
// a new SubscriptionRefMapper instance using the specific TopicName / GroupId mappers.  It also relies on
// the Context having injected informers (SubscriptionInformer).
func (f *SubscriptionRefMapperFactory) Create(ctx context.Context) ResetOffsetRefMapper {
	return NewSubscriptionRefMapper(ctx, f.TopicNameMapper, f.GroupIdMapper)
}

//
// SubscriptionRefMapper
//

// SubscriptionTopicNameMapper defines a function signature for mapping a Subscription to a Kafka Topic name.
type SubscriptionTopicNameMapper func(*messagingv1.Subscription) (string, error)

// SubscriptionConsumerGroupIdMapper defines a function signature for mapping a Subscription to a Kafka ConsumerGroup ID.
type SubscriptionConsumerGroupIdMapper func(*messagingv1.Subscription) (string, error)

// Verify The Subscription ResetOffsetRefMapper Implements The Interface
var _ ResetOffsetRefMapper = &SubscriptionRefMapper{}

// SubscriptionRefMapper implements the ResetOffsetRefMapper for Knative Subscriptions
type SubscriptionRefMapper struct {
	logger             *zap.Logger
	subscriptionLister messaginglisters.SubscriptionLister
	topicNameMapper    SubscriptionTopicNameMapper
	groupIdMapper      SubscriptionConsumerGroupIdMapper
}

// NewSubscriptionRefMapper returns an initialized ResetOffsetSubscriptionRefMapper
func NewSubscriptionRefMapper(ctx context.Context,
	topicNameMapper SubscriptionTopicNameMapper,
	groupIdMapper SubscriptionConsumerGroupIdMapper) *SubscriptionRefMapper {

	// Get The Logger From Context
	logger := logging.FromContext(ctx).Desugar()

	// Get The Subscription Informer From Context (Context Must Have Injected Informers From SharedMain())
	subscriptionInformer := subscriptioninformers.Get(ctx)

	// Return An Initialized ResetOffsetSubscriptionRefMapper
	return &SubscriptionRefMapper{
		logger:             logger,
		subscriptionLister: subscriptionInformer.Lister(),
		topicNameMapper:    topicNameMapper,
		groupIdMapper:      groupIdMapper,
	}
}

// MapRef implements the ResetOffsetRefMapper interface for Subscription references. It will return an
// error in all cases other than successfully mapping the ResetOffset.Spec.Ref to a Kafka Topic / Group.
func (m *SubscriptionRefMapper) MapRef(resetOffset *kafkav1alpha1.ResetOffset) (string, string, error) {

	// Validate The ResetOffset
	if resetOffset == nil {
		m.logger.Warn("Received nil ResetOffset argument")
		return "", "", fmt.Errorf("unable to map nil ResetOffset")
	}

	// Get The ResetOffset Ref From Spec & Enhance Logger
	ref := resetOffset.Spec.Ref
	logger := m.logger.With(zap.Any("Ref", ref))

	// Validate The Reference
	if !strings.HasPrefix(ref.APIVersion, messaging.GroupName) ||
		ref.Kind != "Subscription" ||
		len(ref.Name) <= 0 {
		m.logger.Warn("Received ResetOffset with invalid Subscription reference")
		return "", "", fmt.Errorf("received ResetOffset with invalid Subscription reference: %v", ref)
	}

	// Default Optional Ref.Namespace If Not Provided
	refNamespace := ref.Namespace
	if len(refNamespace) <= 0 {
		refNamespace = resetOffset.Namespace
	}

	// Attempt To Get The Specified Subscription
	subscription, err := m.subscriptionLister.Subscriptions(refNamespace).Get(ref.Name)
	if err != nil {
		logger.Error("Failed to get Subscription referenced by ResetOffset", zap.Error(err))
		return "", "", fmt.Errorf("failed to get Subscription referenced by ResetOffset.Spec.Ref '%v': %v", ref, err)
	}
	if subscription == nil {
		logger.Info("No Subscription found for ResetOffset reference")
		return "", "", fmt.Errorf("no Subscription found for ResetOffset.Spec.Ref %v", ref)
	}

	// Map The Subscription To Kafka Topic Name Via Custom SubscriptionTopicNameMapper
	topicName, err := m.topicNameMapper(subscription)
	if err != nil {
		logger.Error("Failed to map Knative Subscription to Kafka Topic name", zap.Error(err))
		return "", "", fmt.Errorf("failed to map Knative Subscription '%v' to Kafka Topic name: %v", ref, err)
	}

	// Map The Subscription To Kafka ConsumerGroup ID Via Custom SubscriptionGroupIdMapper
	groupId, err := m.groupIdMapper(subscription)
	if err != nil {
		logger.Error("Failed to map Knative Subscription to Kafka ConsumerGroup ID", zap.Error(err))
		return "", "", fmt.Errorf("failed to map Knative Subscription '%v' to Kafka ConsumerGroup ID: %v", ref, err)
	}

	// Successfully Mapped The Ref To Kafka Topic / Group - Return Results
	return topicName, groupId, nil
}
