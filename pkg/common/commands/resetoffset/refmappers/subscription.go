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
	TopicNameMapper          SubscriptionTopicNameMapper
	GroupIdMapper            SubscriptionConsumerGroupIdMapper
	ConnectionPoolKeyMapper  SubscriptionConnectionPoolKeyMapper
	DataPlaneNamespaceMapper SubscriptionDataPlaneNamespaceMapper
	DataPlaneLabelsMapper    SubscriptionDataPlaneLabelsMapper
}

// NewSubscriptionRefMapperFactory returns an initialized SubscriptionRefMapperFactory
func NewSubscriptionRefMapperFactory(topicNameMapper SubscriptionTopicNameMapper,
	groupIdMapper SubscriptionConsumerGroupIdMapper,
	connectionPoolKeyMapper SubscriptionConnectionPoolKeyMapper,
	dataPlaneNamespaceMapper SubscriptionDataPlaneNamespaceMapper,
	dataPlaneLabelsMapper SubscriptionDataPlaneLabelsMapper) *SubscriptionRefMapperFactory {

	return &SubscriptionRefMapperFactory{
		TopicNameMapper:          topicNameMapper,
		GroupIdMapper:            groupIdMapper,
		ConnectionPoolKeyMapper:  connectionPoolKeyMapper,
		DataPlaneNamespaceMapper: dataPlaneNamespaceMapper,
		DataPlaneLabelsMapper:    dataPlaneLabelsMapper,
	}
}

// Create implements the ResetOffsetRefMapperFactory interface for Subscription references.  It will return
// a new SubscriptionRefMapper instance using the specific TopicName / GroupId mappers.  It also relies on
// the Context having injected informers (SubscriptionInformer).
func (f *SubscriptionRefMapperFactory) Create(ctx context.Context) ResetOffsetRefMapper {
	return NewSubscriptionRefMapper(ctx,
		f.TopicNameMapper,
		f.GroupIdMapper,
		f.ConnectionPoolKeyMapper,
		f.DataPlaneNamespaceMapper,
		f.DataPlaneLabelsMapper)
}

//
// SubscriptionRefMapper
//

// SubscriptionTopicNameMapper defines a function signature for mapping a Subscription to a Kafka Topic name.
type SubscriptionTopicNameMapper func(*messagingv1.Subscription) (string, error)

// SubscriptionConsumerGroupIdMapper defines a function signature for mapping a Subscription to a Kafka ConsumerGroup ID.
type SubscriptionConsumerGroupIdMapper func(*messagingv1.Subscription) (string, error)

// SubscriptionConnectionPoolKeyMapper defines a function signature for mapping a Subscription to a control-protocol ControlPlaneConnectionPool Key.
type SubscriptionConnectionPoolKeyMapper func(*messagingv1.Subscription) (string, error)

// SubscriptionDataPlaneNamespaceMapper defines a function signature for mapping a Subscription to the Kubernetes namespace of the DataPlane components.
type SubscriptionDataPlaneNamespaceMapper func(subscription *messagingv1.Subscription) (string, error)

// SubscriptionDataPlaneLabelsMapper defines a function signature for mapping a Subscription to the Kubernetes labels of the DataPlane Pods.
type SubscriptionDataPlaneLabelsMapper func(subscription *messagingv1.Subscription) (map[string]string, error)

// Verify The Subscription ResetOffsetRefMapper Implements The Interface
var _ ResetOffsetRefMapper = &SubscriptionRefMapper{}

// SubscriptionRefMapper implements the ResetOffsetRefMapper for Knative Subscriptions
type SubscriptionRefMapper struct {
	logger                   *zap.Logger
	subscriptionLister       messaginglisters.SubscriptionLister
	topicNameMapper          SubscriptionTopicNameMapper
	groupIdMapper            SubscriptionConsumerGroupIdMapper
	connectionPoolKeyMapper  SubscriptionConnectionPoolKeyMapper
	dataPlaneNamespaceMapper SubscriptionDataPlaneNamespaceMapper
	dataPlaneLabelsMapper    SubscriptionDataPlaneLabelsMapper
}

// NewSubscriptionRefMapper returns an initialized ResetOffsetSubscriptionRefMapper
func NewSubscriptionRefMapper(ctx context.Context,
	topicNameMapper SubscriptionTopicNameMapper,
	groupIdMapper SubscriptionConsumerGroupIdMapper,
	connectionPoolKeyMapper SubscriptionConnectionPoolKeyMapper,
	dataPlaneNamespaceMapper SubscriptionDataPlaneNamespaceMapper,
	dataPlaneLabelsMapper SubscriptionDataPlaneLabelsMapper) *SubscriptionRefMapper {

	// Get The Logger From Context
	logger := logging.FromContext(ctx).Desugar()

	// Get The Subscription Informer From Context (Context Must Have Injected Informers From SharedMain())
	subscriptionInformer := subscriptioninformers.Get(ctx)

	// Return An Initialized ResetOffsetSubscriptionRefMapper
	return &SubscriptionRefMapper{
		logger:                   logger,
		subscriptionLister:       subscriptionInformer.Lister(),
		topicNameMapper:          topicNameMapper,
		groupIdMapper:            groupIdMapper,
		connectionPoolKeyMapper:  connectionPoolKeyMapper,
		dataPlaneNamespaceMapper: dataPlaneNamespaceMapper,
		dataPlaneLabelsMapper:    dataPlaneLabelsMapper,
	}
}

// MapRef implements the ResetOffsetRefMapper interface for Subscription references. It will return an
// error in all cases other than successfully mapping the ResetOffset.Spec.Ref to a Kafka Topic / Group.
func (m *SubscriptionRefMapper) MapRef(resetOffset *kafkav1alpha1.ResetOffset) (*RefInfo, error) {

	// Validate The ResetOffset
	if resetOffset == nil {
		m.logger.Warn("Received nil ResetOffset argument")
		return nil, fmt.Errorf("unable to map nil ResetOffset")
	}

	// Get The ResetOffset Ref From Spec & Enhance Logger
	ref := resetOffset.Spec.Ref
	logger := m.logger.With(zap.Any("Ref", ref))

	// Validate The Reference
	if !strings.HasPrefix(ref.APIVersion, messaging.GroupName) || ref.Kind != "Subscription" {
		m.logger.Warn("Received ResetOffset with non Subscription reference")
		return nil, fmt.Errorf("received ResetOffset with non Subscription reference: %v", ref)
	}
	if ref.Name == "" {
		m.logger.Warn("Received ResetOffset with unnamed Subscription reference")
		return nil, fmt.Errorf("received ResetOffset with unnamed Subscription reference: %v", ref)
	}

	// Default Optional Ref.Namespace If Not Provided
	refNamespace := ref.Namespace
	if refNamespace == "" {
		refNamespace = resetOffset.Namespace
	}

	// Attempt To Get The Specified Subscription
	subscription, err := m.subscriptionLister.Subscriptions(refNamespace).Get(ref.Name)
	if err != nil {
		logger.Error("Failed to get Subscription referenced by ResetOffset", zap.Error(err))
		return nil, fmt.Errorf("failed to get Subscription referenced by ResetOffset.Spec.Ref '%v': %v", ref, err)
	}
	if subscription == nil {
		logger.Info("No Subscription found for ResetOffset reference")
		return nil, fmt.Errorf("no Subscription found for ResetOffset.Spec.Ref %v", ref)
	}

	// Map The Subscription To Kafka Topic Name Via Custom SubscriptionTopicNameMapper
	topicName, err := m.topicNameMapper(subscription)
	if err != nil {
		logger.Error("Failed to map Knative Subscription to Kafka Topic name", zap.Error(err))
		return nil, fmt.Errorf("failed to map Knative Subscription '%v' to Kafka Topic name: %v", ref, err)
	}

	// Map The Subscription To Kafka ConsumerGroup ID Via Custom SubscriptionGroupIdMapper
	groupId, err := m.groupIdMapper(subscription)
	if err != nil {
		logger.Error("Failed to map Knative Subscription to Kafka ConsumerGroup ID", zap.Error(err))
		return nil, fmt.Errorf("failed to map Knative Subscription '%v' to Kafka ConsumerGroup ID: %v", ref, err)
	}

	// Map The Subscription To The control-protocol ControlPlaneConnectionPool Key Via Custom SubscriptionConnectionPoolKeyMapper
	connectionPoolKey, err := m.connectionPoolKeyMapper(subscription)
	if err != nil {
		logger.Error("Failed to map Knative Subscription to ControlPlaneConnectionPool Key", zap.Error(err))
		return nil, fmt.Errorf("failed to map Knative Subscription '%v' to ConnectinPool Key: %v", ref, err)
	}

	// Map The Subscription To The Kubernetes Namespace of the DataPlane Pods.
	dataPlaneNamespace, err := m.dataPlaneNamespaceMapper(subscription)
	if err != nil {
		logger.Error("Failed to map Knative Subscription to DataPlane Namespace", zap.Error(err))
		return nil, fmt.Errorf("failed to map Knative Subscription '%v' to DataPlane Namespace: %v", ref, err)
	}

	// Map The Subscription To The Kubernetes Namespace of the DataPlane Pods.
	dataPlaneLabels, err := m.dataPlaneLabelsMapper(subscription)
	if err != nil {
		logger.Error("Failed to map Knative Subscription to DataPlane Pod Labels", zap.Error(err))
		return nil, fmt.Errorf("failed to map Knative Subscription '%v' to DataPlane Pod Labels: %v", ref, err)
	}

	// Create The RefInfo Struct
	refInfo := &RefInfo{
		TopicName:          topicName,
		GroupId:            groupId,
		ConnectionPoolKey:  connectionPoolKey,
		DataPlaneNamespace: dataPlaneNamespace,
		DataPlaneLabels:    dataPlaneLabels,
	}

	// Successfully Mapped The Ref - Return Results
	return refInfo, nil
}
