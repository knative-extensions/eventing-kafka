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

package kafka

import (
	"context"
	"fmt"

	"github.com/Shopify/sarama"
	"go.uber.org/zap"
	"knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/admin/types"
	"knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/admin/util"
	"knative.dev/pkg/logging"
)

//
// This is an implementation of the AdminClient interface backed by the Sarama API. This is largely
// a pass-through to the Sarama ClusterAdmin with some additional functionality layered on top.
//

// Ensure The KafkaAdminClient Struct Implements The AdminClientInterface
var _ types.AdminClientInterface = &KafkaAdminClient{}

// Kafka AdminClient Definition
type KafkaAdminClient struct {
	logger       *zap.Logger
	clusterAdmin sarama.ClusterAdmin
}

// Create A New Kafka AdminClient Based On The Kafka Secret In The Specified K8S Namespace
func NewAdminClient(ctx context.Context, brokers []string, saramaConfig *sarama.Config) (types.AdminClientInterface, error) {

	// Get The Logger From The Context
	logger := logging.FromContext(ctx).Desugar()

	// Create A New Sarama ClusterAdmin
	clusterAdmin, err := NewClusterAdminFn(brokers, saramaConfig)
	if err != nil {
		logger.Error("Failed To Create New ClusterAdmin", zap.Any("Config", saramaConfig), zap.Error(err))
		return nil, err
	}

	// Create The KafkaAdminClient
	kafkaAdminClient := &KafkaAdminClient{
		logger:       logger,
		clusterAdmin: clusterAdmin,
	}

	// Return The KafkaAdminClient - Success
	logger.Debug("Successfully Created New Kafka AdminClient")
	return kafkaAdminClient, nil
}

// Sarama NewClusterAdmin() Wrapper Function Variable To Facilitate Unit Testing
var NewClusterAdminFn = func(brokers []string, config *sarama.Config) (sarama.ClusterAdmin, error) {
	return sarama.NewClusterAdmin(brokers, config)
}

// Sarama Pass-Through Function For Creating Topics
func (k KafkaAdminClient) CreateTopic(_ context.Context, topicName string, topicDetail *sarama.TopicDetail) *sarama.TopicError {
	if k.clusterAdmin == nil {
		k.logger.Error("Unable To Create Topic Due To Invalid ClusterAdmin - Check Kafka Authorization Secret")
		return util.NewUnknownTopicError("unable to create topic due to invalid ClusterAdmin - check Kafka authorization secrets")
	} else {
		err := k.clusterAdmin.CreateTopic(topicName, topicDetail, false)
		return util.PromoteErrorToTopicError(err)
	}
}

// Sarama Pass-Through Function For Deleting Topics
func (k KafkaAdminClient) DeleteTopic(_ context.Context, topicName string) *sarama.TopicError {
	if k.clusterAdmin == nil {
		k.logger.Error("Unable To Delete Topic Due To Invalid ClusterAdmin - Check Kafka Authorization Secret")
		return util.NewUnknownTopicError("unable to delete topic due to invalid ClusterAdmin - check Kafka authorization secrets")
	} else {
		err := k.clusterAdmin.DeleteTopic(topicName)
		return util.PromoteErrorToTopicError(err)
	}
}

// Sarama Pass-Through Function For Closing ClusterAdmin
func (k KafkaAdminClient) Close() error {
	if k.clusterAdmin == nil {
		k.logger.Error("Unable To Close Invalid ClusterAdmin - Check Kafka Authorization Secret")
		return fmt.Errorf("unable to close invalid ClusterAdmin - check Kafka authorization secrets")
	} else {
		return k.clusterAdmin.Close()
	}
}
