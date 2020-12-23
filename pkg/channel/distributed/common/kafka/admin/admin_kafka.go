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

package admin

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/Shopify/sarama"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	adminutil "knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/admin/util"
	"knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/constants"
	"knative.dev/eventing-kafka/pkg/common/client"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/logging"
)

//
// This is an implementation of the AdminClient interface backed by the Sarama API. This is largely
// a pass-through to the Sarama ClusterAdmin with some additional functionality layered on top.
//

// Ensure The KafkaAdminClient Struct Implements The AdminClientInterface
var _ AdminClientInterface = &KafkaAdminClient{}

// Kafka AdminClient Definition
type KafkaAdminClient struct {
	logger       *zap.Logger
	namespace    string
	kafkaSecret  string
	clientId     string
	clusterAdmin sarama.ClusterAdmin
}

// Create A New Kafka AdminClient Based On The Kafka Secret In The Specified K8S Namespace
func NewKafkaAdminClient(ctx context.Context, saramaConfig *sarama.Config, clientId string, namespace string) (AdminClientInterface, error) {

	// Get The Logger From The Context
	logger := logging.FromContext(ctx).Desugar()

	// Get The K8S Client From The Context
	k8sClient := kubeclient.Get(ctx)

	// Get A List Of The Kafka Secrets
	kafkaSecrets, err := adminutil.GetKafkaSecrets(ctx, k8sClient, namespace)
	if err != nil {
		logger.Error("Failed To Get Kafka Authentication Secrets", zap.Error(err))
		return nil, err
	}

	// Currently Only Support One Kafka Secret - Invalid AdminClient For All Other Cases!
	var kafkaSecret corev1.Secret
	if len(kafkaSecrets.Items) != 1 {
		logger.Warn(fmt.Sprintf("Expected 1 Kafka Secret But Found %d - Kafka AdminClient Will Not Be Functional!", len(kafkaSecrets.Items)))
		return &KafkaAdminClient{logger: logger, namespace: namespace, clientId: clientId}, nil
	} else {
		logger.Info("Found 1 Kafka Secret", zap.String("Secret", kafkaSecrets.Items[0].Name))
		kafkaSecret = kafkaSecrets.Items[0]
	}

	// Validate Secret Data
	if !adminutil.ValidateKafkaSecret(logger, &kafkaSecret) {
		err = errors.New("invalid Kafka Secret found")
		return nil, err
	}

	// Extract The Relevant Data From The Kafka Secret
	brokers := strings.Split(string(kafkaSecret.Data[constants.KafkaSecretKeyBrokers]), ",")
	username := string(kafkaSecret.Data[constants.KafkaSecretKeyUsername])
	password := string(kafkaSecret.Data[constants.KafkaSecretKeyPassword])

	client.UpdateConfigWithDefaults(saramaConfig)
	saramaConfig.ClientID = clientId

	// Update The Sarama ClusterAdmin Configuration With Our Values
	if username != "" {
		err = client.UpdateSaramaConfigWithKafkaAuthConfig(saramaConfig, &client.KafkaAuthConfig{
			SASL: &client.KafkaSaslConfig{
				User:     username,
				Password: password,
			},
		})
		if err != nil {
			logger.Fatal("Unable to set SASL username and password on Sarama config", zap.Error(err))
			return nil, err
		}
	}

	// Create A New Sarama ClusterAdmin
	clusterAdmin, err := NewClusterAdminWrapper(brokers, saramaConfig)
	if err != nil {
		logger.Error("Failed To Create New ClusterAdmin", zap.Any("Config", saramaConfig), zap.Error(err))
		return nil, err
	}

	// Create The KafkaAdminClient
	kafkaAdminClient := &KafkaAdminClient{
		logger:       logger,
		namespace:    namespace,
		kafkaSecret:  kafkaSecret.Name,
		clientId:     clientId,
		clusterAdmin: clusterAdmin,
	}

	// Return The KafkaAdminClient - Success
	logger.Debug("Successfully Created New Kafka AdminClient")
	return kafkaAdminClient, nil
}

// Sarama NewClusterAdmin() Wrapper Function Variable To Facilitate Unit Testing
var NewClusterAdminWrapper = func(brokers []string, config *sarama.Config) (sarama.ClusterAdmin, error) {
	return sarama.NewClusterAdmin(brokers, config)
}

// Sarama Pass-Through Function For Creating Topics
func (k KafkaAdminClient) CreateTopic(_ context.Context, topicName string, topicDetail *sarama.TopicDetail) *sarama.TopicError {
	if k.clusterAdmin == nil {
		k.logger.Error("Unable To Create Topic Due To Invalid ClusterAdmin - Check Kafka Authorization Secret")
		return adminutil.NewUnknownTopicError("unable to create topic due to invalid ClusterAdmin - check Kafka authorization secrets")
	} else {
		err := k.clusterAdmin.CreateTopic(topicName, topicDetail, false)
		return adminutil.PromoteErrorToTopicError(err)
	}
}

// Sarama Pass-Through Function For Deleting Topics
func (k KafkaAdminClient) DeleteTopic(_ context.Context, topicName string) *sarama.TopicError {
	if k.clusterAdmin == nil {
		k.logger.Error("Unable To Delete Topic Due To Invalid ClusterAdmin - Check Kafka Authorization Secret")
		return adminutil.NewUnknownTopicError("unable to delete topic due to invalid ClusterAdmin - check Kafka authorization secrets")
	} else {
		err := k.clusterAdmin.DeleteTopic(topicName)
		return adminutil.PromoteErrorToTopicError(err)
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

// Get The K8S Secret With Kafka Credentials For The Specified Topic Name
func (k KafkaAdminClient) GetKafkaSecretName(_ string) string {
	return k.kafkaSecret
}
