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

	"github.com/Shopify/sarama"
	"knative.dev/eventing-kafka/pkg/channel/consolidated/reconciler/controller/resources"
	"knative.dev/eventing-kafka/pkg/channel/consolidated/utils"
	"knative.dev/pkg/logging"
)

// AdminClientManager manages a ClusterAdmin connection and recreates one when needed
// it is made to overcome https://github.com/Shopify/sarama/issues/1162
type AdminClientManager struct {
	ca sarama.ClusterAdmin
}

func NewAdminClientManager(ctx context.Context, clientID string,
	kc utils.KafkaConfig) (AdminClientManager, error) {
	logger := logging.FromContext(ctx)
	kafkaClusterAdmin, err := resources.MakeClient(clientID, kc.Brokers)
	if err != nil {
		logger.Error("error while creating ClusterAdmin", err)
		return AdminClientManager{}, err
	}
	return AdminClientManager{
		kafkaClusterAdmin,
	}, nil
}

func (c *AdminClientManager) ListConsumerGroups() (map[string]string, error) {
	//TODO add reconnect
	return c.ca.ListConsumerGroups()
}
