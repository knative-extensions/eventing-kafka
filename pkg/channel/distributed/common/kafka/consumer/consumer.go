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

package consumer

import (
	"github.com/Shopify/sarama"
	"github.com/rcrowley/go-metrics"
)

// Create A Kafka ConsumerGroup (Optional SASL Authentication)
func CreateConsumerGroup(brokers []string, config *sarama.Config, groupId string) (sarama.ConsumerGroup, metrics.Registry, error) {

	// Create A New Sarama ConsumerGroup & Return Results
	consumerGroup, err := NewConsumerGroupWrapper(brokers, groupId, config)
	return consumerGroup, config.MetricRegistry, err
}

// Function Reference Variable To Facilitate Mocking In Unit Tests
var NewConsumerGroupWrapper = func(brokers []string, groupId string, config *sarama.Config) (sarama.ConsumerGroup, error) {
	return sarama.NewConsumerGroup(brokers, groupId, config)
}
