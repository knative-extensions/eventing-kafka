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

package producer

import (
	"github.com/Shopify/sarama"
	"github.com/rcrowley/go-metrics"
)

// Create A Sarama Kafka SyncProducer (Optional Authentication)
func CreateSyncProducer(brokers []string, config *sarama.Config) (sarama.SyncProducer, metrics.Registry, error) {

	// Create A New Sarama SyncProducer & Return Results
	syncProducer, err := newSyncProducerWrapper(brokers, config)
	return syncProducer, config.MetricRegistry, err
}

// Function Reference Variable To Facilitate Mocking In Unit Tests
var newSyncProducerWrapper = func(brokers []string, config *sarama.Config) (sarama.SyncProducer, error) {
	return sarama.NewSyncProducer(brokers, config)
}
