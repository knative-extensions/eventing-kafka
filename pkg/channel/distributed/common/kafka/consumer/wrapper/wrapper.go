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

package wrapper

import (
	"github.com/Shopify/sarama"
)

// Define Function Types For Wrapper Variables (Typesafe Stubbing For Tests)
type NewConsumerGroupFnType = func(brokers []string, groupId string, config *sarama.Config) (sarama.ConsumerGroup, error)

// Function Variables To Facilitate Mocking Of Sarama Functionality In Unit Tests
var NewConsumerGroupFn = SaramaNewConsumerGroupWrapper

// The Production Sarama NewConsumerGroup Wrapper Function
func SaramaNewConsumerGroupWrapper(brokers []string, groupId string, config *sarama.Config) (sarama.ConsumerGroup, error) {
	return sarama.NewConsumerGroup(brokers, groupId, config)
}
