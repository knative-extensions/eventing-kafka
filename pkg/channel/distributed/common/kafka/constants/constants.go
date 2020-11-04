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

package constants

import (
	"github.com/Shopify/sarama"
)

// Constants
const (
	// Duration Convenience
	MillisPerDay = 24 * 60 * 60 * 1000 // 86400000

	// Kafka Secret Label
	KafkaSecretLabel = "eventing-kafka.knative.dev/kafka-secret"

	// Kafka Secret Keys
	KafkaSecretKeyBrokers   = "brokers"
	KafkaSecretKeyNamespace = "namespace"
	KafkaSecretKeyUsername  = "username"
	KafkaSecretKeyPassword  = "password"

	// Kafka Admin/Consumer/Producer Config Values
	ConfigNetSaslVersion = sarama.SASLHandshakeV1 // Latest version, seems to work with EventHubs as well.

	// Kafka Topic Config Keys
	TopicDetailConfigRetentionMs = "retention.ms"

	// EventHub Error Codes
	EventHubErrorCodeUnknown       = -2
	EventHubErrorCodeParseFailure  = -1
	EventHubErrorCodeCapacityLimit = 403
	EventHubErrorCodeConflict      = 409

	// EventHub Constraints
	MaxEventHubNamespaces = 100

	// KafkaChannel Constants
	KafkaChannelServiceNameSuffix = "kn-channel" // Specific Value For Use With Knative e2e Tests!
)
