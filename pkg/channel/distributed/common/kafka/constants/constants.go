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
	KafkaSecretKeyNamespace = "namespace"
	KafkaSecretKeyUsername  = "username"
	KafkaSecretKeyPassword  = "password"
	KafkaSecretKeySaslType  = "sasltype"

	// Kafka Topic Config Keys
	TopicDetailConfigRetentionMs = "retention.ms"

	// EventHub Error Codes
	EventHubErrorCodeUnknown       = -2
	EventHubErrorCodeParseFailure  = -1
	EventHubErrorCodeCapacityLimit = 403
	EventHubErrorCodeConflict      = 409

	// KafkaChannel Constants
	KafkaChannelServiceNameSuffix = "kn-channel" // Specific Value For Use With Knative e2e Tests!
)

// Non-Constant Constants ;)
var (
	//
	// Default Kafka Version
	//
	// This is the default value which will be used when creating Sarama.Config if not
	// otherwise specified in the ConfigMap.  It is set to the lowest common denominator
	// version to provide the most compatible and likely to succeed solution.  Specifically,
	// Sarama's ConsumerGroups repeatedly close due to EOF failures when working against
	// Azure EventHubs if this is set any higher than V1_0_0_0.
	//
	ConfigKafkaVersionDefault = sarama.V1_0_0_0
)
