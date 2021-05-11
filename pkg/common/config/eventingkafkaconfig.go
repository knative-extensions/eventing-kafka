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

package config

import (
	"github.com/Shopify/sarama"
	"k8s.io/apimachinery/pkg/api/resource"
	"knative.dev/eventing-kafka/pkg/common/client"
)

// EKKubernetesConfig and these EK sub-structs contain our custom configuration settings,
// stored in the config-kafka configmap.  The sub-structs are explicitly declared so that they
// can have their own JSON tags in the overall EventingKafkaConfig
type EKKubernetesConfig struct {
	CpuLimit      resource.Quantity `json:"cpuLimit,omitempty"`
	CpuRequest    resource.Quantity `json:"cpuRequest,omitempty"`
	MemoryLimit   resource.Quantity `json:"memoryLimit,omitempty"`
	MemoryRequest resource.Quantity `json:"memoryRequest,omitempty"`
	Replicas      int               `json:"replicas,omitempty"`
}

// EKReceiverConfig has the base Kubernetes fields (Cpu, Memory, Replicas) only
type EKReceiverConfig struct {
	EKKubernetesConfig
}

// EKDispatcherConfig has the base Kubernetes fields (Cpu, Memory, Replicas) only
type EKDispatcherConfig struct {
	EKKubernetesConfig
}

// EKKafkaTopicConfig contains some defaults that are only used if not provided by the channel spec
type EKKafkaTopicConfig struct {
	DefaultNumPartitions     int32 `json:"defaultNumPartitions,omitempty"`
	DefaultReplicationFactor int16 `json:"defaultReplicationFactor,omitempty"`
	DefaultRetentionMillis   int64 `json:"defaultRetentionMillis,omitempty"`
}

// EKCloudEventConfig contains the values send to the Knative cloudevents' ConfigureConnectionArgs function
// If they are not provided in the configmap, the DefaultMaxIdleConns and DefaultMaxIdleConnsPerHost constants are used
type EKCloudEventConfig struct {
	MaxIdleConns        int `json:"maxIdleConns,omitempty"`
	MaxIdleConnsPerHost int `json:"maxIdleConnsPerHost,omitempty"`
}

// EKKafkaConfig contains items relevant to Kafka specifically, and the Sarama logging flag
type EKKafkaConfig struct {
	Brokers             string             `json:"brokers,omitempty"`
	AuthSecretName      string             `json:"authSecretName,omitempty"`
	AuthSecretNamespace string             `json:"authSecretNamespace,omitempty"`
	Topic               EKKafkaTopicConfig `json:"topic,omitempty"`
}

// EKDistributedConfig holds fields needed only by the Distributed Channel component
type EKDistributedConfig struct {
	Receiver  EKReceiverConfig `json:"receiver,omitempty"`
	AdminType string           `json:"adminType,omitempty"`
}

// EKConsolidatedConfig is reserved for configuration fields needed only by the Consolidated Channel component
type EKConsolidatedConfig struct {
}

// EKSourceConfig is reserved for configuration fields needed by the Kafka Source component
type EKSourceConfig struct {
}

// EKChannelConfig contains items relevant to the eventing-kafka channels (distributed and consolidated)
type EKChannelConfig struct {
	Dispatcher   EKDispatcherConfig   `json:"dispatcher,omitempty"`
	Distributed  EKDistributedConfig  `json:"distributed,omitempty"`
	Consolidated EKConsolidatedConfig `json:"consolidated,omitempty"`
}

// EKSaramaConfig holds the sarama.Config struct (populated separately), and the global Sarama debug logging flag
type EKSaramaConfig struct {
	EnableLogging bool           `json:"enableLogging,omitempty"`
	Config        *sarama.Config `json:"-"` // Sarama config string is converted to sarama.Config struct, stored here
}

// EventingKafkaConfig is the main struct that holds the Receiver, Dispatcher, and Kafka sub-items
type EventingKafkaConfig struct {
	Channel     EKChannelConfig         `json:"channel,omitempty"`
	CloudEvents EKCloudEventConfig      `json:"cloudevents,omitempty"`
	Kafka       EKKafkaConfig           `json:"kafka,omitempty"`
	Sarama      EKSaramaConfig          `json:"sarama,omitempty"`
	Auth        *client.KafkaAuthConfig `json:"-"` // Not directly part of the configmap; stored here for convenience
}
