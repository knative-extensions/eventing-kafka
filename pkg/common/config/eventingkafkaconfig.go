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
	"k8s.io/apimachinery/pkg/api/resource"
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
	EnableSaramaLogging bool               `json:"enableSaramaLogging,omitempty"`
	Topic               EKKafkaTopicConfig `json:"topic,omitempty"`
	AdminType           string             `json:"adminType,omitempty"`
	AuthSecretName      string             `json:"authSecretName,omitempty"`
	AuthSecretNamespace string             `json:"authSecretNamespace,omitempty"`
}

// EventingKafkaConfig is the main struct that holds the Receiver, Dispatcher, and Kafka sub-items
type EventingKafkaConfig struct {
	Receiver    EKReceiverConfig   `json:"receiver,omitempty"`
	Dispatcher  EKDispatcherConfig `json:"dispatcher,omitempty"`
	CloudEvents EKCloudEventConfig `json:"cloudevents,omitempty"`
	Kafka       EKKafkaConfig      `json:"kafka,omitempty"`
}
