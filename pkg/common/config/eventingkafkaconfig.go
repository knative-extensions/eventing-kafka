package config

import (
	"k8s.io/apimachinery/pkg/api/resource"
)

// The EventingKafkaConfig and these EK sub-structs contain our custom configuration settings,
// stored in the config-kafka configmap.  The sub-structs are explicitly declared so that they
// can have their own JSON tags in the overall EventingKafkaConfig
type EKKubernetesConfig struct {
	CpuLimit      resource.Quantity `json:"cpuLimit,omitempty"`
	CpuRequest    resource.Quantity `json:"cpuRequest,omitempty"`
	MemoryLimit   resource.Quantity `json:"memoryLimit,omitempty"`
	MemoryRequest resource.Quantity `json:"memoryRequest,omitempty"`
	Replicas      int               `json:"replicas,omitempty"`
}

// The Receiver config has the base Kubernetes fields (Cpu, Memory, Replicas) only
type EKReceiverConfig struct {
	EKKubernetesConfig
}

// The Dispatcher config has the base Kubernetes fields (Cpu, Memory, Replicas) only
type EKDispatcherConfig struct {
	EKKubernetesConfig
}

// EKKafkaTopicConfig contains some defaults that are only used if not provided by the channel spec
type EKKafkaTopicConfig struct {
	DefaultNumPartitions     int32 `json:"defaultNumPartitions,omitempty"`
	DefaultReplicationFactor int16 `json:"defaultReplicationFactor,omitempty"`
	DefaultRetentionMillis   int64 `json:"defaultRetentionMillis,omitempty"`
}

// EKKafkaConfig contains items relevant to Kafka specifically, and the Sarama logging flag
type EKKafkaConfig struct {
	Brokers             string             `json:"brokers,omitempty"`
	EnableSaramaLogging bool               `json:"enableSaramaLogging,omitempty"`
	Topic               EKKafkaTopicConfig `json:"topic,omitempty"`
	AdminType           string             `json:"adminType,omitempty"`
}

// EventingKafkaConfig is the main struct that holds the Receiver, Dispatcher, and Kafka sub-items
type EventingKafkaConfig struct {
	Receiver   EKReceiverConfig   `json:"receiver,omitempty"`
	Dispatcher EKDispatcherConfig `json:"dispatcher,omitempty"`
	Kafka      EKKafkaConfig      `json:"kafka,omitempty"`
}
