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
// can have their own JSON tags in the overall EventingKafkaConfig.  The user is responsible
// for providing valid values depending on use case (e.g. respecting Kubernetes label naming
// conventions) or the reconciliation of the respective resource will fail.
type EKKubernetesConfig struct {
	DeploymentAnnotations string            `json:"deploymentAnnotations,omitempty"`
	DeploymentLabels      string            `json:"deploymentLabels,omitempty"`
	PodAnnotations        string            `json:"podAnnotations,omitempty"`
	PodLabels             string            `json:"podLabels,omitempty"`
	ServiceAnnotations    string            `json:"serviceAnnotations,omitempty"`
	ServiceLabels         string            `json:"serviceLabels,omitempty"`
	CpuLimit              resource.Quantity `json:"cpuLimit,omitempty"`
	CpuRequest            resource.Quantity `json:"cpuRequest,omitempty"`
	MemoryLimit           resource.Quantity `json:"memoryLimit,omitempty"`
	MemoryRequest         resource.Quantity `json:"memoryRequest,omitempty"`
	Replicas              int               `json:"replicas,omitempty"`
}

// DeploymentAnnotationsMap returns the raw DeploymentAnnotations CSV Key=Value string as map of key/value strings.
func (c EKKubernetesConfig) DeploymentAnnotationsMap() map[string]string {
	return CsvKeyValueStringToMap(c.DeploymentAnnotations)
}

// DeploymentLabelsMap returns the raw DeploymentLabels CSV Key=Value string as map of key/value strings.
func (c EKKubernetesConfig) DeploymentLabelsMap() map[string]string {
	return CsvKeyValueStringToMap(c.DeploymentLabels)
}

// PodAnnotationsMap returns the raw PodAnnotations CSV Key=Value string as map of key/value strings.
func (c EKKubernetesConfig) PodAnnotationsMap() map[string]string {
	return CsvKeyValueStringToMap(c.PodAnnotations)
}

// PodLabelsMap returns the raw PodLabels CSV Key=Value string as map of key/value strings.
func (c EKKubernetesConfig) PodLabelsMap() map[string]string {
	return CsvKeyValueStringToMap(c.PodLabels)
}

// ServiceAnnotationsMap returns the raw ServiceAnnotations CSV Key=Value string as map of key/value strings.
func (c EKKubernetesConfig) ServiceAnnotationsMap() map[string]string {
	return CsvKeyValueStringToMap(c.ServiceAnnotations)
}

// ServiceLabelsMap returns the raw ServiceLabels CSV Key=Value string as map of key/value strings.
func (c EKKubernetesConfig) ServiceLabelsMap() map[string]string {
	return CsvKeyValueStringToMap(c.ServiceLabels)
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

// EKKafkaConfig contains items relevant to Kafka specifically
type EKKafkaConfig struct {
	Brokers             string             `json:"brokers,omitempty"`
	AuthSecretName      string             `json:"authSecretName,omitempty"`
	AuthSecretNamespace string             `json:"authSecretNamespace,omitempty"`
	Topic               EKKafkaTopicConfig `json:"topic,omitempty"`
}

// EKSourceConfig is reserved for configuration fields needed by the Kafka Source component
type EKSourceConfig struct {
}

// EKChannelConfig contains items relevant to the eventing-kafka channels
// NOTE:  Currently the consolidated channel type does not make use of most of these fields
type EKChannelConfig struct {
	Dispatcher EKDispatcherConfig `json:"dispatcher,omitempty"` // Consolidated and Distributed channels
	Receiver   EKReceiverConfig   `json:"receiver,omitempty"`   // Distributed channel only
	AdminType  string             `json:"adminType,omitempty"`  // Distributed channel only
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
	Source      EKSourceConfig          `json:"source,omitempty"`
	Auth        *client.KafkaAuthConfig `json:"-"` // Not directly part of the configmap; loaded from the secret
}
