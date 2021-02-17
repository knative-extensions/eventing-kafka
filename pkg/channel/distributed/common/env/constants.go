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

package env

// Package Constants
const (

	// Eventing-Kafka Configuration
	ServiceAccountEnvVarKey      = "SERVICE_ACCOUNT"
	MetricsPortEnvVarKey         = "METRICS_PORT"
	MetricsDomainEnvVarKey       = "METRICS_DOMAIN"
	HealthPortEnvVarKey          = "HEALTH_PORT"
	PodNameEnvVarKey             = "POD_NAME"
	ContainerNameEnvVarKey       = "CONTAINER_NAME"
	ResyncPeriodMinutesEnvVarKey = "RESYNC_PERIOD_MINUTES"

	// Kafka Authorization
	KafkaSecretNamespaceEnvVarKey = "KAFKA_SECRET_NAMESPACE"
	KafkaSecretNameEnvVarKey      = "KAFKA_SECRET_NAME"

	// Kafka Configuration
	KafkaTopicEnvVarKey = "KAFKA_TOPIC"

	// Knative Logging Configuration
	KnativeLoggingConfigMapNameEnvVarKey = "CONFIG_LOGGING_NAME" // Note - Matches value of configMapNameEnv constant in Knative.dev/pkg/logging !

	// Dispatcher Configuration
	ChannelKeyEnvVarKey  = "CHANNEL_KEY"
	ServiceNameEnvVarKey = "SERVICE_NAME"
)
