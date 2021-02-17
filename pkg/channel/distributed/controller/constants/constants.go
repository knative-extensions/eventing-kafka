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

const (

	// Kafka Admin Type Types
	KafkaAdminTypeValueKafka  = "kafka"
	KafkaAdminTypeValueAzure  = "azure"
	KafkaAdminTypeValueCustom = "custom"

	// The Controller's Component Name (Needs To Be DNS Safe!)
	ControllerComponentName = "eventing-kafka-channel-controller"

	// Knative Duck Versions
	SubscribableDuckVersionAnnotationV1 = "v1"

	// CRD Kinds
	SecretKind              = "Secret"
	ServiceKind             = "Service"
	DeploymentKind          = "Deployment"
	KnativeSubscriptionKind = "Subscription"
	KafkaChannelKind        = "KafkaChannel"

	// HTTP Port
	HttpPortName = "http"
	// IMPORTANT: HttpServicePort is the inbound port of the service resource. It must be 80 because the
	// Channel resource's url doesn't currently have a port set. Therefore, any client using just the url
	// will send to port 80 by default.
	HttpServicePortNumber = 80
	// IMPORTANT: HttpContainerPortNumber must be 8080 due to dependency issues in the channel. This variable
	// is necessary in order to reconcile the channel resources (service, deployment, etc) correctly.
	// Refer to: https://knative.dev/eventing-kafka/blob/master/cmd/channel/main.go
	HttpContainerPortNumber = 8080

	// Prometheus MetricsPort
	MetricsPortName = "metrics"

	// Reconciliation Error Messages
	ReconciliationFailedError = "reconciliation failed"
	FinalizationFailedError   = "finalization failed"

	// Eventing-Kafka Finalizers Prefix
	EventingKafkaFinalizerPrefix = "eventing-kafka/"
	KafkaChannelFinalizerSuffix  = "kafkachannels.messaging.knative.dev" // Matches default value in client/injection/reconciler/messaging/v1beta1/kafkachannel
	KafkaSecretFinalizerSuffix   = "kafkasecrets.eventing-kafka.knative.dev"

	// Container Names
	DispatcherContainerName = "kafkachannel-dispatcher"
	ReceiverContainerName   = "kafkachannel-receiver"

	// Labels
	AppLabel                    = "app"
	KafkaChannelNameLabel       = "kafkachannel-name"
	KafkaChannelNamespaceLabel  = "kafkachannel-namespace"
	KafkaChannelReceiverLabel   = "kafkachannel-receiver"   // Receiver Label - Used To Mark Deployment As Receiver
	KafkaChannelDispatcherLabel = "kafkachannel-dispatcher" // Dispatcher Label - Used To Mark Deployment As Dispatcher
	KafkaSecretLabel            = "kafkasecret"             // Secret Label - Indicates The Kafka Secret Of The KafkaChannel
	KafkaTopicLabel             = "kafkaTopic"              // Topic Label - Indicates The Kafka Topic Of The KnativeChannel

	// Prometheus ServiceMonitor Selector Labels / Values
	K8sAppChannelSelectorLabel    = "k8s-app"
	K8sAppChannelSelectorValue    = "eventing-kafka-channels"
	K8sAppDispatcherSelectorLabel = "k8s-app"
	K8sAppDispatcherSelectorValue = "eventing-kafka-dispatchers"

	// Kafka Topic Configuration
	KafkaTopicConfigRetentionMs = "retention.ms"

	// Health Configuration
	HealthPort                = 8082
	ChannelLivenessDelay      = 30
	ChannelLivenessPeriod     = 5
	ChannelReadinessDelay     = 30
	ChannelReadinessPeriod    = 5
	DispatcherLivenessDelay   = 30
	DispatcherLivenessPeriod  = 5
	DispatcherReadinessDelay  = 30
	DispatcherReadinessPeriod = 5
)
