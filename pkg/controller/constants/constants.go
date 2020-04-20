package constants

const (
	// Knative Eventing Namespace
	KnativeEventingNamespace = "knative-eventing"

	// Knative Controller Naming
	KafkaChannelControllerAgentName = "kafka-channel-controller"
	KafkaSecretControllerAgentName  = "kafka-secret-controller"

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
	// Refer to: https://github.com/kyma-incubator/knative-kafka/blob/master/cmd/channel/main.go
	HttpContainerPortNumber = 8080

	// Kafka Secret Data Keys
	KafkaSecretDataKeyBrokers  = "brokers"
	KafkaSecretDataKeyUsername = "username"
	KafkaSecretDataKeyPassword = "password"

	// Prometheus MetricsPort
	MetricsPortName = "metrics"

	// Reconciliation Error Messages
	ReconciliationFailedError = "reconciliation failed"

	// KnativeKafka Finalizers Prefix
	KnativeKafkaFinalizerPrefix = "knative-kafka/"

	// Labels
	AppLabel                    = "app"
	KafkaChannelNameLabel       = "kafkachannel-name"
	KafkaChannelNamespaceLabel  = "kafkachannel-namespace"
	KafkaChannelChannelLabel    = "kafkachannel-channel"    // Channel Label - Used To Mark Deployment As Related To Channel
	KafkaChannelDispatcherLabel = "kafkachannel-dispatcher" // Dispatcher Label - Used To Mark Deployment As Dispatcher
	KafkaSecretLabel            = "kafkasecret"             // Secret Label - Indicates The Kafka Secret Of The KafkaChannel
	KafkaTopicLabel             = "kafkaTopic"              // Topic Label - Indicates The Kafka Topic Of The KnativeChannel

	// Prometheus ServiceMonitor Selector Labels / Values
	K8sAppChannelSelectorLabel    = "k8s-app"
	K8sAppChannelSelectorValue    = "knative-kafka-channels"
	K8sAppDispatcherSelectorLabel = "k8s-app"
	K8sAppDispatcherSelectorValue = "knative-kafka-dispatchers"

	// Kafka Topic Configuration
	KafkaTopicConfigRetentionMs = "retention.ms"

	// Health Configuration
	HealthPort                = 8082
	ChannelLivenessDelay      = 10
	ChannelLivenessPeriod     = 5
	ChannelReadinessDelay     = 10
	ChannelReadinessPeriod    = 5
	DispatcherLivenessDelay   = 10
	DispatcherLivenessPeriod  = 5
	DispatcherReadinessDelay  = 10
	DispatcherReadinessPeriod = 5
)
