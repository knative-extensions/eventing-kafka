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

package testing

import (
	"fmt"
	"strconv"
	"time"

	"k8s.io/utils/pointer"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/intstr"
	clientgotesting "k8s.io/client-go/testing"
	kafkav1beta1 "knative.dev/eventing-kafka/pkg/apis/messaging/v1beta1"
	commonenv "knative.dev/eventing-kafka/pkg/channel/distributed/common/env"
	"knative.dev/eventing-kafka/pkg/channel/distributed/common/health"
	kafkaconstants "knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/constants"
	kafkautil "knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/util"
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/constants"
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/env"
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/event"
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/util"
	commonconfig "knative.dev/eventing-kafka/pkg/common/config"
	commontesting "knative.dev/eventing-kafka/pkg/common/testing"
	"knative.dev/eventing/pkg/apis/messaging"
	"knative.dev/pkg/apis"
	"knative.dev/pkg/logging"
	reconcilertesting "knative.dev/pkg/reconciler/testing"
	"knative.dev/pkg/system"
)

// Constants
const (
	// Prometheus MetricsPort
	MetricsPortName = "metrics"

	// Environment Test Data
	ServiceAccount           = "TestServiceAccount"
	KafkaAdminType           = "kafka"
	MetricsPort              = 9876
	MetricsDomain            = "eventing-kafka"
	HealthPort               = 8082
	ResyncPeriod             = 3600 * time.Minute
	ReceiverImage            = "TestReceiverImage"
	ReceiverReplicas         = 1
	DispatcherImage          = "TestDispatcherImage"
	DispatcherReplicas       = 1
	DefaultNumPartitions     = 4
	DefaultReplicationFactor = 1
	DefaultRetentionMillis   = 99999

	// Channel Test Data
	KafkaChannelNamespace  = "kafkachannel-namespace"
	KafkaChannelName       = "kafkachannel-name"
	KafkaChannelKey        = KafkaChannelNamespace + "/" + KafkaChannelName
	KafkaSecretNamespace   = "eventing-test-ns" // Needs To Match system.Namespace() Call In Reconciliation
	KafkaSecretName        = "kafkasecret-name"
	KafkaSecretKey         = KafkaSecretNamespace + "/" + KafkaSecretName
	ReceiverDeploymentName = KafkaSecretName + "-b9176d5f-receiver" // Truncated MD5 Hash Of KafkaSecretName
	ReceiverServiceName    = ReceiverDeploymentName
	TopicName              = KafkaChannelNamespace + "." + KafkaChannelName

	KafkaSecretDataValueBrokers  = "TestKafkaSecretDataBrokers"
	KafkaSecretDataValueUsername = "TestKafkaSecretDataUsername"
	KafkaSecretDataValuePassword = "TestKafkaSecretDataPassword"
	KafkaSecretDataValueSaslType = "PLAIN"

	// ChannelSpec Test Data
	NumPartitions     = 123
	ReplicationFactor = 456

	// Test MetaData
	ErrorString   = "Expected Mock Test Error"
	SuccessString = "Expected Mock Test Success"

	// Test Dispatcher Resources
	DispatcherMemoryRequest = "20Mi"
	DispatcherCpuRequest    = "100m"
	DispatcherMemoryLimit   = "50Mi"
	DispatcherCpuLimit      = "300m"

	// Test Receiver Resources
	ReceiverMemoryRequest = "10Mi"
	ReceiverMemoryLimit   = "20Mi"
	ReceiverCpuRequest    = "10m"
	ReceiverCpuLimit      = "100m"

	ControllerConfigYaml = `
receiver:
  cpuLimit: 200m
  cpuRequest: 100m
  memoryLimit: 100Mi
  memoryRequest: 50Mi
  replicas: 1
dispatcher:
  cpuLimit: 500m
  cpuRequest: 300m
  memoryLimit: 128Mi
  memoryRequest: 50Mi
  replicas: 1
  retryInitialIntervalMillis: 500
  retryTimeMillis: 300000
  retryExponentialBackoff: true
kafka:
  enableSaramaLogging: false
  topic:
    defaultNumPartitions: 4
    defaultReplicationFactor: 1
    defaultRetentionMillis: 604800000
  adminType: kafka
`
	SaramaConfigYaml = `
Version: 2.0.0
Admin:
  Timeout: 10000000000  # 10 seconds
Net:
  KeepAlive: 30000000000  # 30 seconds
  MaxOpenRequests: 1 # Set to 1 for use with Idempotent Producer
  TLS:
    Enable: false
  SASL:
    Enable: false
    Mechanism: PLAIN
    Version: 1
Metadata:
  RefreshFrequency: 300000000000
Consumer:
  Offsets:
    AutoCommit:
        Interval: 5000000000
    Retention: 604800000000000
Producer:
  Idempotent: true
  RequiredAcks: -1
`
)

var (
	DefaultRetentionMillisString = strconv.FormatInt(DefaultRetentionMillis, 10)
	DeletionTimestamp            = metav1.Now()
)

//
// Utility Data Creation Functions
//

// Service / Deployment Options For Customizing Test Data
type ServiceOption func(service *corev1.Service)
type DeploymentOption func(service *appsv1.Deployment)

// Set The Service's DeletionTimestamp To Current Time
func WithDeletionTimestampService(service *corev1.Service) {
	service.ObjectMeta.SetDeletionTimestamp(&DeletionTimestamp)
}

// Set The Deployment's DeletionTimestamp To Current Time
func WithDeletionTimestampDeployment(deployment *appsv1.Deployment) {
	deployment.ObjectMeta.SetDeletionTimestamp(&DeletionTimestamp)
}

// Clear The Specified Service's Finalizers
func WithoutFinalizersService(service *corev1.Service) {
	service.ObjectMeta.Finalizers = []string{}
}

// Clear The Dispatcher Deployment's Finalizers
func WithoutFinalizersDeployment(deployment *appsv1.Deployment) {
	deployment.ObjectMeta.Finalizers = []string{}
}

func WithoutResources(deployment *appsv1.Deployment) {
	deployment.Spec.Template.Spec.Containers[0].Resources.Limits = nil
	deployment.Spec.Template.Spec.Containers[0].Resources.Requests = nil
}

func WithDifferentName(deployment *appsv1.Deployment) {
	deployment.Spec.Template.Spec.Containers[0].Name = "DifferentName"
}

func WithDifferentImage(deployment *appsv1.Deployment) {
	deployment.Spec.Template.Spec.Containers[0].Image = "DifferentImage"
}

func WithDifferentCommand(deployment *appsv1.Deployment) {
	deployment.Spec.Template.Spec.Containers[0].Command = []string{"DifferentCommand"}
}

func WithDifferentArgs(deployment *appsv1.Deployment) {
	deployment.Spec.Template.Spec.Containers[0].Args = []string{"DifferentArgs"}
}

func WithDifferentWorkingDir(deployment *appsv1.Deployment) {
	deployment.Spec.Template.Spec.Containers[0].WorkingDir = "DifferentWorkingDir"
}

func WithDifferentPorts(deployment *appsv1.Deployment) {
	deployment.Spec.Template.Spec.Containers[0].Ports = []corev1.ContainerPort{
		{Name: "DifferentPortName"},
	}
}

func WithMissingEnvironment(deployment *appsv1.Deployment) {
	deployment.Spec.Template.Spec.Containers[0].Env = []corev1.EnvVar{}
}

func WithDifferentEnvironment(deployment *appsv1.Deployment) {
	deployment.Spec.Template.Spec.Containers[0].Env = append(
		deployment.Spec.Template.Spec.Containers[0].Env,
		corev1.EnvVar{Name: "NewEnvName", Value: "NewEnvValue"},
	)
}

func WithDifferentVolumeMounts(deployment *appsv1.Deployment) {
	deployment.Spec.Template.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{
		{Name: "DifferentVolumeMount"},
	}
}

func WithDifferentVolumeDevices(deployment *appsv1.Deployment) {
	deployment.Spec.Template.Spec.Containers[0].VolumeDevices = []corev1.VolumeDevice{
		{Name: "DifferentVolumeDevice"},
	}
}

func WithDifferentLivenessProbe(deployment *appsv1.Deployment) {
	deployment.Spec.Template.Spec.Containers[0].LivenessProbe = &corev1.Probe{Handler: corev1.Handler{}}
}

func WithDifferentReadinessProbe(deployment *appsv1.Deployment) {
	deployment.Spec.Template.Spec.Containers[0].ReadinessProbe = &corev1.Probe{Handler: corev1.Handler{}}
}

func WithDifferentStartupProbe(deployment *appsv1.Deployment) {
	deployment.Spec.Template.Spec.Containers[0].StartupProbe = &corev1.Probe{Handler: corev1.Handler{}}
}

func WithDifferentLifecycle(deployment *appsv1.Deployment) {
	deployment.Spec.Template.Spec.Containers[0].Lifecycle = &corev1.Lifecycle{PostStart: &corev1.Handler{}}
}

func WithDifferentTerminationPath(deployment *appsv1.Deployment) {
	deployment.Spec.Template.Spec.Containers[0].TerminationMessagePath = "DifferentTerminationPath"
}

func WithDifferentTerminationPolicy(deployment *appsv1.Deployment) {
	deployment.Spec.Template.Spec.Containers[0].TerminationMessagePolicy = corev1.TerminationMessageFallbackToLogsOnError
}

func WithDifferentImagePullPolicy(deployment *appsv1.Deployment) {
	deployment.Spec.Template.Spec.Containers[0].ImagePullPolicy = corev1.PullNever
}

func WithDifferentSecurityContext(deployment *appsv1.Deployment) {
	deployment.Spec.Template.Spec.Containers[0].SecurityContext = &corev1.SecurityContext{}
}

func WithDifferentReplicas(deployment *appsv1.Deployment) {
	deployment.Spec.Replicas = pointer.Int32Ptr(10)
}

func WithoutLabels(deployment *appsv1.Deployment) {
	deployment.ObjectMeta.Labels = map[string]string{}
}

func WithExtraLabels(deployment *appsv1.Deployment) {
	deployment.ObjectMeta.Labels["ExtraLabelName"] = "ExtraLabelValue"
}

//
// ControllerConfig Test Data
//

// Set The Required Environment Variables
func NewEnvironment() *env.Environment {
	return &env.Environment{
		SystemNamespace: commontesting.SystemNamespace,
		ServiceAccount:  ServiceAccount,
		MetricsPort:     MetricsPort,
		MetricsDomain:   MetricsDomain,
		DispatcherImage: DispatcherImage,
		ReceiverImage:   ReceiverImage,
		ResyncPeriod:    ResyncPeriod,
	}
}

// KafkaConfigOption Enables Customization Of An EventingKafkaConfig
type KafkaConfigOption func(kafkaConfig *commonconfig.EventingKafkaConfig)

// Set The Required Config Fields
func NewConfig(options ...KafkaConfigOption) *commonconfig.EventingKafkaConfig {
	kafkaConfig := &commonconfig.EventingKafkaConfig{
		Dispatcher: commonconfig.EKDispatcherConfig{
			EKKubernetesConfig: commonconfig.EKKubernetesConfig{
				Replicas:      DispatcherReplicas,
				CpuLimit:      resource.MustParse(DispatcherCpuLimit),
				CpuRequest:    resource.MustParse(DispatcherCpuRequest),
				MemoryLimit:   resource.MustParse(DispatcherMemoryLimit),
				MemoryRequest: resource.MustParse(DispatcherMemoryRequest),
			},
		},
		Receiver: commonconfig.EKReceiverConfig{
			EKKubernetesConfig: commonconfig.EKKubernetesConfig{
				Replicas:      ReceiverReplicas,
				CpuLimit:      resource.MustParse(ReceiverCpuLimit),
				CpuRequest:    resource.MustParse(ReceiverCpuRequest),
				MemoryLimit:   resource.MustParse(ReceiverMemoryLimit),
				MemoryRequest: resource.MustParse(ReceiverMemoryRequest),
			},
		},
		Kafka: commonconfig.EKKafkaConfig{
			Topic: commonconfig.EKKafkaTopicConfig{
				DefaultNumPartitions:     DefaultNumPartitions,
				DefaultReplicationFactor: DefaultReplicationFactor,
				DefaultRetentionMillis:   DefaultRetentionMillis,
			},
			AdminType: KafkaAdminType,
		},
	}

	// Apply The Specified Kafka Config Customizations
	for _, option := range options {
		option(kafkaConfig)
	}

	return kafkaConfig
}

// Remove The Receiver Resource Requests And Limits
func WithNoReceiverResources(kafkaConfig *commonconfig.EventingKafkaConfig) {
	kafkaConfig.Receiver.EKKubernetesConfig.CpuLimit = resource.Quantity{}
	kafkaConfig.Receiver.EKKubernetesConfig.CpuRequest = resource.Quantity{}
	kafkaConfig.Receiver.EKKubernetesConfig.MemoryLimit = resource.Quantity{}
	kafkaConfig.Receiver.EKKubernetesConfig.MemoryRequest = resource.Quantity{}
}

// Remove The Dispatcher Resource Requests And Limits
func WithNoDispatcherResources(kafkaConfig *commonconfig.EventingKafkaConfig) {
	kafkaConfig.Dispatcher.EKKubernetesConfig.CpuLimit = resource.Quantity{}
	kafkaConfig.Dispatcher.EKKubernetesConfig.CpuRequest = resource.Quantity{}
	kafkaConfig.Dispatcher.EKKubernetesConfig.MemoryLimit = resource.Quantity{}
	kafkaConfig.Dispatcher.EKKubernetesConfig.MemoryRequest = resource.Quantity{}
}

//
// Kafka Secret Resources
//

// KafkaSecretOption Enables Customization Of A KafkaChannel
type KafkaSecretOption func(secret *corev1.Secret)

// Create A New Kafka Auth Secret For Testing
func NewKafkaSecret(options ...KafkaSecretOption) *corev1.Secret {

	// Create The Specified Kafka Secret
	secret := &corev1.Secret{
		TypeMeta: metav1.TypeMeta{
			Kind:       constants.SecretKind,
			APIVersion: corev1.SchemeGroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      KafkaSecretName,
			Namespace: KafkaSecretNamespace,
			Labels:    map[string]string{kafkaconstants.KafkaSecretLabel: "true"},
		},
		Data: map[string][]byte{
			kafkaconstants.KafkaSecretKeyUsername: []byte(KafkaSecretDataValueUsername),
			kafkaconstants.KafkaSecretKeyPassword: []byte(KafkaSecretDataValuePassword),
			kafkaconstants.KafkaSecretKeySaslType: []byte(KafkaSecretDataValueSaslType),
		},
		Type: "opaque",
	}

	// Apply The Specified Kafka secret Customizations
	for _, option := range options {
		option(secret)
	}

	// Return The Test Kafka Secret
	return secret
}

// Set The Kafka Secret's DeletionTimestamp To Current Time
func WithKafkaSecretDeleted(secret *corev1.Secret) {
	secret.ObjectMeta.SetDeletionTimestamp(&DeletionTimestamp)
}

// Set The Kafka Secret's Finalizer
func WithKafkaSecretFinalizer(secret *corev1.Secret) {
	secret.ObjectMeta.Finalizers = []string{constants.EventingKafkaFinalizerPrefix + "kafkasecrets.eventing-kafka.knative.dev"}
}

// Utility Function For Creating A PatchActionImpl For The Finalizer Patch Command
func NewKafkaSecretFinalizerPatchActionImpl() clientgotesting.PatchActionImpl {
	return clientgotesting.PatchActionImpl{
		ActionImpl: clientgotesting.ActionImpl{
			Namespace:   KafkaSecretNamespace,
			Verb:        "patch",
			Resource:    schema.GroupVersionResource{Group: corev1.SchemeGroupVersion.Group, Version: corev1.SchemeGroupVersion.Version, Resource: "secrets"},
			Subresource: "",
		},
		Name:      KafkaSecretName,
		PatchType: "application/merge-patch+json",
		Patch:     []byte(`{"metadata":{"finalizers":["eventing-kafka/kafkasecrets.eventing-kafka.knative.dev"],"resourceVersion":""}}`),
	}
}

// Utility Function For Creating A Successful Kafka Secret Reconciled Event
func NewKafkaSecretSuccessfulReconciliationEvent() string {
	return reconcilertesting.Eventf(corev1.EventTypeNormal, event.KafkaSecretReconciled.String(), fmt.Sprintf("Kafka Secret Reconciled Successfully: \"%s/%s\"", KafkaSecretNamespace, KafkaSecretName))
}

// Utility Function For Creating A Failed Kafka secret Reconciled Event
func NewKafkaSecretFailedReconciliationEvent() string {
	return reconcilertesting.Eventf(corev1.EventTypeWarning, "InternalError", constants.ReconciliationFailedError)
}

// Utility Function For Creating A Successful Kafka Secret Finalizer Update Event
func NewKafkaSecretSuccessfulFinalizedEvent() string {
	return reconcilertesting.Eventf(corev1.EventTypeNormal, event.KafkaSecretFinalized.String(), fmt.Sprintf("Kafka Secret Finalized Successfully: \"%s/%s\"", KafkaSecretNamespace, KafkaSecretName))
}

// Utility Function For Creating A Successful Kafka Secret Finalizer Update Event
func NewKafkaSecretFinalizerUpdateEvent() string {
	return reconcilertesting.Eventf(corev1.EventTypeNormal, "FinalizerUpdate", `Updated "%s" finalizers`, KafkaSecretName)
}

//
// KafkaChannel Resources
//

// KafkaChannelOption Enables Customization Of A KafkaChannel
type KafkaChannelOption func(*kafkav1beta1.KafkaChannel)

// Utility Function For Creating A Custom KafkaChannel For Testing
func NewKafkaChannel(options ...KafkaChannelOption) *kafkav1beta1.KafkaChannel {

	// Create The Specified KafkaChannel
	kafkachannel := &kafkav1beta1.KafkaChannel{
		TypeMeta: metav1.TypeMeta{
			APIVersion: kafkav1beta1.SchemeGroupVersion.String(),
			Kind:       constants.KafkaChannelKind,
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: KafkaChannelNamespace,
			Name:      KafkaChannelName,
		},
		Spec: kafkav1beta1.KafkaChannelSpec{
			NumPartitions:     NumPartitions,
			ReplicationFactor: ReplicationFactor,
			// TODO RetentionMillis:   RetentionMillis,
		},
	}

	// Apply The Specified KafkaChannel Customizations
	for _, option := range options {
		option(kafkachannel)
	}

	// Return The Test KafkaChannel
	return kafkachannel
}

// Set The KafkaChannel's Status To Initialized State
func WithInitializedConditions(kafkachannel *kafkav1beta1.KafkaChannel) {
	kafkachannel.Status.InitializeConditions()
	kafkachannel.Status.MarkConfigTrue()
}

// Set The KafkaChannel's DeletionTimestamp To Current Time
func WithDeletionTimestamp(kafkachannel *kafkav1beta1.KafkaChannel) {
	kafkachannel.ObjectMeta.SetDeletionTimestamp(&DeletionTimestamp)
}

// Set The KafkaChannel's Finalizer
func WithFinalizer(kafkachannel *kafkav1beta1.KafkaChannel) {
	kafkachannel.ObjectMeta.Finalizers = []string{constants.KafkaChannelFinalizerSuffix}
}

// Set The KafkaChannel's MetaData
func WithMetaData(kafkachannel *kafkav1beta1.KafkaChannel) {
	WithAnnotations(kafkachannel)
	WithLabels(kafkachannel)
}

// Set The KafkaChannel's Annotations
func WithAnnotations(kafkachannel *kafkav1beta1.KafkaChannel) {
	kafkachannel.ObjectMeta.Annotations = map[string]string{
		messaging.SubscribableDuckVersionAnnotation: constants.SubscribableDuckVersionAnnotationV1,
	}
}

// Set The KafkaChannel's Labels
func WithLabels(kafkachannel *kafkav1beta1.KafkaChannel) {
	kafkachannel.ObjectMeta.Labels = map[string]string{
		constants.KafkaTopicLabel:  fmt.Sprintf("%s.%s", KafkaChannelNamespace, KafkaChannelName),
		constants.KafkaSecretLabel: KafkaSecretName,
	}
}

// Set The KafkaChannel's Address
func WithAddress(kafkachannel *kafkav1beta1.KafkaChannel) {
	kafkachannel.Status.SetAddress(&apis.URL{
		Scheme: "http",
		Host:   fmt.Sprintf("%s-%s.%s.svc.cluster.local", KafkaChannelName, kafkaconstants.KafkaChannelServiceNameSuffix, KafkaChannelNamespace),
	})
}

// Set The KafkaChannel's Service As READY
func WithKafkaChannelServiceReady(kafkachannel *kafkav1beta1.KafkaChannel) {
	kafkachannel.Status.MarkChannelServiceTrue()
}

// Set The KafkaChannel's Services As Failed
func WithKafkaChannelServiceFailed(kafkachannel *kafkav1beta1.KafkaChannel) {
	kafkachannel.Status.MarkChannelServiceFailed(event.KafkaChannelServiceReconciliationFailed.String(), "Failed To Create KafkaChannel Service: inducing failure for create services")
}

// Set The KafkaChannel's Receiver Service As READY
func WithReceiverServiceReady(kafkachannel *kafkav1beta1.KafkaChannel) {
	kafkachannel.Status.MarkServiceTrue()
}

// Set The KafkaChannel's Receiver Service As Failed
func WithReceiverServiceFailed(kafkachannel *kafkav1beta1.KafkaChannel) {
	kafkachannel.Status.MarkServiceFailed(event.ReceiverServiceReconciliationFailed.String(), "Receiver Service Failed: inducing failure for create services")
}

// Set The KafkaChannel's Receiver Service As Finalized
func WithReceiverServiceFinalized(kafkachannel *kafkav1beta1.KafkaChannel) {
	kafkachannel.Status.MarkServiceFailed("ChannelServiceUnavailable", "Kafka Auth Secret Finalized")
}

// Set The KafkaChannel's Receiver Deployment As READY
func WithReceiverDeploymentReady(kafkachannel *kafkav1beta1.KafkaChannel) {
	kafkachannel.Status.MarkEndpointsTrue()
}

// Set The KafkaChannel's Receiver Deployment As Failed
func WithReceiverDeploymentFailed(kafkachannel *kafkav1beta1.KafkaChannel) {
	kafkachannel.Status.MarkEndpointsFailed(event.ReceiverDeploymentReconciliationFailed.String(), "Receiver Deployment Failed: inducing failure for create deployments")
}

// Set The KafkaChannel's Receiver Deployment As Finalized
func WithReceiverDeploymentFinalized(kafkachannel *kafkav1beta1.KafkaChannel) {
	kafkachannel.Status.MarkEndpointsFailed("ChannelDeploymentUnavailable", "Kafka Auth Secret Finalized")
}

// Set The KafkaChannel's Dispatcher Deployment As READY
func WithDispatcherDeploymentReady(_ *kafkav1beta1.KafkaChannel) {
	// TODO - This is unnecessary since the testing framework doesn't return any Status Conditions from the K8S commands (Create, Get)
	//        which means the propagate function doesn't do anything.  This is a testing gap with the framework and propagateDispatcherStatus()
	// kafkachannel.Status.PropagateDispatcherStatus()
}

// Set The KafkaChannel's Dispatcher Deployment As Failed
func WithDispatcherFailed(kafkachannel *kafkav1beta1.KafkaChannel) {
	kafkachannel.Status.MarkDispatcherFailed(event.DispatcherDeploymentReconciliationFailed.String(), "Failed To Create Dispatcher Deployment: inducing failure for create deployments")
}

// Set The KafkaChannel's Dispatcher Update As Failed
func WithDispatcherUpdateFailed(kafkachannel *kafkav1beta1.KafkaChannel) {
	kafkachannel.Status.MarkDispatcherFailed(event.DispatcherDeploymentUpdateFailed.String(), "Failed To Update Dispatcher Deployment: inducing failure for update deployments")
}

// Set The KafkaChannel's Topic READY
func WithTopicReady(kafkachannel *kafkav1beta1.KafkaChannel) {
	kafkachannel.Status.MarkTopicTrue()
}

// Utility Function For Creating A Custom KafkaChannel "Channel" Service For Testing
func NewKafkaChannelService(options ...ServiceOption) *corev1.Service {

	// Create The KafkaChannel Service
	service := &corev1.Service{
		TypeMeta: metav1.TypeMeta{
			APIVersion: corev1.SchemeGroupVersion.String(),
			Kind:       constants.ServiceKind,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      kafkautil.AppendKafkaChannelServiceNameSuffix(KafkaChannelName),
			Namespace: KafkaChannelNamespace,
			Labels: map[string]string{
				constants.KafkaChannelNameLabel:      KafkaChannelName,
				constants.KafkaChannelNamespaceLabel: KafkaChannelNamespace,
				constants.KafkaChannelReceiverLabel:  "true",
				constants.K8sAppChannelSelectorLabel: constants.K8sAppChannelSelectorValue,
			},
			OwnerReferences: []metav1.OwnerReference{
				NewChannelOwnerRef(),
			},
		},
		Spec: corev1.ServiceSpec{
			Type:         corev1.ServiceTypeExternalName,
			ExternalName: ReceiverDeploymentName + "." + system.Namespace() + ".svc.cluster.local",
		},
	}

	// Apply The Specified Service Customizations
	for _, option := range options {
		option(service)
	}

	// Return The Test KafkaChannel Service
	return service
}

// Utility Function For Creating A Custom Receiver Service For Testing
func NewKafkaChannelReceiverService(options ...ServiceOption) *corev1.Service {

	// Create The Receiver Service
	service := &corev1.Service{
		TypeMeta: metav1.TypeMeta{
			APIVersion: corev1.SchemeGroupVersion.String(),
			Kind:       constants.ServiceKind,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      ReceiverDeploymentName,
			Namespace: system.Namespace(),
			Labels: map[string]string{
				"k8s-app":               "eventing-kafka-channels",
				"kafkachannel-receiver": "true",
			},
			OwnerReferences: []metav1.OwnerReference{
				NewSecretOwnerRef(),
			},
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{
				{
					Name:       constants.HttpPortName,
					Port:       constants.HttpServicePortNumber,
					TargetPort: intstr.FromInt(constants.HttpContainerPortNumber),
				},
				{
					Name:       MetricsPortName,
					Port:       MetricsPort,
					TargetPort: intstr.FromInt(MetricsPort),
				},
			},
			Selector: map[string]string{
				"app": ReceiverDeploymentName,
			},
		},
	}

	// Apply The Specified Service Customizations
	for _, option := range options {
		option(service)
	}

	// Return The Test Receiver Service
	return service
}

// Utility Function For Creating A Receiver Deployment For The Test Channel
func NewKafkaChannelReceiverDeployment(options ...DeploymentOption) *appsv1.Deployment {

	systemNamespace := system.Namespace()
	// Replicas Int Reference
	replicas := int32(ReceiverReplicas)

	// Create The Receiver Deployment
	deployment := &appsv1.Deployment{
		TypeMeta: metav1.TypeMeta{
			APIVersion: appsv1.SchemeGroupVersion.String(),
			Kind:       constants.DeploymentKind,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      ReceiverDeploymentName,
			Namespace: systemNamespace,
			Labels: map[string]string{
				"app":                   ReceiverDeploymentName,
				"kafkachannel-receiver": "true",
			},
			OwnerReferences: []metav1.OwnerReference{
				NewSecretOwnerRef(),
			},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app": ReceiverDeploymentName,
				},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app": ReceiverDeploymentName,
					},
				},
				Spec: corev1.PodSpec{
					ServiceAccountName: ServiceAccount,
					Containers: []corev1.Container{
						{
							Name: ReceiverDeploymentName,
							LivenessProbe: &corev1.Probe{
								Handler: corev1.Handler{
									HTTPGet: &corev1.HTTPGetAction{
										Port: intstr.FromInt(constants.HealthPort),
										Path: health.LivenessPath,
									},
								},
								InitialDelaySeconds: constants.ChannelLivenessDelay,
								PeriodSeconds:       constants.ChannelLivenessPeriod,
							},
							ReadinessProbe: &corev1.Probe{
								Handler: corev1.Handler{
									HTTPGet: &corev1.HTTPGetAction{
										Port: intstr.FromInt(constants.HealthPort),
										Path: health.ReadinessPath,
									},
								},
								InitialDelaySeconds: constants.ChannelReadinessDelay,
								PeriodSeconds:       constants.ChannelReadinessPeriod,
							},
							Image: ReceiverImage,
							Ports: []corev1.ContainerPort{
								{
									Name:          "server",
									ContainerPort: int32(8080),
								},
							},
							Env: []corev1.EnvVar{
								{
									Name:  system.NamespaceEnvKey,
									Value: systemNamespace,
								},
								{
									Name: commonenv.PodNameEnvVarKey,
									ValueFrom: &corev1.EnvVarSource{
										FieldRef: &corev1.ObjectFieldSelector{
											FieldPath: "metadata.name",
										},
									},
								},
								{
									Name:  commonenv.ContainerNameEnvVarKey,
									Value: constants.ReceiverContainerName,
								},
								{
									Name:  commonenv.KnativeLoggingConfigMapNameEnvVarKey,
									Value: logging.ConfigMapName(),
								},
								{
									Name:  commonenv.ServiceNameEnvVarKey,
									Value: ReceiverServiceName,
								},
								{
									Name:  commonenv.MetricsPortEnvVarKey,
									Value: strconv.Itoa(MetricsPort),
								},
								{
									Name:  commonenv.MetricsDomainEnvVarKey,
									Value: MetricsDomain,
								},
								{
									Name:  commonenv.HealthPortEnvVarKey,
									Value: strconv.Itoa(HealthPort),
								},
								{
									Name:  commonenv.ResyncPeriodMinutesEnvVarKey,
									Value: strconv.Itoa(int(ResyncPeriod / time.Minute)),
								},
								{
									Name:  commonenv.KafkaSecretNameEnvVarKey,
									Value: KafkaSecretName,
								},
								{
									Name:  commonenv.KafkaSecretNamespaceEnvVarKey,
									Value: KafkaSecretNamespace,
								},
							},
							ImagePullPolicy: corev1.PullIfNotPresent,
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse(ReceiverCpuRequest),
									corev1.ResourceMemory: resource.MustParse(ReceiverMemoryRequest),
								},
								Limits: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse(ReceiverCpuLimit),
									corev1.ResourceMemory: resource.MustParse(ReceiverMemoryLimit),
								},
							},
						},
					},
				},
			},
		},
	}

	// Apply The Specified Deployment Customizations
	for _, option := range options {
		option(deployment)
	}

	// Return The Test Receiver Deployment
	return deployment
}

// Utility Function For Creating A Custom KafkaChannel Dispatcher Service For Testing
func NewKafkaChannelDispatcherService(options ...ServiceOption) *corev1.Service {

	// Get The Expected Service Name For The Test KafkaChannel
	serviceName := util.DispatcherDnsSafeName(&kafkav1beta1.KafkaChannel{
		ObjectMeta: metav1.ObjectMeta{Namespace: KafkaChannelNamespace, Name: KafkaChannelName},
	})

	// Create The Dispatcher Service
	service := &corev1.Service{
		TypeMeta: metav1.TypeMeta{
			APIVersion: corev1.SchemeGroupVersion.String(),
			Kind:       constants.ServiceKind,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      serviceName,
			Namespace: system.Namespace(),
			Labels: map[string]string{
				constants.KafkaChannelNameLabel:       KafkaChannelName,
				constants.KafkaChannelNamespaceLabel:  KafkaChannelNamespace,
				constants.KafkaChannelDispatcherLabel: "true",
				constants.K8sAppChannelSelectorLabel:  constants.K8sAppDispatcherSelectorValue,
			},
			Finalizers: []string{constants.EventingKafkaFinalizerPrefix + constants.KafkaChannelFinalizerSuffix},
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{
				{
					Name:       MetricsPortName,
					Port:       int32(MetricsPort),
					TargetPort: intstr.FromInt(MetricsPort),
				},
			},
			Selector: map[string]string{
				"app": serviceName,
			},
		},
	}

	// Apply The Specified Service Customizations
	for _, option := range options {
		option(service)
	}

	// Return The Test Dispatcher Service
	return service
}

// Utility Function For Creating A Custom KafkaChannel Dispatcher Deployment For Testing
func NewKafkaChannelDispatcherDeployment(options ...DeploymentOption) *appsv1.Deployment {

	// Get The Expected Dispatcher & Topic Names For The Test KafkaChannel
	sparseKafkaChannel := &kafkav1beta1.KafkaChannel{ObjectMeta: metav1.ObjectMeta{Namespace: KafkaChannelNamespace, Name: KafkaChannelName}}
	dispatcherName := util.DispatcherDnsSafeName(sparseKafkaChannel)
	topicName := util.TopicName(sparseKafkaChannel)
	systemNamespace := system.Namespace()

	// Replicas Int Reference
	replicas := int32(DispatcherReplicas)

	deployment := &appsv1.Deployment{
		TypeMeta: metav1.TypeMeta{
			APIVersion: appsv1.SchemeGroupVersion.String(),
			Kind:       constants.DeploymentKind,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      dispatcherName,
			Namespace: systemNamespace,
			Labels: map[string]string{
				constants.AppLabel:                    dispatcherName,
				constants.KafkaChannelNameLabel:       KafkaChannelName,
				constants.KafkaChannelNamespaceLabel:  KafkaChannelNamespace,
				constants.KafkaChannelDispatcherLabel: "true",
			},
			Finalizers: []string{constants.EventingKafkaFinalizerPrefix + constants.KafkaChannelFinalizerSuffix},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app": dispatcherName,
				},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app": dispatcherName,
					},
				},
				Spec: corev1.PodSpec{
					ServiceAccountName: ServiceAccount,
					Containers: []corev1.Container{
						{
							Name:  dispatcherName,
							Image: DispatcherImage,
							LivenessProbe: &corev1.Probe{
								Handler: corev1.Handler{
									HTTPGet: &corev1.HTTPGetAction{
										Port: intstr.FromInt(constants.HealthPort),
										Path: health.LivenessPath,
									},
								},
								InitialDelaySeconds: constants.DispatcherLivenessDelay,
								PeriodSeconds:       constants.DispatcherLivenessPeriod,
							},
							ReadinessProbe: &corev1.Probe{
								Handler: corev1.Handler{
									HTTPGet: &corev1.HTTPGetAction{
										Port: intstr.FromInt(constants.HealthPort),
										Path: health.ReadinessPath,
									},
								},
								InitialDelaySeconds: constants.DispatcherReadinessDelay,
								PeriodSeconds:       constants.DispatcherReadinessPeriod,
							},
							Env: []corev1.EnvVar{
								{
									Name:  system.NamespaceEnvKey,
									Value: systemNamespace,
								},
								{
									Name: commonenv.PodNameEnvVarKey,
									ValueFrom: &corev1.EnvVarSource{
										FieldRef: &corev1.ObjectFieldSelector{
											FieldPath: "metadata.name",
										},
									},
								},
								{
									Name:  commonenv.ContainerNameEnvVarKey,
									Value: constants.DispatcherContainerName,
								},
								{
									Name:  commonenv.KnativeLoggingConfigMapNameEnvVarKey,
									Value: logging.ConfigMapName(),
								},
								{
									Name:  commonenv.MetricsPortEnvVarKey,
									Value: strconv.Itoa(MetricsPort),
								},
								{
									Name:  commonenv.MetricsDomainEnvVarKey,
									Value: MetricsDomain,
								},
								{
									Name:  commonenv.HealthPortEnvVarKey,
									Value: strconv.Itoa(HealthPort),
								},
								{
									Name:  commonenv.ChannelKeyEnvVarKey,
									Value: fmt.Sprintf("%s/%s", KafkaChannelNamespace, KafkaChannelName),
								},
								{
									Name:  commonenv.ServiceNameEnvVarKey,
									Value: dispatcherName,
								},
								{
									Name:  commonenv.KafkaTopicEnvVarKey,
									Value: topicName,
								},
								{
									Name:  commonenv.ResyncPeriodMinutesEnvVarKey,
									Value: strconv.Itoa(int(ResyncPeriod / time.Minute)),
								},
								{
									Name:  commonenv.KafkaSecretNameEnvVarKey,
									Value: KafkaSecretName,
								},
								{
									Name:  commonenv.KafkaSecretNamespaceEnvVarKey,
									Value: KafkaSecretNamespace,
								},
							},
							ImagePullPolicy: corev1.PullIfNotPresent,
							Resources: corev1.ResourceRequirements{
								Limits: corev1.ResourceList{
									corev1.ResourceMemory: resource.MustParse(DispatcherMemoryLimit),
									corev1.ResourceCPU:    resource.MustParse(DispatcherCpuLimit),
								},
								Requests: corev1.ResourceList{
									corev1.ResourceMemory: resource.MustParse(DispatcherMemoryRequest),
									corev1.ResourceCPU:    resource.MustParse(DispatcherCpuRequest),
								},
							},
						},
					},
				},
			},
		},
	}

	// Apply The Specified Deployment Customizations
	for _, option := range options {
		option(deployment)
	}

	// Return The Test Dispatcher Deployment
	return deployment
}

// Utility Function For Creating A New OwnerReference Model For The Test Kafka Secret
func NewSecretOwnerRef() metav1.OwnerReference {
	blockOwnerDeletion := true
	controller := true
	return metav1.OwnerReference{
		APIVersion:         corev1.SchemeGroupVersion.String(),
		Kind:               constants.SecretKind,
		Name:               KafkaSecretName,
		UID:                "",
		BlockOwnerDeletion: &blockOwnerDeletion,
		Controller:         &controller,
	}
}

// Utility Function For Creating A New OwnerReference Model For The Test Channel
func NewChannelOwnerRef() metav1.OwnerReference {
	blockOwnerDeletion := true
	controller := true
	return metav1.OwnerReference{
		APIVersion:         kafkav1beta1.SchemeGroupVersion.String(),
		Kind:               constants.KafkaChannelKind,
		Name:               KafkaChannelName,
		UID:                "",
		BlockOwnerDeletion: &blockOwnerDeletion,
		Controller:         &controller,
	}
}

// Utility Function For Creating A UpdateActionImpl For The KafkaChannel Labels Update Command
func NewKafkaChannelLabelUpdate(kafkachannel *kafkav1beta1.KafkaChannel) clientgotesting.UpdateActionImpl {
	return clientgotesting.UpdateActionImpl{
		ActionImpl: clientgotesting.ActionImpl{
			Namespace:   "KafkaChannelNamespace",
			Verb:        "update",
			Resource:    schema.GroupVersionResource{Group: kafkav1beta1.SchemeGroupVersion.Group, Version: kafkav1beta1.SchemeGroupVersion.Version, Resource: "kafkachannels"},
			Subresource: "",
		},
		Object: kafkachannel,
	}
}

// Utility Function For Creating A PatchActionImpl For The Finalizer Patch Command
func NewFinalizerPatchActionImpl() clientgotesting.PatchActionImpl {
	return clientgotesting.PatchActionImpl{
		ActionImpl: clientgotesting.ActionImpl{
			Namespace:   KafkaChannelNamespace,
			Verb:        "patch",
			Resource:    schema.GroupVersionResource{Group: kafkav1beta1.SchemeGroupVersion.Group, Version: kafkav1beta1.SchemeGroupVersion.Version, Resource: "kafkachannels"},
			Subresource: "",
		},
		Name:      KafkaChannelName,
		PatchType: "application/merge-patch+json",
		Patch:     []byte(`{"metadata":{"finalizers":["kafkachannels.messaging.knative.dev"],"resourceVersion":""}}`),
		// Above finalizer name matches package private "defaultFinalizerName" constant in injection/reconciler/messaging/v1beta1/kafkachannel ;)
	}
}

// Utility Function For Creating A Dispatcher Deployment Updated Event
func NewKafkaChannelDispatcherUpdatedEvent() string {
	return reconcilertesting.Eventf(corev1.EventTypeNormal, event.DispatcherDeploymentUpdated.String(), "Dispatcher Deployment Updated")
}

// Utility Function For Creating A Dispatcher Deployment Update Failure Event
func NewKafkaChannelDispatcherUpdateFailedEvent() string {
	return reconcilertesting.Eventf(corev1.EventTypeWarning, event.DispatcherDeploymentUpdateFailed.String(), "Dispatcher Deployment Update Failed")
}

// Utility Function For Creating A Successful KafkaChannel Reconciled Event
func NewKafkaChannelSuccessfulReconciliationEvent() string {
	return reconcilertesting.Eventf(corev1.EventTypeNormal, event.KafkaChannelReconciled.String(), `KafkaChannel Reconciled Successfully: "%s/%s"`, KafkaChannelNamespace, KafkaChannelName)
}

// Utility Function For Creating A Failed KafkaChannel Reconciled Event
func NewKafkaChannelFailedReconciliationEvent() string {
	return reconcilertesting.Eventf(corev1.EventTypeWarning, "InternalError", constants.ReconciliationFailedError)
}

// Utility Function For Creating A Failed KafkaChannel Reconciled Event
func NewKafkaChannelFailedFinalizationEvent() string {
	return reconcilertesting.Eventf(corev1.EventTypeWarning, "InternalError", constants.FinalizationFailedError)
}

// Utility Function For Creating A Successful KafkaChannel Finalizer Update Event
func NewKafkaChannelFinalizerUpdateEvent() string {
	return reconcilertesting.Eventf(corev1.EventTypeNormal, "FinalizerUpdate", `Updated "%s" finalizers`, KafkaChannelName)
}

// Utility Function For Creating A Successful KafkaChannel Finalizer Update Event
func NewKafkaChannelSuccessfulFinalizedEvent() string {
	return reconcilertesting.Eventf(corev1.EventTypeNormal, event.KafkaChannelFinalized.String(), fmt.Sprintf("KafkaChannel Finalized Successfully: \"%s/%s\"", KafkaChannelNamespace, KafkaChannelName))
}

// Utility Function For Creating A UpdateActionImpl For A Service Update Command
func NewServiceUpdateActionImpl(service *corev1.Service) clientgotesting.UpdateActionImpl {
	return clientgotesting.UpdateActionImpl{
		ActionImpl: clientgotesting.ActionImpl{
			Namespace:   service.Namespace,
			Verb:        "update",
			Resource:    schema.GroupVersionResource{Group: corev1.SchemeGroupVersion.Group, Version: corev1.SchemeGroupVersion.Version, Resource: "services"},
			Subresource: "",
		},
		Object: service,
	}
}

// Utility Function For Creating A UpdateActionImpl For A Deployment Update Command
func NewDeploymentUpdateActionImpl(deployment *appsv1.Deployment) clientgotesting.UpdateActionImpl {
	return clientgotesting.UpdateActionImpl{
		ActionImpl: clientgotesting.ActionImpl{
			Namespace:   deployment.Namespace,
			Verb:        "update",
			Resource:    schema.GroupVersionResource{Group: appsv1.SchemeGroupVersion.Group, Version: appsv1.SchemeGroupVersion.Version, Resource: "deployments"},
			Subresource: "",
		},
		Object: deployment,
	}
}

// Utility Function For Creating A DeleteActionImpl For A Service Delete Command
func NewServiceDeleteActionImpl(service *corev1.Service) clientgotesting.DeleteActionImpl {
	return clientgotesting.DeleteActionImpl{
		ActionImpl: clientgotesting.ActionImpl{
			Namespace:   service.Namespace,
			Verb:        "delete",
			Resource:    schema.GroupVersionResource{Group: corev1.SchemeGroupVersion.Group, Version: corev1.SchemeGroupVersion.Version, Resource: "services"},
			Subresource: "",
		},
		Name: service.Name,
	}
}

// Utility Function For Creating A DeleteActionImpl For A Deployment Delete Command
func NewDeploymentDeleteActionImpl(deployment *appsv1.Deployment) clientgotesting.DeleteActionImpl {
	return clientgotesting.DeleteActionImpl{
		ActionImpl: clientgotesting.ActionImpl{
			Namespace:   deployment.Namespace,
			Verb:        "delete",
			Resource:    schema.GroupVersionResource{Group: appsv1.SchemeGroupVersion.Group, Version: appsv1.SchemeGroupVersion.Version, Resource: "deployments"},
			Subresource: "",
		},
		Name: deployment.Name,
	}
}

func NewKafkaChannelStatusUpdates() []clientgotesting.UpdateActionImpl {
	return []clientgotesting.UpdateActionImpl{
		{
			Object: NewKafkaChannel(
				WithFinalizer,
				WithAddress,
				WithInitializedConditions,
				WithKafkaChannelServiceReady,
				WithDispatcherDeploymentReady,
				WithTopicReady,
			),
		},
	}
}

func NewKafkaChannelUpdate() clientgotesting.UpdateActionImpl {
	return clientgotesting.UpdateActionImpl{
		Object: NewKafkaChannel(
			WithFinalizer,
			WithMetaData,
			WithAddress,
			WithInitializedConditions,
			WithKafkaChannelServiceReady,
			WithDispatcherDeploymentReady,
			WithTopicReady,
		),
	}
}
