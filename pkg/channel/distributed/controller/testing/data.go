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

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/intstr"
	clientgotesting "k8s.io/client-go/testing"
	"k8s.io/utils/pointer"
	"knative.dev/eventing-kafka/pkg/common/client"
	"knative.dev/eventing/pkg/apis/messaging"
	"knative.dev/pkg/apis"
	"knative.dev/pkg/logging"
	reconcilertesting "knative.dev/pkg/reconciler/testing"
	"knative.dev/pkg/system"

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
	commonconstants "knative.dev/eventing-kafka/pkg/common/constants"
	commontesting "knative.dev/eventing-kafka/pkg/common/testing"
)

// Constants
const (
	// MetricsPortName - Prometheus MetricsPort
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
	ReceiverDeploymentName = KafkaSecretName + "-b9176d5f-receiver" // Truncated MD5 Hash Of KafkaSecretName
	ReceiverServiceName    = ReceiverDeploymentName
	TopicName              = KafkaChannelNamespace + "." + KafkaChannelName

	KafkaSecretDataValueUsername = "TestKafkaSecretDataUsername"
	KafkaSecretDataValuePassword = "TestKafkaSecretDataPassword"
	KafkaSecretDataValueSaslType = "PLAIN"

	// NumPartitions - ChannelSpec Test Data
	NumPartitions = 123
	// ReplicationFactor - ChannelSpec Test Data
	ReplicationFactor = 456
	// RetentionMillis - ChannelSpec Test Data
	RetentionMillis = 123456

	// ErrorString - Mock Test Error MetaData
	ErrorString = "Expected Mock Test Error"
	// SuccessString - Mock Test Success MetaData
	SuccessString = "Expected Mock Test Success"

	// DispatcherMemoryRequest - Test Dispatcher Memory Request Resource
	DispatcherMemoryRequest = "20Mi"
	// DispatcherCpuRequest - Test Dispatcher CPU Request Resource
	DispatcherCpuRequest = "100m"
	// DispatcherMemoryLimit - Test Dispatcher Memory Limit Resource
	DispatcherMemoryLimit = "50Mi"
	// DispatcherCpuLimit - Test Dispatcher CPU Limit Resource
	DispatcherCpuLimit = "300m"

	// ReceiverMemoryRequest - Test Receiver Memory Request Resource
	ReceiverMemoryRequest = "10Mi"
	// ReceiverMemoryLimit - Test Receiver Memory Limit Resource
	ReceiverMemoryLimit = "20Mi"
	// ReceiverCpuRequest - Test Receiver CPU Request Resource
	ReceiverCpuRequest = "10m"
	// ReceiverCpuLimit - Test Receiver CPU Limit Resource
	ReceiverCpuLimit = "100m"

	ConfigMapHash = "deadbeef"

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
kafka:
  authSecretName: ` + KafkaSecretName + `
  authSecretNamespace: ` + KafkaSecretNamespace + `
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
	RetentionMillisString 		 = strconv.FormatInt(RetentionMillis, 10)
)

//
// Utility Data Creation Functions
//

// ServiceOption Allows Customization Of A Service
type ServiceOption func(service *corev1.Service)

// DeploymentOption Allows Customization Of A Deployment
type DeploymentOption func(deployment *appsv1.Deployment)

// WithDeletionTimestampService Sets The Service's DeletionTimestamp To Current Time
func WithDeletionTimestampService(service *corev1.Service) {
	service.ObjectMeta.SetDeletionTimestamp(&DeletionTimestamp)
}

// WithDeletionTimestampDeployment Sets The Deployment's DeletionTimestamp To Current Time
func WithDeletionTimestampDeployment(deployment *appsv1.Deployment) {
	deployment.ObjectMeta.SetDeletionTimestamp(&DeletionTimestamp)
}

// WithConfigMapHash Adds A Particular Hash Annotation To A Deployment
func WithConfigMapHash(configMapHash string) func(deployment *appsv1.Deployment) {
	return func(deployment *appsv1.Deployment) {
		deployment.Spec.Template.ObjectMeta.Annotations[commonconstants.ConfigMapHashAnnotationKey] = configMapHash
	}
}

// WithoutFinalizersService Clears The Specified Service's Finalizers
func WithoutFinalizersService(service *corev1.Service) {
	service.ObjectMeta.Finalizers = []string{}
}

// WithoutFinalizersDeployment Clears The Dispatcher Deployment's Finalizers
func WithoutFinalizersDeployment(deployment *appsv1.Deployment) {
	deployment.ObjectMeta.Finalizers = []string{}
}

// WithoutServicePorts Clears The Service Ports
func WithoutServicePorts(service *corev1.Service) {
	service.Spec.Ports = []corev1.ServicePort{}
}

// WithoutServiceSelector Clears The Selector
func WithoutServiceSelector(service *corev1.Service) {
	service.Spec.Selector = map[string]string{}
}

// WithoutServiceLabels Clears The Labels
func WithoutServiceLabels(service *corev1.Service) {
	service.Labels = map[string]string{}
}

// WithExtraServiceLabels Adds An Arbitrary Additional Label
func WithExtraServiceLabels(service *corev1.Service) {
	service.Labels["ExtraLabelName"] = "ExtraLabelValue"
}

// WithDifferentServiceStatus Changes The LoadBalancerIngress Hostname
func WithDifferentServiceStatus(service *corev1.Service) {
	service.Status.LoadBalancer = corev1.LoadBalancerStatus{
		Ingress: []corev1.LoadBalancerIngress{
			{
				IP:       "1.2.3.4",
				Hostname: "DifferentHostname",
			},
		},
	}
}

// WithoutResources Removes The Resource Limits And Requests
func WithoutResources(deployment *appsv1.Deployment) {
	deployment.Spec.Template.Spec.Containers[0].Resources.Limits = nil
	deployment.Spec.Template.Spec.Containers[0].Resources.Requests = nil
}

// WithDifferentName Changes The Container Name
func WithDifferentName(deployment *appsv1.Deployment) {
	deployment.Spec.Template.Spec.Containers[0].Name = "DifferentName"
}

// WithDifferentImage Changes The Container Image
func WithDifferentImage(deployment *appsv1.Deployment) {
	deployment.Spec.Template.Spec.Containers[0].Image = "DifferentImage"
}

// WithDifferentCommand Changes The Container Command
func WithDifferentCommand(deployment *appsv1.Deployment) {
	deployment.Spec.Template.Spec.Containers[0].Command = []string{"DifferentCommand"}
}

// WithDifferentArgs Changes The Container Args
func WithDifferentArgs(deployment *appsv1.Deployment) {
	deployment.Spec.Template.Spec.Containers[0].Args = []string{"DifferentArgs"}
}

// WithDifferentWorkingDir Changes The Container WorkingDir
func WithDifferentWorkingDir(deployment *appsv1.Deployment) {
	deployment.Spec.Template.Spec.Containers[0].WorkingDir = "DifferentWorkingDir"
}

// WithDifferentPorts Changes The Container Ports
func WithDifferentPorts(deployment *appsv1.Deployment) {
	deployment.Spec.Template.Spec.Containers[0].Ports = []corev1.ContainerPort{
		{Name: "DifferentPortName"},
	}
}

// WithMissingEnvironment Clears The Environment
func WithMissingEnvironment(deployment *appsv1.Deployment) {
	deployment.Spec.Template.Spec.Containers[0].Env = []corev1.EnvVar{}
}

// WithDifferentEnvironment Adds An Arbitrary Extra Environment Variable
func WithDifferentEnvironment(deployment *appsv1.Deployment) {
	deployment.Spec.Template.Spec.Containers[0].Env = append(
		deployment.Spec.Template.Spec.Containers[0].Env,
		corev1.EnvVar{Name: "NewEnvName", Value: "NewEnvValue"},
	)
}

// WithDifferentVolumeMounts Changes The Container Volume Mounts
func WithDifferentVolumeMounts(deployment *appsv1.Deployment) {
	deployment.Spec.Template.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{
		{Name: "DifferentVolumeMount"},
	}
}

// WithDifferentVolumeDevices Changes The WithDifferentVolumeDevices
func WithDifferentVolumeDevices(deployment *appsv1.Deployment) {
	deployment.Spec.Template.Spec.Containers[0].VolumeDevices = []corev1.VolumeDevice{
		{Name: "DifferentVolumeDevice"},
	}
}

// WithDifferentVolumes Changes The Volumes In The Spec
func WithDifferentVolumes(deployment *appsv1.Deployment) {
	deployment.Spec.Template.Spec.Volumes = []corev1.Volume{
		{Name: "DifferentVolume"},
	}
}

// WithDifferentLivenessProbe Changes The LivenessProbe In The Container
func WithDifferentLivenessProbe(deployment *appsv1.Deployment) {
	deployment.Spec.Template.Spec.Containers[0].LivenessProbe = &corev1.Probe{Handler: corev1.Handler{}}
}

// WithDifferentReadinessProbe Changes The WReadinessProbe In The Container
func WithDifferentReadinessProbe(deployment *appsv1.Deployment) {
	deployment.Spec.Template.Spec.Containers[0].ReadinessProbe = &corev1.Probe{Handler: corev1.Handler{}}
}

// WithDifferentLifecycle Changes The Lifecycle In The Container
func WithDifferentLifecycle(deployment *appsv1.Deployment) {
	deployment.Spec.Template.Spec.Containers[0].Lifecycle = &corev1.Lifecycle{PostStart: &corev1.Handler{}}
}

// WithDifferentTerminationPath Changes The TerminationPath In The Container
func WithDifferentTerminationPath(deployment *appsv1.Deployment) {
	deployment.Spec.Template.Spec.Containers[0].TerminationMessagePath = "DifferentTerminationPath"
}

// WithDifferentTerminationPolicy Changes The TerminationPolicy In The Container
func WithDifferentTerminationPolicy(deployment *appsv1.Deployment) {
	deployment.Spec.Template.Spec.Containers[0].TerminationMessagePolicy = corev1.TerminationMessageFallbackToLogsOnError
}

// WithDifferentImagePullPolicy Changes The ImagePullPolicy In The Container
func WithDifferentImagePullPolicy(deployment *appsv1.Deployment) {
	deployment.Spec.Template.Spec.Containers[0].ImagePullPolicy = corev1.PullNever
}

// WithDifferentSecurityContext Changes The SecurityContext In The Container
func WithDifferentSecurityContext(deployment *appsv1.Deployment) {
	deployment.Spec.Template.Spec.Containers[0].SecurityContext = &corev1.SecurityContext{}
}

// WithDifferentReplicas Changes The Replicas In The Spec
func WithDifferentReplicas(deployment *appsv1.Deployment) {
	deployment.Spec.Replicas = pointer.Int32Ptr(10)
}

// WithoutLabels Clears The Labels
func WithoutLabels(deployment *appsv1.Deployment) {
	deployment.ObjectMeta.Labels = map[string]string{}
}

// WithExtraLabels Adds An Arbitrary Additional Label
func WithExtraLabels(deployment *appsv1.Deployment) {
	deployment.ObjectMeta.Labels["ExtraLabelName"] = "ExtraLabelValue"
}

// WithoutAnnotations Clears The Annotations
func WithoutAnnotations(deployment *appsv1.Deployment) {
	deployment.Spec.Template.ObjectMeta.Annotations = map[string]string{}
}

// WithExtraAnnotations Adds An Arbitrary Additional Annotation
func WithExtraAnnotations(deployment *appsv1.Deployment) {
	deployment.Spec.Template.ObjectMeta.Annotations["ExtraAnnotationName"] = "ExtraAnnotationValue"
}

//
// ControllerConfig Test Data
//

// NewEnvironment Sets The Required Environment Variables
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

// NewConfig Sets The Required Config Fields
func NewConfig(options ...KafkaConfigOption) *commonconfig.EventingKafkaConfig {
	kafkaConfig := &commonconfig.EventingKafkaConfig{
		Auth: &client.KafkaAuthConfig{
			SASL: &client.KafkaSaslConfig{
				User:     KafkaSecretDataValueUsername,
				Password: KafkaSecretDataValuePassword,
				SaslType: KafkaSecretDataValueSaslType,
			},
		},
		Kafka: commonconfig.EKKafkaConfig{
			AuthSecretName:      KafkaSecretName,
			AuthSecretNamespace: KafkaSecretNamespace,
			Topic: commonconfig.EKKafkaTopicConfig{
				DefaultNumPartitions:     DefaultNumPartitions,
				DefaultReplicationFactor: DefaultReplicationFactor,
				DefaultRetentionMillis:   DefaultRetentionMillis,
			},
		},
		Channel: commonconfig.EKChannelConfig{
			AdminType: KafkaAdminType,
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
		},
	}

	// Apply The Specified Kafka Config Customizations
	for _, option := range options {
		option(kafkaConfig)
	}

	return kafkaConfig
}

// WithNoReceiverResources Removes The Receiver Resource Requests And Limits
func WithNoReceiverResources(kafkaConfig *commonconfig.EventingKafkaConfig) {
	kafkaConfig.Channel.Receiver.EKKubernetesConfig.CpuLimit = resource.Quantity{}
	kafkaConfig.Channel.Receiver.EKKubernetesConfig.CpuRequest = resource.Quantity{}
	kafkaConfig.Channel.Receiver.EKKubernetesConfig.MemoryLimit = resource.Quantity{}
	kafkaConfig.Channel.Receiver.EKKubernetesConfig.MemoryRequest = resource.Quantity{}
}

// WithNoDispatcherResources Removes The Dispatcher Resource Requests And Limits
func WithNoDispatcherResources(kafkaConfig *commonconfig.EventingKafkaConfig) {
	kafkaConfig.Channel.Dispatcher.EKKubernetesConfig.CpuLimit = resource.Quantity{}
	kafkaConfig.Channel.Dispatcher.EKKubernetesConfig.CpuRequest = resource.Quantity{}
	kafkaConfig.Channel.Dispatcher.EKKubernetesConfig.MemoryLimit = resource.Quantity{}
	kafkaConfig.Channel.Dispatcher.EKKubernetesConfig.MemoryRequest = resource.Quantity{}
}

//
// Kafka Secret Resources
//

// KafkaSecretOption Enables Customization Of A Kafka Secret
type KafkaSecretOption func(secret *corev1.Secret)

// NewKafkaSecret Creates A New Kafka Auth Secret For Testing
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
		},
		Data: map[string][]byte{
			commonconstants.KafkaSecretKeyUsername: []byte(KafkaSecretDataValueUsername),
			commonconstants.KafkaSecretKeyPassword: []byte(KafkaSecretDataValuePassword),
			commonconstants.KafkaSecretKeySaslType: []byte(KafkaSecretDataValueSaslType),
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

// WithKafkaSecretFinalizer Sets The Kafka Secret's Finalizer
func WithKafkaSecretFinalizer(secret *corev1.Secret) {
	secret.ObjectMeta.Finalizers = []string{constants.EventingKafkaFinalizerPrefix + "kafkasecrets.eventing-kafka.knative.dev"}
}

//
// KafkaChannel Resources
//

// KafkaChannelOption Enables Customization Of A KafkaChannel
type KafkaChannelOption func(*kafkav1beta1.KafkaChannel)

// NewKafkaChannel Creates A Custom KafkaChannel For Testing
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
			RetentionMillis:   RetentionMillis,
		},
	}

	// Apply The Specified KafkaChannel Customizations
	for _, option := range options {
		option(kafkachannel)
	}

	// Return The Test KafkaChannel
	return kafkachannel
}

// NewSecretAndKafkaChannel is a convenience for adding a default kafka secret to a single kafka channel object slice
func NewSecretAndKafkaChannel(options ...KafkaChannelOption) []runtime.Object {
	return []runtime.Object{NewKafkaSecret(), NewKafkaChannel(options...)}
}

// WithInitializedConditions Sets The KafkaChannel's Status To Initialized State
func WithInitializedConditions(kafkachannel *kafkav1beta1.KafkaChannel) {
	kafkachannel.Status.InitializeConditions()
	kafkachannel.Status.MarkConfigTrue()
}

// WithEmptySpec Removes The KafkaChannel's Spec
func WithEmptySpec(kafkachannel *kafkav1beta1.KafkaChannel) {
	kafkachannel.Spec = kafkav1beta1.KafkaChannelSpec{}
}

// WithDeletionTimestamp Sets The KafkaChannel's DeletionTimestamp To Current Time
func WithDeletionTimestamp(kafkachannel *kafkav1beta1.KafkaChannel) {
	kafkachannel.ObjectMeta.SetDeletionTimestamp(&DeletionTimestamp)
}

// WithFinalizer Sets The KafkaChannel's Finalizer
func WithFinalizer(kafkachannel *kafkav1beta1.KafkaChannel) {
	kafkachannel.ObjectMeta.Finalizers = []string{constants.KafkaChannelFinalizerSuffix}
}

// WithMetaData Sets The KafkaChannel's MetaData
func WithMetaData(kafkachannel *kafkav1beta1.KafkaChannel) {
	WithAnnotations(kafkachannel)
	WithLabels(kafkachannel)
}

// WithAnnotations Sets The KafkaChannel's Annotations
func WithAnnotations(kafkachannel *kafkav1beta1.KafkaChannel) {
	kafkachannel.ObjectMeta.Annotations = map[string]string{
		messaging.SubscribableDuckVersionAnnotation: constants.SubscribableDuckVersionAnnotationV1,
	}
}

// WithLabels Sets The KafkaChannel's Labels
func WithLabels(kafkachannel *kafkav1beta1.KafkaChannel) {
	kafkachannel.ObjectMeta.Labels = map[string]string{
		constants.KafkaTopicLabel: fmt.Sprintf("%s.%s", KafkaChannelNamespace, KafkaChannelName),
	}
}

// WithAddress Sets The KafkaChannel's Address
func WithAddress(kafkachannel *kafkav1beta1.KafkaChannel) {
	kafkachannel.Status.SetAddress(&apis.URL{
		Scheme: "http",
		Host:   fmt.Sprintf("%s-%s.%s.svc.cluster.local", KafkaChannelName, kafkaconstants.KafkaChannelServiceNameSuffix, KafkaChannelNamespace),
	})
}

// WithKafkaChannelConfigurationFailedNoSecret Sets The KafkaChannel's Configuration As Failed - No Secret
func WithKafkaChannelConfigurationFailedNoSecret(kafkachannel *kafkav1beta1.KafkaChannel) {
	kafkachannel.Status.GetConditionSet().Manage(&kafkachannel.Status).MarkFalse("ConfigurationReady", "KafkaSecretReconciled", "No Kafka Secret For KafkaChannel")
}

// WithKafkaChannelServiceReady Sets The KafkaChannel's Service As READY
func WithKafkaChannelServiceReady(kafkachannel *kafkav1beta1.KafkaChannel) {
	kafkachannel.Status.MarkChannelServiceTrue()
}

// WithKafkaChannelServiceFailed Sets The KafkaChannel's Services As Failed
func WithKafkaChannelServiceFailed(kafkachannel *kafkav1beta1.KafkaChannel) {
	kafkachannel.Status.MarkChannelServiceFailed(event.KafkaChannelServiceReconciliationFailed.String(), "Failed To Create KafkaChannel Service: inducing failure for create services")
}

// WithReceiverServiceReady Sets The KafkaChannel's Receiver Service As READY ("ChannelServiceReady")
func WithReceiverServiceReady(kafkachannel *kafkav1beta1.KafkaChannel) {
	kafkachannel.Status.MarkServiceTrue()
}

// WithReceiverServiceFailed Sets The KafkaChannel's Receiver Service As Failed
func WithReceiverServiceFailed(kafkachannel *kafkav1beta1.KafkaChannel) {
	kafkachannel.Status.MarkServiceFailed(event.ReceiverServiceReconciliationFailed.String(), "Receiver Service Failed: inducing failure for create services")
}

// WithReceiverServiceFailedNoSecret Sets The KafkaChannel's Receiver Service As Failed ("no secret found")
func WithReceiverServiceFailedNoSecret(kafkachannel *kafkav1beta1.KafkaChannel) {
	kafkachannel.Status.MarkServiceFailed(event.ReceiverServiceReconciliationFailed.String(), "Receiver Service Failed: no secret found")
}

// WithReceiverServiceFailedTimestamp Sets The KafkaChannel's Receiver Service As Failed ("encountered Receiver Service with DeletionTimestamp" ...)
func WithReceiverServiceFailedTimestamp(kafkachannel *kafkav1beta1.KafkaChannel) {
	kafkachannel.Status.MarkServiceFailed(event.ReceiverServiceReconciliationFailed.String(), "Receiver Service Failed: encountered Receiver Service with DeletionTimestamp eventing-test-ns/kafkasecret-name-b9176d5f-receiver - potential race condition")
}

// WithReceiverDeploymentFailedTimestamp Sets The KafkaChannel's Receiver Deployment As Failed ("encountered Receiver Deployment with DeletionTimestamp" ...)
func WithReceiverDeploymentFailedTimestamp(kafkachannel *kafkav1beta1.KafkaChannel) {
	kafkachannel.Status.MarkEndpointsFailed(event.ReceiverDeploymentReconciliationFailed.String(), "Receiver Deployment Failed: encountered Receiver Deployment with DeletionTimestamp eventing-test-ns/kafkasecret-name-b9176d5f-receiver - potential race condition")
}

// WithReceiverDeploymentReady Sets The KafkaChannel's Receiver Deployment As READY  ("EndpointsReady")
func WithReceiverDeploymentReady(kafkachannel *kafkav1beta1.KafkaChannel) {
	kafkachannel.Status.MarkEndpointsTrue()
}

// WithReceiverDeploymentFailed Sets The KafkaChannel's Receiver Deployment As Failed
func WithReceiverDeploymentFailed(kafkachannel *kafkav1beta1.KafkaChannel) {
	kafkachannel.Status.MarkEndpointsFailed(event.ReceiverDeploymentReconciliationFailed.String(), "Receiver Deployment Failed: inducing failure for create deployments")
}

// WithReceiverDeploymentFailedNoSecret Sets The KafkaChannel's Receiver Deployment As Failed ("no secret found")
func WithReceiverDeploymentFailedNoSecret(kafkachannel *kafkav1beta1.KafkaChannel) {
	kafkachannel.Status.MarkEndpointsFailed(event.ReceiverDeploymentReconciliationFailed.String(), "Receiver Deployment Failed: no secret found")
}

// WithDispatcherDeploymentReady Sets The KafkaChannel's Dispatcher Deployment As READY
func WithDispatcherDeploymentReady(_ *kafkav1beta1.KafkaChannel) {
	// TODO - This is unnecessary since the testing framework doesn't return any Status Conditions from the K8S commands (Create, Get)
	//        which means the propagate function doesn't do anything.  This is a testing gap with the framework and propagateDispatcherStatus()
	// kafkachannel.Status.PropagateDispatcherStatus()
}

// WithDispatcherFailed Sets The KafkaChannel's Dispatcher Deployment As Failed
func WithDispatcherFailed(kafkachannel *kafkav1beta1.KafkaChannel) {
	kafkachannel.Status.MarkDispatcherFailed(event.DispatcherDeploymentReconciliationFailed.String(), "Failed To Create Dispatcher Deployment: inducing failure for create deployments")
}

// WithDispatcherUpdateFailed Sets The KafkaChannel's Dispatcher Update As Failed
func WithDispatcherUpdateFailed(kafkachannel *kafkav1beta1.KafkaChannel) {
	kafkachannel.Status.MarkDispatcherFailed(event.DispatcherDeploymentUpdateFailed.String(), "Failed To Update Dispatcher Deployment: inducing failure for update deployments")
}

// WithDispatcherServicePatchFailed Sets The KafkaChannel's Dispatcher Patch As Failed
func WithDispatcherServicePatchFailed(kafkachannel *kafkav1beta1.KafkaChannel) {
	kafkachannel.Status.MarkDispatcherFailed(event.DispatcherServicePatchFailed.String(), "Failed To Patch Dispatcher Service: inducing failure for patch services")
}

// WithTopicReady Sets The KafkaChannel's Topic READY
func WithTopicReady(kafkachannel *kafkav1beta1.KafkaChannel) {
	kafkachannel.Status.MarkTopicTrue()
}

// NewKafkaChannelService Creates A Custom KafkaChannel "Channel" Service For Testing
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

// NewKafkaChannelReceiverService Creates A Custom Receiver Service For Testing
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

// NewKafkaChannelReceiverDeployment Creates A Receiver Deployment For The Test Channel
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
					Annotations: map[string]string{
						commonconstants.ConfigMapHashAnnotationKey: ConfigMapHash,
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
								TimeoutSeconds:      constants.ChannelLivenessTimeout,
								SuccessThreshold:    constants.ChannelLivenessSuccessThreshold,
								FailureThreshold:    constants.ChannelLivenessFailureThreshold,
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
								TimeoutSeconds:      constants.ChannelReadinessTimeout,
								SuccessThreshold:    constants.ChannelReadinessSuccessThreshold,
								FailureThreshold:    constants.ChannelReadinessFailureThreshold,
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
									Name:  commonconstants.KnativeLoggingConfigMapNameEnvVarKey,
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
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      commonconstants.SettingsConfigMapName,
									MountPath: commonconstants.SettingsConfigMapMountPath,
								},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: commonconstants.SettingsConfigMapName,
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{
										Name: commonconstants.SettingsConfigMapName,
									},
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

// NewKafkaChannelDispatcherService Creates A Custom KafkaChannel Dispatcher Service For Testing
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

// NewKafkaChannelDispatcherDeployment Creates A Custom KafkaChannel Dispatcher Deployment For Testing
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
					Annotations: map[string]string{
						commonconstants.ConfigMapHashAnnotationKey: ConfigMapHash,
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
								TimeoutSeconds:      constants.DispatcherLivenessTimeout,
								SuccessThreshold:    constants.DispatcherLivenessSuccessThreshold,
								FailureThreshold:    constants.DispatcherLivenessFailureThreshold,
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
								TimeoutSeconds:      constants.DispatcherReadinessTimeout,
								SuccessThreshold:    constants.DispatcherReadinessSuccessThreshold,
								FailureThreshold:    constants.DispatcherReadinessFailureThreshold,
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
									Name:  commonconstants.KnativeLoggingConfigMapNameEnvVarKey,
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
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      commonconstants.SettingsConfigMapName,
									MountPath: commonconstants.SettingsConfigMapMountPath,
								},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: commonconstants.SettingsConfigMapName,
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{
										Name: commonconstants.SettingsConfigMapName,
									},
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

// NewChannelOwnerRef Creates A New OwnerReference Model For The Test Channel
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

// NewKafkaChannelLabelUpdate Creates A UpdateActionImpl For The KafkaChannel Labels Update Command
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

// NewFinalizerPatchActionImpl Creates A PatchActionImpl For The Finalizer Patch Command
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

// NewKafkaChannelDispatcherDeploymentUpdatedEvent Creates A Dispatcher Deployment Updated Event
func NewKafkaChannelDispatcherDeploymentUpdatedEvent() string {
	return reconcilertesting.Eventf(corev1.EventTypeNormal, event.DispatcherDeploymentUpdated.String(), "Dispatcher Deployment Updated")
}

// NewKafkaChannelDispatcherDeploymentUpdateFailedEvent Creates A Dispatcher Deployment Update Failure Event
func NewKafkaChannelDispatcherDeploymentUpdateFailedEvent() string {
	return reconcilertesting.Eventf(corev1.EventTypeWarning, event.DispatcherDeploymentUpdateFailed.String(), "Dispatcher Deployment Update Failed")
}

// NewKafkaChannelDispatcherServicePatchedEvent Creates A Dispatcher Service Patched Event
func NewKafkaChannelDispatcherServicePatchedEvent() string {
	return reconcilertesting.Eventf(corev1.EventTypeNormal, event.DispatcherServicePatched.String(), "Dispatcher Service Patched")
}

// NewKafkaChannelDispatcherServicePatchFailedEvent Creates A Dispatcher Service Patch Failure Event
func NewKafkaChannelDispatcherServicePatchFailedEvent() string {
	return reconcilertesting.Eventf(corev1.EventTypeWarning, event.DispatcherServicePatchFailed.String(), "Dispatcher Service Patch Failed")
}

// NewKafkaChannelSuccessfulReconciliationEvent Creates A Successful KafkaChannel Reconciled Event
func NewKafkaChannelSuccessfulReconciliationEvent() string {
	return reconcilertesting.Eventf(corev1.EventTypeNormal, event.KafkaChannelReconciled.String(), `KafkaChannel Reconciled Successfully: "%s/%s"`, KafkaChannelNamespace, KafkaChannelName)
}

// NewKafkaChannelFailedReconciliationEvent Creates A Failed KafkaChannel Reconciled Event
func NewKafkaChannelFailedReconciliationEvent() string {
	return reconcilertesting.Eventf(corev1.EventTypeWarning, "InternalError", constants.ReconciliationFailedError)
}

// NewKafkaChannelFailedFinalizationEvent Creates A Failed KafkaChannel Reconciled Event
func NewKafkaChannelFailedFinalizationEvent() string {
	return reconcilertesting.Eventf(corev1.EventTypeWarning, "InternalError", constants.FinalizationFailedError)
}

// NewKafkaChannelFinalizerUpdateEvent Creates A Successful KafkaChannel Finalizer Update Event
func NewKafkaChannelFinalizerUpdateEvent() string {
	return reconcilertesting.Eventf(corev1.EventTypeNormal, "FinalizerUpdate", `Updated "%s" finalizers`, KafkaChannelName)
}

// NewKafkaChannelSuccessfulFinalizedEvent Creates A Successful KafkaChannel Finalizer Update Event
func NewKafkaChannelSuccessfulFinalizedEvent() string {
	return reconcilertesting.Eventf(corev1.EventTypeNormal, event.KafkaChannelFinalized.String(), fmt.Sprintf("KafkaChannel Finalized Successfully: \"%s/%s\"", KafkaChannelNamespace, KafkaChannelName))
}

// NewServiceUpdateActionImpl Creates A UpdateActionImpl For A Service Update Command
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

// NewDeploymentUpdateActionImpl Creates A UpdateActionImpl For A Deployment Update Command
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

// NewServiceDeleteActionImpl Creates A DeleteActionImpl For A Service Delete Command
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

// NewDeploymentDeleteActionImpl Creates A DeleteActionImpl For A Deployment Delete Command
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
