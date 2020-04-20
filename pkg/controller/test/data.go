package test

import (
	"fmt"
	"github.com/kyma-incubator/knative-kafka/pkg/common/health"
	kafkautil "github.com/kyma-incubator/knative-kafka/pkg/common/kafka/util"
	"github.com/kyma-incubator/knative-kafka/pkg/controller/constants"
	"github.com/kyma-incubator/knative-kafka/pkg/controller/env"
	"github.com/kyma-incubator/knative-kafka/pkg/controller/event"
	"github.com/kyma-incubator/knative-kafka/pkg/controller/util"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/intstr"
	clientgotesting "k8s.io/client-go/testing"
	kafkav1alpha1 "knative.dev/eventing-contrib/kafka/channel/pkg/apis/messaging/v1alpha1"
	"knative.dev/pkg/apis"
	"knative.dev/pkg/logging"
	reconcilertesting "knative.dev/pkg/reconciler/testing"
	"knative.dev/pkg/system"
	"strconv"
	"time"
)

// Constants
const (
	// Prometheus MetricsPort
	MetricsPortName = "metrics"

	// Environment Test Data
	ServiceAccount                         = "TestServiceAccount"
	MetricsPort                            = 9876
	HealthPort                             = 8082
	KafkaOffsetCommitMessageCount          = 99
	KafkaOffsetCommitDurationMillis        = 9999
	ChannelImage                           = "TestChannelImage"
	ChannelReplicas                        = 1
	DispatcherImage                        = "TestDispatcherImage"
	DispatcherReplicas                     = 1
	DefaultNumPartitions                   = 4
	DefaultReplicationFactor               = 1
	DefaultRetentionMillis                 = 99999
	DefaultEventRetryInitialIntervalMillis = 88888
	DefaultEventRetryTimeMillisMax         = 11111111
	DefaultExponentialBackoff              = true

	// Channel Test Data
	KafkaChannelNamespace = "kafkachannel-namespace"
	KafkaChannelName      = "kafkachannel-name"
	KafkaChannelKey       = KafkaChannelNamespace + "/" + KafkaChannelName
	KafkaSecretNamespace  = constants.KnativeEventingNamespace // Needs To Match Hardcoded Value In Reconciliation
	KafkaSecretName       = "kafkasecret-name"
	KafkaSecretKey        = KafkaSecretNamespace + "/" + KafkaSecretName
	ChannelDeploymentName = KafkaSecretName + "-channel"
	TopicName             = KafkaChannelNamespace + "." + KafkaChannelName

	KafkaSecretDataValueBrokers  = "TestKafkaSecretDataBrokers"
	KafkaSecretDataValueUsername = "TestKafkaSecretDataUsername"
	KafkaSecretDataValuePassword = "TestKafkaSecretDataPassword"

	// ChannelSpec Test Data
	NumPartitions     = 123
	ReplicationFactor = 456

	// Test MetaData
	ErrorString = "Expected Mock Test Error"

	// Test Dispatcher Resources
	DispatcherMemoryRequest = "20Mi"
	DispatcherCpuRequest    = "100m"
	DispatcherMemoryLimit   = "50Mi"
	DispatcherCpuLimit      = "300m"

	// Test Channel Resources
	ChannelMemoryRequest = "10Mi"
	ChannelMemoryLimit   = "20Mi"
	ChannelCpuRequest    = "10m"
	ChannelCpuLimit      = "100m"
)

//
// ControllerConfig Test Data
//

// Set The Required Environment Variables
func NewEnvironment() *env.Environment {
	return &env.Environment{
		ServiceAccount:                       ServiceAccount,
		MetricsPort:                          MetricsPort,
		KafkaOffsetCommitMessageCount:        KafkaOffsetCommitMessageCount,
		KafkaOffsetCommitDurationMillis:      KafkaOffsetCommitDurationMillis,
		DefaultNumPartitions:                 DefaultNumPartitions,
		DefaultReplicationFactor:             DefaultReplicationFactor,
		DefaultRetentionMillis:               DefaultRetentionMillis,
		DispatcherImage:                      DispatcherImage,
		DispatcherReplicas:                   DispatcherReplicas,
		DispatcherRetryInitialIntervalMillis: DefaultEventRetryInitialIntervalMillis,
		DispatcherRetryTimeMillisMax:         DefaultEventRetryTimeMillisMax,
		DispatcherRetryExponentialBackoff:    DefaultExponentialBackoff,
		DispatcherCpuLimit:                   resource.MustParse(DispatcherCpuLimit),
		DispatcherCpuRequest:                 resource.MustParse(DispatcherCpuRequest),
		DispatcherMemoryLimit:                resource.MustParse(DispatcherMemoryLimit),
		DispatcherMemoryRequest:              resource.MustParse(DispatcherMemoryRequest),
		ChannelImage:                         ChannelImage,
		ChannelReplicas:                      ChannelReplicas,
		ChannelMemoryRequest:                 resource.MustParse(ChannelMemoryRequest),
		ChannelMemoryLimit:                   resource.MustParse(ChannelMemoryLimit),
		ChannelCpuRequest:                    resource.MustParse(ChannelCpuRequest),
		ChannelCpuLimit:                      resource.MustParse(ChannelCpuLimit),
	}
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
		},
		Data: map[string][]byte{
			constants.KafkaSecretDataKeyBrokers:  []byte(KafkaSecretDataValueBrokers),
			constants.KafkaSecretDataKeyUsername: []byte(KafkaSecretDataValueUsername),
			constants.KafkaSecretDataKeyPassword: []byte(KafkaSecretDataValuePassword),
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
	deleteTime := metav1.NewTime(time.Unix(1e9, 0))
	secret.ObjectMeta.SetDeletionTimestamp(&deleteTime)
}

// Set The Kafka Secret's Finalizer
func WithKafkaSecretFinalizer(secret *corev1.Secret) {
	secret.ObjectMeta.Finalizers = []string{constants.KnativeKafkaFinalizerPrefix + "kafkasecrets.knativekafka.kyma-project.io"}
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
		Patch:     []byte(`{"metadata":{"finalizers":["knative-kafka/kafkasecrets.knativekafka.kyma-project.io"],"resourceVersion":""}}`),
		// Above finalizer name matches package private "defaultFinalizerName" constant in injection/reconciler/knativekafka/v1alpha1/kafkachannel ;)
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
type KafkaChannelOption func(*kafkav1alpha1.KafkaChannel)

// Utility Function For Creating A Custom KafkaChannel For Testing
func NewKafkaChannel(options ...KafkaChannelOption) *kafkav1alpha1.KafkaChannel {

	// Create The Specified KafkaChannel
	kafkachannel := &kafkav1alpha1.KafkaChannel{
		TypeMeta: metav1.TypeMeta{
			APIVersion: kafkav1alpha1.SchemeGroupVersion.String(),
			Kind:       constants.KafkaChannelKind,
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: KafkaChannelNamespace,
			Name:      KafkaChannelName,
		},
		Spec: kafkav1alpha1.KafkaChannelSpec{
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
func WithInitializedConditions(kafkachannel *kafkav1alpha1.KafkaChannel) {
	kafkachannel.Status.InitializeConditions()
	kafkachannel.Status.MarkConfigTrue()
}

// Set The KafkaChannel's DeletionTimestamp To Current Time
func WithDeletionTimestamp(kafkachannel *kafkav1alpha1.KafkaChannel) {
	deleteTime := metav1.NewTime(time.Unix(1e9, 0))
	kafkachannel.ObjectMeta.SetDeletionTimestamp(&deleteTime)
}

// Set The KafkaChannel's Finalizer
func WithFinalizer(kafkachannel *kafkav1alpha1.KafkaChannel) {
	kafkachannel.ObjectMeta.Finalizers = []string{"kafkachannels.messaging.knative.dev"}
}

// Set The KafkaChannel's Labels
func WithLabels(kafkachannel *kafkav1alpha1.KafkaChannel) {
	kafkachannel.ObjectMeta.Labels = map[string]string{
		constants.KafkaTopicLabel:  fmt.Sprintf("%s.%s", KafkaChannelNamespace, KafkaChannelName),
		constants.KafkaSecretLabel: KafkaSecretName,
	}
}

// Set The KafkaChannel's Address
func WithAddress(kafkachannel *kafkav1alpha1.KafkaChannel) {
	kafkachannel.Status.SetAddress(&apis.URL{
		Scheme: "http",
		Host:   fmt.Sprintf("%s-kafkachannel.%s.svc.cluster.local", KafkaChannelName, KafkaChannelNamespace),
	})
}

// Set The KafkaChannel's Service As READY
func WithKafkaChannelServiceReady(kafkachannel *kafkav1alpha1.KafkaChannel) {
	kafkachannel.Status.MarkChannelServiceTrue()
}

// Set The KafkaChannel's Services As Failed
func WithKafkaChannelServiceFailed(kafkachannel *kafkav1alpha1.KafkaChannel) {
	kafkachannel.Status.MarkChannelServiceFailed(event.KafkaChannelServiceReconciliationFailed.String(), fmt.Sprintf("Failed To Create KafkaChannel Service: inducing failure for create services"))
}

// Set The KafkaChannel's Channel Service As READY
func WithChannelServiceReady(kafkachannel *kafkav1alpha1.KafkaChannel) {
	kafkachannel.Status.MarkServiceTrue()
}

// Set The KafkaChannel's Channel Service As Failed
func WithChannelServiceFailed(kafkachannel *kafkav1alpha1.KafkaChannel) {
	kafkachannel.Status.MarkServiceFailed(event.ChannelServiceReconciliationFailed.String(), "Channel Service Failed: inducing failure for create services")
}

// Set The KafkaChannel's Channel Service As Finalized
func WithChannelServiceFinalized(kafkachannel *kafkav1alpha1.KafkaChannel) {
	kafkachannel.Status.MarkServiceFailed("ChannelServiceUnavailable", "Kafka Auth Secret Finalized")
}

// Set The KafkaChannel's Channel Deployment As READY
func WithChannelDeploymentReady(kafkachannel *kafkav1alpha1.KafkaChannel) {
	kafkachannel.Status.MarkEndpointsTrue()
}

// Set The KafkaChannel's Channel Deployment As Failed
func WithChannelDeploymentFailed(kafkachannel *kafkav1alpha1.KafkaChannel) {
	kafkachannel.Status.MarkEndpointsFailed(event.ChannelDeploymentReconciliationFailed.String(), "Channel Deployment Failed: inducing failure for create deployments")
}

// Set The KafkaChannel's Channel Deployment As Finalized
func WithChannelDeploymentFinalized(kafkachannel *kafkav1alpha1.KafkaChannel) {
	kafkachannel.Status.MarkEndpointsFailed("ChannelDeploymentUnavailable", "Kafka Auth Secret Finalized")
}

// Set The KafkaChannel's Dispatcher Deployment As READY
func WithDispatcherDeploymentReady(kafkachannel *kafkav1alpha1.KafkaChannel) {
	// TODO - This is unnecessary since the testing framework doesn't return any Status Conditions from the K8S commands (Create, Get)
	//        which means the propagate function doesn't do anything.  This is a testing gap with the framework and propagateDispatcherStatus()
	// kafkachannel.Status.PropagateDispatcherStatus()
}

// Set The KafkaChannel's Dispatcher Deployment As Failed
func WithDispatcherFailed(kafkachannel *kafkav1alpha1.KafkaChannel) {
	kafkachannel.Status.MarkDispatcherFailed(event.DispatcherDeploymentReconciliationFailed.String(), "Failed To Create Dispatcher Deployment: inducing failure for create deployments")
}

// Set The KafkaChannel's Topic READY
func WithTopicReady(kafkachannel *kafkav1alpha1.KafkaChannel) {
	kafkachannel.Status.MarkTopicTrue()
}

// Utility Function For Creating A Custom KafkaChannel "Channel" Service For Testing
func NewKafkaChannelService() *corev1.Service {
	return &corev1.Service{
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
				constants.KafkaChannelChannelLabel:   "true",
				constants.K8sAppChannelSelectorLabel: constants.K8sAppChannelSelectorValue,
			},
			OwnerReferences: []metav1.OwnerReference{
				NewChannelOwnerRef(),
			},
		},
		Spec: corev1.ServiceSpec{
			Type:         corev1.ServiceTypeExternalName,
			ExternalName: ChannelDeploymentName + "." + constants.KnativeEventingNamespace + ".svc.cluster.local",
		},
	}
}

// Utility Function For Creating A Custom KafkaChannel "Deployment" Service For Testing
func NewKafkaChannelChannelService() *corev1.Service {
	return &corev1.Service{
		TypeMeta: metav1.TypeMeta{
			APIVersion: corev1.SchemeGroupVersion.String(),
			Kind:       constants.ServiceKind,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      ChannelDeploymentName,
			Namespace: constants.KnativeEventingNamespace,
			Labels: map[string]string{
				"k8s-app":              "knative-kafka-channels",
				"kafkachannel-channel": "true",
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
				"app": ChannelDeploymentName,
			},
		},
	}
}

// Utility Function For Creating A K8S Channel Deployment For The Test Channel
func NewKafkaChannelChannelDeployment() *appsv1.Deployment {
	replicas := int32(ChannelReplicas)
	return &appsv1.Deployment{
		TypeMeta: metav1.TypeMeta{
			APIVersion: appsv1.SchemeGroupVersion.String(),
			Kind:       constants.DeploymentKind,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      ChannelDeploymentName,
			Namespace: constants.KnativeEventingNamespace,
			Labels: map[string]string{
				"app":                  ChannelDeploymentName,
				"kafkachannel-channel": "true",
			},
			OwnerReferences: []metav1.OwnerReference{
				NewSecretOwnerRef(),
			},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app": ChannelDeploymentName,
				},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app": ChannelDeploymentName,
					},
				},
				Spec: corev1.PodSpec{
					ServiceAccountName: ServiceAccount,
					Containers: []corev1.Container{
						{
							Name: ChannelDeploymentName,
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
							Image: ChannelImage,
							Ports: []corev1.ContainerPort{
								{
									Name:          "server",
									ContainerPort: int32(8080),
								},
							},
							Env: []corev1.EnvVar{
								{
									Name:  system.NamespaceEnvKey,
									Value: constants.KnativeEventingNamespace,
								},
								{
									Name:  logging.ConfigMapNameEnv,
									Value: logging.ConfigMapName(),
								},
								{
									Name:  env.MetricsPortEnvVarKey,
									Value: strconv.Itoa(MetricsPort),
								},
								{
									Name:  env.HealthPortEnvVarKey,
									Value: strconv.Itoa(HealthPort),
								},
								{
									Name: env.KafkaBrokerEnvVarKey,
									ValueFrom: &corev1.EnvVarSource{
										SecretKeyRef: &corev1.SecretKeySelector{
											LocalObjectReference: corev1.LocalObjectReference{Name: KafkaSecretName},
											Key:                  constants.KafkaSecretDataKeyBrokers,
										},
									},
								},
								{
									Name: env.KafkaUsernameEnvVarKey,
									ValueFrom: &corev1.EnvVarSource{
										SecretKeyRef: &corev1.SecretKeySelector{
											LocalObjectReference: corev1.LocalObjectReference{Name: KafkaSecretName},
											Key:                  constants.KafkaSecretDataKeyUsername,
										},
									},
								},
								{
									Name: env.KafkaPasswordEnvVarKey,
									ValueFrom: &corev1.EnvVarSource{
										SecretKeyRef: &corev1.SecretKeySelector{
											LocalObjectReference: corev1.LocalObjectReference{Name: KafkaSecretName},
											Key:                  constants.KafkaSecretDataKeyPassword,
										},
									},
								},
							},
							ImagePullPolicy: corev1.PullAlways,
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse(ChannelCpuRequest),
									corev1.ResourceMemory: resource.MustParse(ChannelMemoryRequest),
								},
								Limits: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse(ChannelCpuLimit),
									corev1.ResourceMemory: resource.MustParse(ChannelMemoryLimit),
								},
							},
						},
					},
				},
			},
		},
	}
}

// Utility Function For Creating A Custom KafkaChannel Dispatcher Service For Testing
func NewKafkaChannelDispatcherService() *corev1.Service {

	// Get The Expected Service Name For The Test KafkaChannel
	serviceName := util.DispatcherDnsSafeName(&kafkav1alpha1.KafkaChannel{
		ObjectMeta: metav1.ObjectMeta{Namespace: KafkaChannelNamespace, Name: KafkaChannelName},
	})

	return &corev1.Service{
		TypeMeta: metav1.TypeMeta{
			APIVersion: corev1.SchemeGroupVersion.String(),
			Kind:       constants.ServiceKind,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      serviceName,
			Namespace: constants.KnativeEventingNamespace,
			Labels: map[string]string{
				constants.KafkaChannelNameLabel:       KafkaChannelName,
				constants.KafkaChannelNamespaceLabel:  KafkaChannelNamespace,
				constants.KafkaChannelDispatcherLabel: "true",
				constants.K8sAppChannelSelectorLabel:  constants.K8sAppDispatcherSelectorValue,
			},
			OwnerReferences: []metav1.OwnerReference{
				NewChannelOwnerRef(),
			},
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
}

// Utility Function For Creating A Custom KafkaChannel Dispatcher Deployment For Testing
func NewKafkaChannelDispatcherDeployment() *appsv1.Deployment {

	// Get The Expected Dispatcher & Topic Names For The Test KafkaChannel
	sparseKafkaChannel := &kafkav1alpha1.KafkaChannel{ObjectMeta: metav1.ObjectMeta{Namespace: KafkaChannelNamespace, Name: KafkaChannelName}}
	dispatcherName := util.DispatcherDnsSafeName(sparseKafkaChannel)
	topicName := util.TopicName(sparseKafkaChannel)

	// Replicas Int Reference
	replicas := int32(DispatcherReplicas)

	return &appsv1.Deployment{
		TypeMeta: metav1.TypeMeta{
			APIVersion: appsv1.SchemeGroupVersion.String(),
			Kind:       constants.DeploymentKind,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      dispatcherName,
			Namespace: constants.KnativeEventingNamespace,
			Labels: map[string]string{
				constants.AppLabel:                    dispatcherName,
				constants.KafkaChannelNameLabel:       KafkaChannelName,
				constants.KafkaChannelNamespaceLabel:  KafkaChannelNamespace,
				constants.KafkaChannelDispatcherLabel: "true",
			},
			OwnerReferences: []metav1.OwnerReference{
				NewChannelOwnerRef(),
			},
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
									Value: constants.KnativeEventingNamespace,
								},
								{
									Name:  logging.ConfigMapNameEnv,
									Value: logging.ConfigMapName(),
								},
								{
									Name:  env.MetricsPortEnvVarKey,
									Value: strconv.Itoa(MetricsPort),
								},
								{
									Name:  env.HealthPortEnvVarKey,
									Value: strconv.Itoa(HealthPort),
								},
								{
									Name:  env.ChannelKeyEnvVarKey,
									Value: fmt.Sprintf("%s/%s", KafkaChannelNamespace, KafkaChannelName),
								},
								{
									Name:  env.KafkaTopicEnvVarKey,
									Value: topicName,
								},
								{
									Name:  env.KafkaOffsetCommitMessageCountEnvVarKey,
									Value: strconv.Itoa(KafkaOffsetCommitMessageCount),
								},
								{
									Name:  env.KafkaOffsetCommitDurationMillisEnvVarKey,
									Value: strconv.Itoa(KafkaOffsetCommitDurationMillis),
								},
								{
									Name:  env.ExponentialBackoffEnvVarKey,
									Value: strconv.FormatBool(DefaultExponentialBackoff),
								},
								{
									Name:  env.InitialRetryIntervalEnvVarKey,
									Value: strconv.Itoa(DefaultEventRetryInitialIntervalMillis),
								},
								{
									Name:  env.MaxRetryTimeEnvVarKey,
									Value: strconv.Itoa(DefaultEventRetryTimeMillisMax),
								},
								{
									Name: env.KafkaBrokerEnvVarKey,
									ValueFrom: &corev1.EnvVarSource{
										SecretKeyRef: &corev1.SecretKeySelector{
											LocalObjectReference: corev1.LocalObjectReference{Name: KafkaSecretName},
											Key:                  constants.KafkaSecretDataKeyBrokers,
										},
									},
								},
								{
									Name: env.KafkaUsernameEnvVarKey,
									ValueFrom: &corev1.EnvVarSource{
										SecretKeyRef: &corev1.SecretKeySelector{
											LocalObjectReference: corev1.LocalObjectReference{Name: KafkaSecretName},
											Key:                  constants.KafkaSecretDataKeyUsername,
										},
									},
								},
								{
									Name: env.KafkaPasswordEnvVarKey,
									ValueFrom: &corev1.EnvVarSource{
										SecretKeyRef: &corev1.SecretKeySelector{
											LocalObjectReference: corev1.LocalObjectReference{Name: KafkaSecretName},
											Key:                  constants.KafkaSecretDataKeyPassword,
										},
									},
								},
							},
							ImagePullPolicy: corev1.PullAlways,
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
		APIVersion:         kafkav1alpha1.SchemeGroupVersion.String(),
		Kind:               constants.KafkaChannelKind,
		Name:               KafkaChannelName,
		UID:                "",
		BlockOwnerDeletion: &blockOwnerDeletion,
		Controller:         &controller,
	}
}

// Utility Function For Creating A UpdateActionImpl For The KafkaChannel Labels Update Command
func NewKafkaChannelLabelUpdate(kafkachannel *kafkav1alpha1.KafkaChannel) clientgotesting.UpdateActionImpl {
	return clientgotesting.UpdateActionImpl{
		ActionImpl: clientgotesting.ActionImpl{
			Namespace:   "KafkaChannelNamespace",
			Verb:        "update",
			Resource:    schema.GroupVersionResource{Group: kafkav1alpha1.SchemeGroupVersion.Group, Version: kafkav1alpha1.SchemeGroupVersion.Version, Resource: "kafkachannels"},
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
			Resource:    schema.GroupVersionResource{Group: kafkav1alpha1.SchemeGroupVersion.Group, Version: kafkav1alpha1.SchemeGroupVersion.Version, Resource: "kafkachannels"},
			Subresource: "",
		},
		Name:      KafkaChannelName,
		PatchType: "application/merge-patch+json",
		Patch:     []byte(`{"metadata":{"finalizers":["kafkachannels.messaging.knative.dev"],"resourceVersion":""}}`),
		// Above finalizer name matches package private "defaultFinalizerName" constant in injection/reconciler/knativekafka/v1alpha1/kafkachannel ;)
	}
}

// Utility Function For Creating A Successful KafkaChannel Reconciled Event
func NewKafkaChannelSuccessfulReconciliationEvent() string {
	return reconcilertesting.Eventf(corev1.EventTypeNormal, event.KafkaChannelReconciled.String(), `KafkaChannel Reconciled Successfully: "%s/%s"`, KafkaChannelNamespace, KafkaChannelName)
}

// Utility Function For Creating A Failed KafkaChannel Reconciled Event
func NewKafkaChannelFailedReconciliationEvent() string {
	return reconcilertesting.Eventf(corev1.EventTypeWarning, "InternalError", constants.ReconciliationFailedError)
}

// Utility Function For Creating A Successful KafkaChannel Finalizer Update Event
func NewKafkaChannelFinalizerUpdateEvent() string {
	return reconcilertesting.Eventf(corev1.EventTypeNormal, "FinalizerUpdate", `Updated "%s" finalizers`, KafkaChannelName)
}

// Utility Function For Creating A Successful KafkaChannel Finalizer Update Event
func NewKafkaChannelSuccessfulFinalizedEvent() string {
	return reconcilertesting.Eventf(corev1.EventTypeNormal, event.KafkaChannelFinalized.String(), fmt.Sprintf("KafkaChannel Finalized Successfully: \"%s/%s\"", KafkaChannelNamespace, KafkaChannelName))
}
