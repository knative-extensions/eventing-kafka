package kafkasecret

import (
	"context"
	"fmt"
	"go.uber.org/zap"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	commonenv "knative.dev/eventing-kafka/pkg/common/env"
	"knative.dev/eventing-kafka/pkg/common/health"
	"knative.dev/eventing-kafka/pkg/controller/constants"
	"knative.dev/eventing-kafka/pkg/controller/event"
	"knative.dev/eventing-kafka/pkg/controller/util"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/system"
	"strconv"
)

// Reconcile The "Channel" Inbound For The Specified Kafka Secret
func (r *Reconciler) reconcileChannel(ctx context.Context, secret *corev1.Secret) error {

	// Get Secret Specific Logger
	logger := util.SecretLogger(r.logger, secret)

	// Reconcile The Channel's Service
	serviceErr := r.reconcileChannelService(secret)
	if serviceErr != nil {
		controller.GetEventRecorder(ctx).Eventf(secret, corev1.EventTypeWarning, event.ChannelServiceReconciliationFailed.String(), "Failed To Reconcile Channel Service: %v", serviceErr)
		logger.Error("Failed To Reconcile Channel Service", zap.Error(serviceErr))
	} else {
		logger.Info("Successfully Reconciled Channel Service")
	}

	// Reconcile The Channel's Deployment
	deploymentErr := r.reconcileChannelDeployment(secret)
	if deploymentErr != nil {
		controller.GetEventRecorder(ctx).Eventf(secret, corev1.EventTypeWarning, event.ChannelDeploymentReconciliationFailed.String(), "Failed To Reconcile Channel Deployment: %v", deploymentErr)
		logger.Error("Failed To Reconcile Channel Deployment", zap.Error(deploymentErr))
	} else {
		logger.Info("Successfully Reconciled Channel Deployment")
	}

	// Reconcile Channel's KafkaChannel Status
	statusErr := r.reconcileKafkaChannelStatus(secret,
		serviceErr == nil, event.ChannelServiceReconciliationFailed.String(), fmt.Sprintf("Channel Service Failed: %v", serviceErr),
		deploymentErr == nil, event.ChannelDeploymentReconciliationFailed.String(), fmt.Sprintf("Channel Deployment Failed: %v", deploymentErr))
	if statusErr != nil {
		controller.GetEventRecorder(ctx).Eventf(secret, corev1.EventTypeWarning, event.ChannelStatusReconciliationFailed.String(), "Failed To Reconcile Channel's KafkaChannel Status: %v", statusErr)
		logger.Error("Failed To Reconcile KafkaChannel Status", zap.Error(statusErr))
	} else {
		logger.Info("Successfully Reconciled KafkaChannel Status")
	}

	// Return Results
	if serviceErr != nil || deploymentErr != nil || statusErr != nil {
		return fmt.Errorf("failed to reconcile channel resources")
	} else {
		return nil // Success
	}
}

//
// Kafka Channel Service
//

// Reconcile The Kafka Channel Service
func (r *Reconciler) reconcileChannelService(secret *corev1.Secret) error {

	// Attempt To Get The Service Associated With The Specified Secret
	service, err := r.getChannelService(secret)
	if err != nil {

		// If The Service Was Not Found - Then Create A New One For The Secret
		if errors.IsNotFound(err) {

			// Then Create The New Kafka Channel Service
			r.logger.Info("Channel Service Not Found - Creating New One")
			service = r.newChannelService(secret)
			service, err = r.kubeClientset.CoreV1().Services(service.Namespace).Create(service)
			if err != nil {
				r.logger.Error("Failed To Create Channel Service", zap.Error(err))
				return err
			} else {
				r.logger.Info("Successfully Created Channel Service")
				return nil
			}

		} else {

			// Failed In Attempt To Get Kafka Channel Service From K8S
			r.logger.Error("Failed To Get Channel Service", zap.Error(err))
			return err
		}
	} else {

		// Verified The Channel Service Exists
		r.logger.Info("Successfully Verified Channel Service")
		return nil
	}
}

// Get The Kafka Service Associated With The Specified Channel
func (r *Reconciler) getChannelService(secret *corev1.Secret) (*corev1.Service, error) {

	// Get The Dispatcher Deployment Name For The Channel - Use Same For Service
	deploymentName := util.ChannelDnsSafeName(secret.Name)

	// Get The Service By Namespace / Name
	service := &corev1.Service{}
	service, err := r.serviceLister.Services(constants.KnativeEventingNamespace).Get(deploymentName)

	// Return The Results
	return service, err
}

// Create Kafka Service Model For The Specified Channel
func (r *Reconciler) newChannelService(secret *corev1.Secret) *corev1.Service {

	// Get The Dispatcher Deployment Name For The Channel - Use Same For Service
	deploymentName := util.ChannelDnsSafeName(secret.Name)

	// Create & Return The Service Model
	return &corev1.Service{
		TypeMeta: metav1.TypeMeta{
			APIVersion: corev1.SchemeGroupVersion.String(),
			Kind:       constants.ServiceKind,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      deploymentName,
			Namespace: constants.KnativeEventingNamespace,
			Labels: map[string]string{
				constants.KafkaChannelChannelLabel:   "true",                               // Allows for identification of KafkaChannels
				constants.K8sAppChannelSelectorLabel: constants.K8sAppChannelSelectorValue, // Prometheus ServiceMonitor
			},
			OwnerReferences: []metav1.OwnerReference{
				util.NewSecretOwnerReference(secret),
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
					Name:       constants.MetricsPortName,
					Port:       int32(r.environment.MetricsPort),
					TargetPort: intstr.FromInt(r.environment.MetricsPort),
				},
			},
			Selector: map[string]string{
				constants.AppLabel: deploymentName, // Matches Deployment Label Key/Value
			},
		},
	}
}

//
// KafkaChannel Deployment - The Kafka Producer Implementation
//

// Reconcile The Kafka Channel Deployment
func (r *Reconciler) reconcileChannelDeployment(secret *corev1.Secret) error {

	// Attempt To Get The KafkaChannel Deployment Associated With The Specified Channel
	deployment, err := r.getChannelDeployment(secret)
	if err != nil {

		// If The KafkaChannel Deployment Was Not Found - Then Create A New Deployment For The Channel
		if errors.IsNotFound(err) {

			// Then Create The New Deployment
			r.logger.Info("KafkaChannel Deployment Not Found - Creating New One")
			deployment, err = r.newChannelDeployment(secret)
			if err != nil {
				r.logger.Error("Failed To Create KafkaChannel Deployment YAML", zap.Error(err))
				return err
			} else {
				deployment, err = r.kubeClientset.AppsV1().Deployments(deployment.Namespace).Create(deployment)
				if err != nil {
					r.logger.Error("Failed To Create KafkaChannel Deployment", zap.Error(err))
					return err
				} else {
					r.logger.Info("Successfully Created KafkaChannel Deployment")
					return nil
				}
			}

		} else {

			// Failed In Attempt To Get Deployment From K8S
			r.logger.Error("Failed To Get KafkaChannel Deployment", zap.Error(err))
			return err
		}
	} else {

		// Verified The Channel Service Exists
		r.logger.Info("Successfully Verified Channel Deployment")
		return nil
	}
}

// Get The Kafka Channel Deployment Associated With The Specified Secret
func (r *Reconciler) getChannelDeployment(secret *corev1.Secret) (*appsv1.Deployment, error) {

	// Get The Channel Deployment Name (One Channel Deployment Per Kafka Auth Secret)
	deploymentName := util.ChannelDnsSafeName(secret.Name)

	// Get The Channel Deployment By Namespace / Name
	deployment := &appsv1.Deployment{}
	deployment, err := r.deploymentLister.Deployments(constants.KnativeEventingNamespace).Get(deploymentName)

	// Return The Results
	return deployment, err
}

// Create Kafka Channel Deployment Model For The Specified Secret
func (r *Reconciler) newChannelDeployment(secret *corev1.Secret) (*appsv1.Deployment, error) {

	// Get The Channel Deployment Name (One Channel Deployment Per Kafka Auth Secret)
	deploymentName := util.ChannelDnsSafeName(secret.Name)

	// Replicas Int Value For De-Referencing
	replicas := int32(r.environment.ChannelReplicas)

	// Create The Channel Container Environment Variables
	channelEnvVars, err := r.channelDeploymentEnvVars(secret)
	if err != nil {
		r.logger.Error("Failed To Create Channel Deployment Environment Variables", zap.Error(err))
		return nil, err
	}

	// Create & Return The Channel's Deployment
	deployment := &appsv1.Deployment{
		TypeMeta: metav1.TypeMeta{
			APIVersion: appsv1.SchemeGroupVersion.String(),
			Kind:       constants.DeploymentKind,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      deploymentName,
			Namespace: constants.KnativeEventingNamespace,
			Labels: map[string]string{
				constants.AppLabel:                 deploymentName, // Matches Service Selector Key/Value Below
				constants.KafkaChannelChannelLabel: "true",         // Allows for identification of KafkaChannels
			},
			OwnerReferences: []metav1.OwnerReference{
				util.NewSecretOwnerReference(secret),
			},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					constants.AppLabel: deploymentName, // Matches Template ObjectMeta Pods
				},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						constants.AppLabel: deploymentName, // Matched By Deployment Selector Above
					},
				},
				Spec: corev1.PodSpec{
					ServiceAccountName: r.environment.ServiceAccount,
					Containers: []corev1.Container{
						{
							Name: deploymentName,
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
							Image: r.environment.ChannelImage,
							Ports: []corev1.ContainerPort{
								{
									Name:          "server",
									ContainerPort: int32(constants.HttpContainerPortNumber),
								},
							},
							Env:             channelEnvVars,
							ImagePullPolicy: corev1.PullIfNotPresent,
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    r.environment.ChannelCpuRequest,
									corev1.ResourceMemory: r.environment.ChannelMemoryRequest,
								},
								Limits: corev1.ResourceList{
									corev1.ResourceCPU:    r.environment.ChannelCpuLimit,
									corev1.ResourceMemory: r.environment.ChannelMemoryLimit,
								},
							},
						},
					},
				},
			},
		},
	}

	// Return Channel Deployment
	return deployment, nil
}

// Create The Kafka Channel Deployment's Env Vars
func (r *Reconciler) channelDeploymentEnvVars(secret *corev1.Secret) ([]corev1.EnvVar, error) {

	// Create The Channel Deployment EnvVars
	envVars := []corev1.EnvVar{
		{
			Name:  system.NamespaceEnvKey,
			Value: constants.KnativeEventingNamespace,
		},
		{
			Name:  logging.ConfigMapNameEnv,
			Value: logging.ConfigMapName(),
		},
		{
			Name:  commonenv.ServiceNameEnvVarKey,
			Value: util.ChannelDnsSafeName(secret.Name),
		},
		{
			Name:  commonenv.MetricsPortEnvVarKey,
			Value: strconv.Itoa(r.environment.MetricsPort),
		},
		{
			Name:  commonenv.HealthPortEnvVarKey,
			Value: strconv.Itoa(constants.HealthPort),
		},
	}

	// Append The Kafka Brokers As Env Var
	envVars = append(envVars, corev1.EnvVar{
		Name: commonenv.KafkaBrokerEnvVarKey,
		ValueFrom: &corev1.EnvVarSource{
			SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: secret.Name},
				Key:                  constants.KafkaSecretDataKeyBrokers,
			},
		},
	})

	// Append The Kafka Username As Env Var
	envVars = append(envVars, corev1.EnvVar{
		Name: commonenv.KafkaUsernameEnvVarKey,
		ValueFrom: &corev1.EnvVarSource{
			SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: secret.Name},
				Key:                  constants.KafkaSecretDataKeyUsername,
			},
		},
	})

	// Append The Kafka Password As Env Var
	envVars = append(envVars, corev1.EnvVar{
		Name: commonenv.KafkaPasswordEnvVarKey,
		ValueFrom: &corev1.EnvVarSource{
			SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: secret.Name},
				Key:                  constants.KafkaSecretDataKeyPassword,
			},
		},
	})

	// Return The Channel Deployment EnvVars Array
	return envVars, nil
}
