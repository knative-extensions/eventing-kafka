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

package kafkasecret

import (
	"context"
	"fmt"
	"strconv"

	"go.uber.org/zap"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	commonconstants "knative.dev/eventing-kafka/pkg/common/constants"
	commonenv "knative.dev/eventing-kafka/pkg/channel/distributed/common/env"
	"knative.dev/eventing-kafka/pkg/channel/distributed/common/health"
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/constants"
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/event"
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/util"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/system"
)

// Reconcile The Receiver (Kafka Producer) For The Specified KafkaChannel
func (r *Reconciler) reconcileChannel(ctx context.Context, secret *corev1.Secret) error {

	// Get Secret Specific Logger
	logger := util.SecretLogger(r.logger, secret)

	// Reconcile The Receiver Service
	serviceErr := r.reconcileReceiverService(ctx, logger, secret)
	if serviceErr != nil {
		controller.GetEventRecorder(ctx).Eventf(secret, corev1.EventTypeWarning, event.ReceiverServiceReconciliationFailed.String(), "Failed To Reconcile Receiver Service: %v", serviceErr)
		logger.Error("Failed To Reconcile Receiver Service", zap.Error(serviceErr))
	} else {
		logger.Info("Successfully Reconciled Receiver Service")
	}

	// Reconcile The Receiver Deployment
	deploymentErr := r.reconcileReceiverDeployment(ctx, logger, secret)
	if deploymentErr != nil {
		controller.GetEventRecorder(ctx).Eventf(secret, corev1.EventTypeWarning, event.ReceiverDeploymentReconciliationFailed.String(), "Failed To Reconcile Receiver Deployment: %v", deploymentErr)
		logger.Error("Failed To Reconcile Receiver Deployment", zap.Error(deploymentErr))
	} else {
		logger.Info("Successfully Reconciled Receiver Deployment")
	}

	// Reconcile Channel's KafkaChannel Status
	statusErr := r.reconcileKafkaChannelStatus(ctx,
		secret,
		serviceErr == nil, event.ReceiverServiceReconciliationFailed.String(), fmt.Sprintf("Receiver Service Failed: %v", serviceErr),
		deploymentErr == nil, event.ReceiverDeploymentReconciliationFailed.String(), fmt.Sprintf("Receiver Deployment Failed: %v", deploymentErr))
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
// Kafka Receiver Service
//

// Reconcile The Receiver Service
func (r *Reconciler) reconcileReceiverService(ctx context.Context, logger *zap.Logger, secret *corev1.Secret) error {

	// Attempt To Get The Receiver Service Associated With The Specified Secret
	_, err := r.getReceiverService(secret)
	if err != nil {

		// If The Service Was Not Found - Then Create A New One For The Secret
		if errors.IsNotFound(err) {

			// Then Create The New Receiver Service
			logger.Info("Receiver Service Not Found - Creating New One")
			service := r.newReceiverService(secret)
			_, err = r.kubeClientset.CoreV1().Services(service.Namespace).Create(ctx, service, metav1.CreateOptions{})
			if err != nil {
				logger.Error("Failed To Create Receiver Service", zap.Error(err))
				return err
			} else {
				logger.Info("Successfully Created Receiver Service")
				return nil
			}

		} else {

			// Failed In Attempt To Get Receiver Service From K8S
			logger.Error("Failed To Get Receiver Service", zap.Error(err))
			return err
		}
	} else {

		// Verified The Receiver Service Exists
		logger.Info("Successfully Verified Receiver Service")
		return nil
	}
}

// Get The Kafka Receiver Service Associated With The Specified Channel
func (r *Reconciler) getReceiverService(secret *corev1.Secret) (*corev1.Service, error) {

	// Get The Receiver Deployment Name For The Receiver - Use Same For Service
	deploymentName := util.ReceiverDnsSafeName(secret.Name)

	// Get The Receiver Service By Namespace / Name
	service, err := r.serviceLister.Services(commonconstants.KnativeEventingNamespace).Get(deploymentName)

	// Return The Results
	return service, err
}

// Create Receiver Service Model For The Specified Secret
func (r *Reconciler) newReceiverService(secret *corev1.Secret) *corev1.Service {

	// Get The Receiver Deployment Name For The Secret - Use Same For Service
	deploymentName := util.ReceiverDnsSafeName(secret.Name)

	// Create & Return The Receiver Service Model
	return &corev1.Service{
		TypeMeta: metav1.TypeMeta{
			APIVersion: corev1.SchemeGroupVersion.String(),
			Kind:       constants.ServiceKind,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      deploymentName,
			Namespace: commonconstants.KnativeEventingNamespace,
			Labels: map[string]string{
				constants.KafkaChannelReceiverLabel:  "true",                               // Allows for identification of Receivers
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
// Kafka Receiver Deployment - The Kafka Producer Implementation
//

// Reconcile The Receiver Deployment
func (r *Reconciler) reconcileReceiverDeployment(ctx context.Context, logger *zap.Logger, secret *corev1.Secret) error {

	// Attempt To Get The Receiver Deployment Associated With The Specified Secret
	_, err := r.getReceiverDeployment(secret)
	if err != nil {

		// If The Receiver Deployment Was Not Found - Then Create A New Deployment For The Secret
		if errors.IsNotFound(err) {

			// Then Create The New Receiver Deployment
			logger.Info("Receiver Deployment Not Found - Creating New One")
			deployment, err := r.newReceiverDeployment(logger, secret)
			if err != nil {
				logger.Error("Failed To Create Receiver Deployment YAML", zap.Error(err))
				return err
			} else {
				_, err = r.kubeClientset.AppsV1().Deployments(deployment.Namespace).Create(ctx, deployment, metav1.CreateOptions{})
				if err != nil {
					logger.Error("Failed To Create Receiver Deployment", zap.Error(err))
					return err
				} else {
					logger.Info("Successfully Created Receiver Deployment")
					return nil
				}
			}

		} else {

			// Failed In Attempt To Get Receiver Deployment From K8S
			logger.Error("Failed To Get Receiver Deployment", zap.Error(err))
			return err
		}
	} else {

		// Verified The Receiver Deployment Exists
		logger.Info("Successfully Verified Receiver Deployment")
		return nil
	}
}

// Get The Receiver Deployment Associated With The Specified Secret
func (r *Reconciler) getReceiverDeployment(secret *corev1.Secret) (*appsv1.Deployment, error) {

	// Get The Receiver Deployment Name (One Receiver Deployment Per Kafka Auth Secret)
	deploymentName := util.ReceiverDnsSafeName(secret.Name)

	// Get The Receiver Deployment By Namespace / Name
	deployment, err := r.deploymentLister.Deployments(commonconstants.KnativeEventingNamespace).Get(deploymentName)

	// Return The Results
	return deployment, err
}

// Create Receiver Deployment Model For The Specified Secret
func (r *Reconciler) newReceiverDeployment(logger *zap.Logger, secret *corev1.Secret) (*appsv1.Deployment, error) {

	// Get The Receiver Deployment Name (One Receiver Deployment Per Kafka Auth Secret)
	deploymentName := util.ReceiverDnsSafeName(secret.Name)

	// Replicas Int Value For De-Referencing
	replicas := int32(r.config.Receiver.Replicas)

	// Create The Receiver Container Environment Variables
	channelEnvVars, err := r.receiverDeploymentEnvVars(secret)
	if err != nil {
		logger.Error("Failed To Create Receiver Deployment Environment Variables", zap.Error(err))
		return nil, err
	}

	// Create The Receiver Deployment
	deployment := &appsv1.Deployment{
		TypeMeta: metav1.TypeMeta{
			APIVersion: appsv1.SchemeGroupVersion.String(),
			Kind:       constants.DeploymentKind,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      deploymentName,
			Namespace: commonconstants.KnativeEventingNamespace,
			Labels: map[string]string{
				constants.AppLabel:                  deploymentName, // Matches Service Selector Key/Value Below
				constants.KafkaChannelReceiverLabel: "true",         // Allows for identification of Receivers
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
							Image: r.environment.ReceiverImage,
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
									corev1.ResourceCPU:    r.config.Receiver.CpuRequest,
									corev1.ResourceMemory: r.config.Receiver.MemoryRequest,
								},
								Limits: corev1.ResourceList{
									corev1.ResourceCPU:    r.config.Receiver.CpuLimit,
									corev1.ResourceMemory: r.config.Receiver.MemoryLimit,
								},
							},
						},
					},
				},
			},
		},
	}

	// Return Receiver Deployment
	return deployment, nil
}

// Create The Receiver Deployment's Env Vars
func (r *Reconciler) receiverDeploymentEnvVars(secret *corev1.Secret) ([]corev1.EnvVar, error) {

	// Create The Receiver Deployment EnvVars
	envVars := []corev1.EnvVar{
		{
			Name:  system.NamespaceEnvKey,
			Value: commonconstants.KnativeEventingNamespace,
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
			Name:  commonenv.ContainerNameEnvVarKEy,
			Value: constants.ReceiverContainerName,
		},
		{
			Name:  commonenv.KnativeLoggingConfigMapNameEnvVarKey,
			Value: logging.ConfigMapName(),
		},
		{
			Name:  commonenv.ServiceNameEnvVarKey,
			Value: util.ReceiverDnsSafeName(secret.Name),
		},
		{
			Name:  commonenv.MetricsPortEnvVarKey,
			Value: strconv.Itoa(r.environment.MetricsPort),
		},
		{
			Name:  commonenv.MetricsDomainEnvVarKey,
			Value: r.environment.MetricsDomain,
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

	// Return The Receiver Deployment EnvVars Array
	return envVars, nil
}
