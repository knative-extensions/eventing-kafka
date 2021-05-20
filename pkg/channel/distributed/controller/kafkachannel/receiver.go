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

package kafkachannel

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"knative.dev/eventing-kafka/pkg/apis/messaging/v1beta1"

	"k8s.io/apimachinery/pkg/types"

	"go.uber.org/zap"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	commonenv "knative.dev/eventing-kafka/pkg/channel/distributed/common/env"
	"knative.dev/eventing-kafka/pkg/channel/distributed/common/health"
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/constants"
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/event"
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/util"
	commonconstants "knative.dev/eventing-kafka/pkg/common/constants"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/system"
)

// reconcileReceiver Reconciles The Receiver (Kafka Producer) For The Specified KafkaChannel
func (r *Reconciler) reconcileReceiver(ctx context.Context, channel *v1beta1.KafkaChannel) error {

	secretExists := true
	secret, err := r.kubeClientset.CoreV1().Secrets(r.config.Kafka.AuthSecretNamespace).Get(ctx, r.config.Kafka.AuthSecretName, metav1.GetOptions{})
	if err != nil && errors.IsNotFound(err) {
		// The Service and Deployment reconciler functions need the namespace and name in order to remove
		// the resources, so construct a Secret with the required values even though it couldn't be found
		secret = &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: r.config.Kafka.AuthSecretName, Namespace: r.config.Kafka.AuthSecretNamespace}}
		secretExists = false
	} else if err != nil {
		logging.FromContext(ctx).Error("Error reading receiver secret", zap.Error(err))
		return fmt.Errorf(constants.ReconciliationFailedError)
	}

	// Get A Logger With Secret Info
	logger := logging.FromContext(ctx).Desugar().With(zap.String("Secret", fmt.Sprintf("%s/%s", secret.Namespace, secret.Name)))

	// Reconcile The Receiver Service
	serviceErr := r.reconcileReceiverService(ctx, logger, secret, secretExists)
	if serviceErr != nil {
		controller.GetEventRecorder(ctx).Eventf(secret, corev1.EventTypeWarning, event.ReceiverServiceReconciliationFailed.String(), "Failed To Reconcile Receiver Service: %v", serviceErr)
		logger.Error("Failed To Reconcile Receiver Service", zap.Error(serviceErr))
	} else {
		logger.Info("Successfully Reconciled Receiver Service")
	}

	// Reconcile The Receiver Deployment
	deploymentErr := r.reconcileReceiverDeployment(ctx, logger, secret, secretExists)
	if deploymentErr != nil {
		controller.GetEventRecorder(ctx).Eventf(secret, corev1.EventTypeWarning, event.ReceiverDeploymentReconciliationFailed.String(), "Failed To Reconcile Receiver Deployment: %v", deploymentErr)
		logger.Error("Failed To Reconcile Receiver Deployment", zap.Error(deploymentErr))
	} else {
		logger.Info("Successfully Reconciled Receiver Deployment")
	}

	if serviceErr == nil && !secretExists {
		serviceErr = fmt.Errorf("no secret found")
	}

	if deploymentErr == nil && !secretExists {
		deploymentErr = fmt.Errorf("no secret found")
	}

	// Reconcile Channel's KafkaChannel Status
	statusErr := r.updateKafkaChannelStatus(ctx,
		channel,
		serviceErr == nil, event.ReceiverServiceReconciliationFailed.String(), fmt.Sprintf("Receiver Service Failed: %v", serviceErr),
		deploymentErr == nil, event.ReceiverDeploymentReconciliationFailed.String(), fmt.Sprintf("Receiver Deployment Failed: %v", deploymentErr))
	if statusErr != nil {
		controller.GetEventRecorder(ctx).Eventf(secret, corev1.EventTypeWarning, event.ChannelStatusReconciliationFailed.String(), "Failed To Reconcile Channel's KafkaChannel Status: %v", statusErr)
		logger.Error("Failed To Reconcile KafkaChannel Status", zap.Error(statusErr))
	} else {
		logger.Info("Successfully Reconciled KafkaChannel Status")
	}

	if !secretExists {
		// Not having an auth secret for the Receiver is a problem; fail the reconciliation
		channel.Status.MarkConfigFailed(event.KafkaSecretReconciled.String(), "No Kafka Secret For KafkaChannel")
		return fmt.Errorf(constants.ReconciliationFailedError)
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

// reconcileReceiverService Reconciles The Receiver Service
func (r *Reconciler) reconcileReceiverService(ctx context.Context, logger *zap.Logger, secret *corev1.Secret, secretExists bool) error {

	// Create A New Service For Comparison
	newService := r.newReceiverService()

	// Attempt To Get The Receiver Service Associated With The Specified Secret
	existingService, err := r.getReceiverService()

	if !secretExists {
		// If there is no secret, the receiver service must be deleted (if it exists)
		if existingService != nil {
			logger.Info("Secret Removed - Deleting Receiver Service")
			return r.kubeClientset.CoreV1().Services(newService.Namespace).Delete(ctx, newService.Name, metav1.DeleteOptions{})
		}
		return nil // No secret, no service, no change
	}

	if existingService == nil || err != nil {

		// If The Service Was Not Found - Then Create A New One For The Secret
		if errors.IsNotFound(err) {

			// Then Create The New Receiver Service
			logger.Info("Receiver Service Not Found - Creating New One")
			_, err = r.kubeClientset.CoreV1().Services(newService.Namespace).Create(ctx, newService, metav1.CreateOptions{})
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

		// Verify Receiver Service Is Not Terminating
		if existingService.DeletionTimestamp.IsZero() {
			logger.Info("Successfully Verified Receiver Service")
		} else {
			logger.Warn("Encountered Receiver Service With DeletionTimestamp - Forcing Reconciliation", zap.String("Namespace", existingService.Namespace), zap.String("Name", existingService.Name))
			return fmt.Errorf("encountered Receiver Service with DeletionTimestamp %s/%s - potential race condition", existingService.Namespace, existingService.Name)
		}

		// Determine whether the existing service is different in a way that demands a patch
		// such as missing required labels or spec differences
		patch, needsUpdate := util.CheckServiceChanged(logger, existingService, newService)

		// Patch the service in Kubernetes if necessary
		if needsUpdate {
			_, err = r.kubeClientset.CoreV1().Services(existingService.Namespace).Patch(ctx, existingService.Name, types.JSONPatchType, patch, metav1.PatchOptions{})
			if err == nil {
				controller.GetEventRecorder(ctx).Event(secret, corev1.EventTypeNormal, event.ReceiverServicePatched.String(), "Receiver Service Patched")
				logger.Info("Receiver Service Changed - Patch Applied")
			} else {
				controller.GetEventRecorder(ctx).Event(secret, corev1.EventTypeWarning, event.ReceiverServicePatchFailed.String(), "Receiver Service Patch Failed")
				logger.Error("Receiver Service Patch Failed", zap.Error(err))
				return err
			}
		}
	}
	return nil
}

// getReceiverService Gets The Kafka Receiver Service Associated With The Specified Channel
func (r *Reconciler) getReceiverService() (*corev1.Service, error) {

	// Get The (Single) Receiver Deployment Name - Use Same For Service
	deploymentName := util.ReceiverDnsSafeName(r.config.Kafka.AuthSecretName)

	// Get The Receiver Service By Namespace / Name
	service, err := r.serviceLister.Services(r.environment.SystemNamespace).Get(deploymentName)

	// Return The Results
	return service, err
}

// newReceiverService Creates The Receiver Service Model For The Specified Secret
func (r *Reconciler) newReceiverService() *corev1.Service {

	// Get The (Single) Receiver Deployment Name - Use Same For Service
	deploymentName := util.ReceiverDnsSafeName(r.config.Kafka.AuthSecretName)

	// Create & Return The Receiver Service Model
	return &corev1.Service{
		TypeMeta: metav1.TypeMeta{
			APIVersion: corev1.SchemeGroupVersion.String(),
			Kind:       constants.ServiceKind,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      deploymentName,
			Namespace: r.environment.SystemNamespace,
			Labels: map[string]string{
				constants.KafkaChannelReceiverLabel:  "true",                               // Allows for identification of Receivers
				constants.K8sAppChannelSelectorLabel: constants.K8sAppChannelSelectorValue, // Prometheus ServiceMonitor
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

// reconcileReceiverDeployment Reconciles The Receiver Deployment
func (r *Reconciler) reconcileReceiverDeployment(ctx context.Context, logger *zap.Logger, secret *corev1.Secret, secretExists bool) error {

	// Create A New Deployment For Comparison
	newDeployment := r.newReceiverDeployment(secret)

	// Attempt To Get The Receiver Deployment Associated With The Specified Secret
	existingDeployment, err := r.getReceiverDeployment()

	if !secretExists {
		// If there is no secret, the receiver deployment must be deleted (if it exists)
		if existingDeployment != nil {
			logger.Info("Secret Removed - Deleting Receiver Deployment")
			return r.kubeClientset.AppsV1().Deployments(newDeployment.Namespace).Delete(ctx, newDeployment.Name, metav1.DeleteOptions{})
		}
		return nil // No secret, no deployment, no change
	}

	if existingDeployment == nil || err != nil {

		// If The Receiver Deployment Was Not Found - Then Create A New Deployment For The Secret
		if errors.IsNotFound(err) {

			// Then Create The New Receiver Deployment
			logger.Info("Receiver Deployment Not Found - Creating New One")

			_, err = r.kubeClientset.AppsV1().Deployments(newDeployment.Namespace).Create(ctx, newDeployment, metav1.CreateOptions{})
			if err != nil {
				logger.Error("Failed To Create Receiver Deployment", zap.Error(err))
				return err
			} else {
				logger.Info("Successfully Created Receiver Deployment")
				return nil
			}

		} else {

			// Failed In Attempt To Get Receiver Deployment From K8S
			logger.Error("Failed To Get Receiver Deployment", zap.Error(err))
			return err
		}
	} else {

		// Verify Receiver Deployment Is Not Terminating
		if existingDeployment.DeletionTimestamp.IsZero() {
			logger.Info("Successfully Verified Receiver Deployment")
		} else {
			logger.Warn("Encountered Receiver Deployment With DeletionTimestamp - Forcing Reconciliation", zap.String("Namespace", existingDeployment.Namespace), zap.String("Name", existingDeployment.Name))
			return fmt.Errorf("encountered Receiver Deployment with DeletionTimestamp %s/%s - potential race condition", existingDeployment.Namespace, existingDeployment.Name)
		}

		// Determine whether the existing deployment is different in a way that demands an update
		// such as missing required labels, a different image, or certain container differences.
		// (This includes the configmap hash annotation, so configmap changes will trigger updates)
		updatedDeployment, needsUpdate := util.CheckDeploymentChanged(logger, existingDeployment, newDeployment)

		// Update the deployment in Kubernetes if necessary
		if needsUpdate {
			_, err = r.kubeClientset.AppsV1().Deployments(newDeployment.Namespace).Update(ctx, updatedDeployment, metav1.UpdateOptions{})
			if err == nil {
				controller.GetEventRecorder(ctx).Event(secret, corev1.EventTypeNormal, event.ReceiverDeploymentUpdated.String(), "Receiver Deployment Updated")
				logger.Info("Receiver Deployment Changed - Update Applied")
			} else {
				controller.GetEventRecorder(ctx).Event(secret, corev1.EventTypeWarning, event.ReceiverDeploymentUpdateFailed.String(), "Receiver Deployment Update Failed")
				logger.Error("Receiver Deployment Update Failed", zap.Error(err))
				return err
			}
		}
		return nil
	}
}

// getReceiverDeployment Gets The Receiver Deployment Associated With The Specified Secret
func (r *Reconciler) getReceiverDeployment() (*appsv1.Deployment, error) {

	// Get The (Single) Receiver Deployment Name
	deploymentName := util.ReceiverDnsSafeName(r.config.Kafka.AuthSecretName)

	// Get The Receiver Deployment By Namespace / Name
	deployment, err := r.deploymentLister.Deployments(r.environment.SystemNamespace).Get(deploymentName)

	// Return The Results
	return deployment, err
}

// newReceiverDeployment creates The Receiver Deployment Model For The Specified Secret
func (r *Reconciler) newReceiverDeployment(secret *corev1.Secret) *appsv1.Deployment {

	// Get The (Single) Receiver Deployment Name
	deploymentName := util.ReceiverDnsSafeName(r.config.Kafka.AuthSecretName)

	// Replicas Int Value For De-Referencing
	replicas := int32(r.config.Receiver.Replicas)

	// Create The Receiver Container Environment Variables
	channelEnvVars := r.receiverDeploymentEnvVars(secret)

	// There is a difference between setting an entry in the limits or requests map to the zero-value
	// of a Quantity and not actually having that entry in the map at all.
	// If we want "no limit" or "no request" then the entry must not be present in the map.
	// Note: Since a "Quantity" type has no nil value, we use the Zero value to represent unlimited.
	resourceLimits := make(map[corev1.ResourceName]resource.Quantity)
	if !r.config.Receiver.MemoryLimit.IsZero() {
		resourceLimits[corev1.ResourceMemory] = r.config.Receiver.MemoryLimit
	}
	if !r.config.Receiver.CpuLimit.IsZero() {
		resourceLimits[corev1.ResourceCPU] = r.config.Receiver.CpuLimit
	}
	resourceRequests := make(map[corev1.ResourceName]resource.Quantity)
	if !r.config.Receiver.MemoryRequest.IsZero() {
		resourceRequests[corev1.ResourceMemory] = r.config.Receiver.MemoryRequest
	}
	if !r.config.Receiver.CpuRequest.IsZero() {
		resourceRequests[corev1.ResourceCPU] = r.config.Receiver.CpuRequest
	}

	// If either the limits or requests are an entirely-empty map, this will be translated to a nil
	// value in the resource itself, so pre-emptively set that here.  This prevents the CheckDeploymentChanged
	// function from returning a difference and restarting the receiver unnecessarily.
	if len(resourceLimits) == 0 {
		resourceLimits = nil
	}
	if len(resourceRequests) == 0 {
		resourceRequests = nil
	}

	// Create The Receiver Deployment
	deployment := &appsv1.Deployment{
		TypeMeta: metav1.TypeMeta{
			APIVersion: appsv1.SchemeGroupVersion.String(),
			Kind:       constants.DeploymentKind,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      deploymentName,
			Namespace: r.environment.SystemNamespace,
			Labels: map[string]string{
				constants.AppLabel:                  deploymentName, // Matches Service Selector Key/Value Below
				constants.KafkaChannelReceiverLabel: "true",         // Allows for identification of Receivers
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
					Annotations: map[string]string{
						commonconstants.ConfigMapHashAnnotationKey: r.kafkaConfigMapHash,
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
								Requests: resourceRequests,
								Limits:   resourceLimits,
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
							VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
								LocalObjectReference: corev1.LocalObjectReference{
									Name: commonconstants.SettingsConfigMapName,
								},
							}},
						},
					},
				},
			},
		},
	}

	// Return Receiver Deployment
	return deployment
}

// receiverDeploymentEnvVars creates The Receiver Deployment's Env Vars
func (r *Reconciler) receiverDeploymentEnvVars(secret *corev1.Secret) []corev1.EnvVar {

	// Create The Receiver Deployment EnvVars
	envVars := []corev1.EnvVar{
		{
			Name:  system.NamespaceEnvKey,
			Value: r.environment.SystemNamespace,
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
		{
			Name:  commonenv.ResyncPeriodMinutesEnvVarKey,
			Value: strconv.Itoa(int(r.environment.ResyncPeriod / time.Minute)),
		},
	}

	// Append The Secret Name As Env Var
	envVars = append(envVars, corev1.EnvVar{
		Name:  commonenv.KafkaSecretNameEnvVarKey,
		Value: secret.Name,
	})

	// Append The Secret Namespace As Env Var
	envVars = append(envVars, corev1.EnvVar{
		Name:  commonenv.KafkaSecretNamespaceEnvVarKey,
		Value: secret.Namespace,
	})

	// Return The Receiver Deployment EnvVars Array
	return envVars
}
