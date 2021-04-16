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

	"k8s.io/apimachinery/pkg/types"

	"go.uber.org/zap"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	kafkav1beta1 "knative.dev/eventing-kafka/pkg/apis/messaging/v1beta1"
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

//
// Reconcile & Finalize The Dispatcher (Kafka Consumer) For The Specified KafkaChannel
//
// In most other cases we can simply rely on K8S Garbage Collection to tear down the
// various resources upon finalization.  That mechanism relies on OwnerReferences
// though, and K8S does NOT support cross-namespace OwnerReferences.  Therefore in
// cases where we want to have such cross-namespace ownership, such as the Dispatcher,
// we have to manage the tear-down / cleanup ourselves.
//

// Reconcile The Dispatcher For The Specified KafkaChannel - Ensure Desired State!
func (r *Reconciler) reconcileDispatcher(ctx context.Context, channel *kafkav1beta1.KafkaChannel) error {

	// Get The Channel-Specific Logger Provided Via The Context
	logger := logging.FromContext(ctx).Desugar()

	// Reconcile The Dispatcher's Service (For Prometheus Only)
	serviceErr := r.reconcileDispatcherService(ctx, logger, channel)
	if serviceErr != nil {
		controller.GetEventRecorder(ctx).Eventf(channel, corev1.EventTypeWarning, event.DispatcherServiceReconciliationFailed.String(), "Failed To Reconcile Dispatcher Service: %v", serviceErr)
		logger.Error("Failed To Reconcile Dispatcher Service", zap.Error(serviceErr))
	} else {
		logger.Info("Successfully Reconciled Dispatcher Service")
	}

	// Reconcile The Dispatcher's Deployment
	deploymentErr := r.reconcileDispatcherDeployment(ctx, logger, channel)
	if deploymentErr != nil {
		controller.GetEventRecorder(ctx).Eventf(channel, corev1.EventTypeWarning, event.DispatcherDeploymentReconciliationFailed.String(), "Failed To Reconcile Dispatcher Deployment: %v", deploymentErr)
		logger.Error("Failed To Reconcile Dispatcher Deployment", zap.Error(deploymentErr))
	} else {
		logger.Info("Successfully Reconciled Dispatcher Deployment")
	}

	// Return Results
	if serviceErr != nil || deploymentErr != nil {
		return fmt.Errorf("failed to reconcile dispatcher resources")
	} else {
		return nil
	}
}

// Finalize The Dispatcher For The Specified KafkaChannel - Ensure Manual Deletion
func (r *Reconciler) finalizeDispatcher(ctx context.Context, channel *kafkav1beta1.KafkaChannel) error {

	// Get The Channel-Specific Logger Provided Via The Context
	logger := logging.FromContext(ctx).Desugar()

	// Finalize The Dispatcher's Service
	serviceErr := r.finalizeDispatcherService(ctx, channel)
	if serviceErr != nil {
		controller.GetEventRecorder(ctx).Eventf(channel, corev1.EventTypeWarning, event.DispatcherServiceFinalizationFailed.String(), "Failed To Finalize Dispatcher Service: %v", serviceErr)
		logger.Error("Failed To Finalize Dispatcher Service", zap.Error(serviceErr))
	} else {
		logger.Info("Successfully Finalized Dispatcher Service")
	}

	// Finalize The Dispatcher's Deployment
	deploymentErr := r.finalizeDispatcherDeployment(ctx, logger, channel)
	if deploymentErr != nil {
		controller.GetEventRecorder(ctx).Eventf(channel, corev1.EventTypeWarning, event.DispatcherDeploymentFinalizationFailed.String(), "Failed To Finalize Dispatcher Deployment: %v", deploymentErr)
		logger.Error("Failed To Finalize Dispatcher Deployment", zap.Error(deploymentErr))
	} else {
		logger.Info("Successfully Finalized Dispatcher Deployment")
	}

	// Return Results
	if serviceErr != nil || deploymentErr != nil {
		return fmt.Errorf("failed to finalize dispatcher resources")
	} else {
		return nil
	}
}

//
// Dispatcher Service (For Prometheus Only)
//

// Reconcile The Dispatcher Service
func (r *Reconciler) reconcileDispatcherService(ctx context.Context, logger *zap.Logger, channel *kafkav1beta1.KafkaChannel) error {

	// Create A New Service For Comparison
	newService := r.newDispatcherService(channel)

	// Attempt To Get The Dispatcher Service Associated With The Specified Channel
	existingService, err := r.getDispatcherService(channel)
	if existingService == nil || err != nil {

		// If The Service Was Not Found - Then Create A New One For The Channel
		if errors.IsNotFound(err) {
			logger.Info("Dispatcher Service Not Found - Creating New One")
			_, err = r.kubeClientset.CoreV1().Services(newService.Namespace).Create(ctx, newService, metav1.CreateOptions{})
			if err != nil {
				logger.Error("Failed To Create Dispatcher Service", zap.Error(err))
				return err
			} else {
				logger.Info("Successfully Created Dispatcher Service")
				return nil
			}
		} else {
			logger.Error("Failed To Get Dispatcher Service For Reconciliation", zap.Error(err))
			return err
		}
	} else {

		// Log Deletion Timestamp & Finalizer State
		if existingService.DeletionTimestamp.IsZero() {
			logger.Info("Successfully Verified Dispatcher Service")
		} else {
			if util.HasFinalizer(r.finalizerName(), &newService.ObjectMeta) {
				logger.Info("Blocking Pending Deletion Of Dispatcher Service (Finalizer Detected)")
			} else {
				logger.Warn("Unable To Block Pending Deletion Of Dispatcher Service (Finalizer Missing)")
			}
		}

		// Determine whether the existing service is different in a way that demands a patch
		// such as missing required labels or spec differences
		patch, needsUpdate := util.CheckServiceChanged(logger, existingService, newService)

		// Patch the service in Kubernetes if necessary
		if needsUpdate {
			_, err = r.kubeClientset.CoreV1().Services(existingService.Namespace).Patch(ctx, existingService.Name, types.JSONPatchType, patch, metav1.PatchOptions{})
			if err == nil {
				controller.GetEventRecorder(ctx).Event(channel, corev1.EventTypeNormal, event.DispatcherServicePatched.String(), "Dispatcher Service Patched")
				logger.Info("Dispatcher Service Changed - Patch Applied")
			} else {
				controller.GetEventRecorder(ctx).Event(channel, corev1.EventTypeWarning, event.DispatcherServicePatchFailed.String(), "Dispatcher Service Patch Failed")
				channel.Status.MarkDispatcherFailed(event.DispatcherServicePatchFailed.String(), "Failed To Patch Dispatcher Service: %v", err)
				logger.Error("Dispatcher Service Patch Failed", zap.Error(err))
				return err
			}
		}
	}
	return nil
}

// Finalize The Dispatcher Service
func (r *Reconciler) finalizeDispatcherService(ctx context.Context, channel *kafkav1beta1.KafkaChannel) error {

	// Get The Channel-Specific Logger Provided Via The Context
	logger := logging.FromContext(ctx).Desugar()

	// Attempt To Get The Dispatcher Service Associated With The Specified Channel
	service, err := r.getDispatcherService(channel)
	if service == nil || err != nil {

		// If The Service Was Not Found - Then Nothing To Do
		if errors.IsNotFound(err) {
			logger.Info("Dispatcher Service Not Found - Nothing To Finalize")
			return nil
		} else {
			logger.Error("Failed To Get Dispatcher Service For Finalization", zap.Error(err))
			return err
		}
	} else {

		// Clone The Service So As Not To Perturb Original
		service = service.DeepCopy()

		// Remove The Finalizer From The Dispatcher Service & Update
		util.RemoveFinalizer(r.finalizerName(), &service.ObjectMeta)
		service, err := r.kubeClientset.CoreV1().Services(service.Namespace).Update(ctx, service, metav1.UpdateOptions{})
		if err != nil {
			logger.Error("Failed To Remove Finalizer From Dispatcher Service", zap.Error(err))
			return err
		} else {
			logger.Info("Successfully Removed Finalizer From Dispatcher Service")
		}

		// Delete The Updated Dispatcher Service
		err = r.kubeClientset.CoreV1().Services(service.Namespace).Delete(ctx, service.Name, metav1.DeleteOptions{})
		if err != nil {
			logger.Error("Failed To Delete Dispatcher Service", zap.Error(err))
			return err
		} else {
			logger.Info("Successfully Deleted Dispatcher Service")
			return nil
		}
	}
}

// Get The Dispatcher Service Associated With The Specified Channel
func (r *Reconciler) getDispatcherService(channel *kafkav1beta1.KafkaChannel) (*corev1.Service, error) {

	// Get The Dispatcher Service Name
	serviceName := util.DispatcherDnsSafeName(channel)

	// Get The Service By Namespace / Name
	service, err := r.serviceLister.Services(r.environment.SystemNamespace).Get(serviceName)

	// Return The Results
	return service, err
}

// Create Dispatcher Service Model For The Specified Subscription
func (r *Reconciler) newDispatcherService(channel *kafkav1beta1.KafkaChannel) *corev1.Service {

	// Get The Dispatcher Service Name For The Channel
	serviceName := util.DispatcherDnsSafeName(channel)

	// Create & Return The Service Model
	return &corev1.Service{
		TypeMeta: metav1.TypeMeta{
			APIVersion: corev1.SchemeGroupVersion.String(),
			Kind:       constants.ServiceKind,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      serviceName,
			Namespace: r.environment.SystemNamespace,
			Labels: map[string]string{
				constants.KafkaChannelDispatcherLabel:   "true",                                  // Identifies the Service as being a KafkaChannel "Dispatcher"
				constants.KafkaChannelNameLabel:         channel.Name,                            // Identifies the Service's Owning KafkaChannel's Name
				constants.KafkaChannelNamespaceLabel:    channel.Namespace,                       // Identifies the Service's Owning KafkaChannel's Namespace
				constants.K8sAppDispatcherSelectorLabel: constants.K8sAppDispatcherSelectorValue, // Prometheus ServiceMonitor
			},
			// K8S Does NOT Support Cross-Namespace OwnerReferences
			// Instead Manage The Lifecycle Directly Via Finalizers (No K8S Garbage Collection)
			Finalizers: []string{r.finalizerName()},
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{
				{
					Name:       constants.MetricsPortName,
					Port:       int32(r.environment.MetricsPort),
					TargetPort: intstr.FromInt(r.environment.MetricsPort),
				},
			},
			Selector: map[string]string{
				constants.AppLabel: serviceName, // Matches Deployment Label Key/Value
			},
		},
	}
}

//
// Dispatcher Deployment
//

// Reconcile The Dispatcher Deployment
func (r *Reconciler) reconcileDispatcherDeployment(ctx context.Context, logger *zap.Logger, channel *kafkav1beta1.KafkaChannel) error {

	// Create A New Deployment For Comparison
	newDeployment, err := r.newDispatcherDeployment(logger, channel)
	if err != nil {
		logger.Error("Failed To Create Dispatcher Deployment YAML", zap.Error(err))
		channel.Status.MarkDispatcherFailed(event.DispatcherDeploymentReconciliationFailed.String(), "Failed To Generate Dispatcher Deployment: %v", err)
		return err
	}

	// Attempt To Get The Dispatcher Deployment Associated With The Specified Channel
	existingDeployment, err := r.getDispatcherDeployment(channel)
	if existingDeployment == nil || err != nil {

		// If The Dispatcher Deployment Was Not Found - Then Create A New Deployment For The Channel
		if errors.IsNotFound(err) {

			// Then Create The New Deployment
			logger.Info("Dispatcher Deployment Not Found - Creating New One")
			newDeployment, err = r.kubeClientset.AppsV1().Deployments(newDeployment.Namespace).Create(ctx, newDeployment, metav1.CreateOptions{})
			if err != nil {
				logger.Error("Failed To Create Dispatcher Deployment", zap.Error(err))
				channel.Status.MarkDispatcherFailed(event.DispatcherDeploymentReconciliationFailed.String(), "Failed To Create Dispatcher Deployment: %v", err)
				return err
			} else {
				logger.Info("Successfully Created Dispatcher Deployment")
				channel.Status.PropagateDispatcherStatus(&newDeployment.Status)
				return nil
			}
		} else {
			// Failed In Attempt To Get Deployment From K8S
			logger.Error("Failed To Get Dispatcher Deployment", zap.Error(err))
			channel.Status.MarkDispatcherUnknown(event.DispatcherDeploymentReconciliationFailed.String(), "Failed To Get Dispatcher Deployment: %v", err)
			return err
		}
	} else {

		// Log Deletion Timestamp & Finalizer State
		if existingDeployment.DeletionTimestamp.IsZero() {
			logger.Info("Successfully Verified Dispatcher Deployment")
		} else {
			if util.HasFinalizer(r.finalizerName(), &newDeployment.ObjectMeta) {
				logger.Info("Blocking Pending Deletion Of Dispatcher Deployment (Finalizer Detected)")
			} else {
				logger.Warn("Unable To Block Pending Deletion Of Dispatcher Deployment (Finalizer Missing)")
			}
		}

		// Determine whether the existing deployment is different in a way that demands an update
		// such as missing required labels, a different image, or certain container differences.
		// (This includes the configmap hash annotation, so configmap changes will trigger updates)
		updatedDeployment, needsUpdate := util.CheckDeploymentChanged(logger, existingDeployment, newDeployment)

		// Update the deployment in Kubernetes if necessary
		if needsUpdate {
			updatedDeployment, err = r.kubeClientset.AppsV1().Deployments(newDeployment.Namespace).Update(ctx, updatedDeployment, metav1.UpdateOptions{})
			if err == nil {
				controller.GetEventRecorder(ctx).Event(channel, corev1.EventTypeNormal, event.DispatcherDeploymentUpdated.String(), "Dispatcher Deployment Updated")
				logger.Info("Dispatcher Deployment Changed - Update Applied")
			} else {
				controller.GetEventRecorder(ctx).Event(channel, corev1.EventTypeWarning, event.DispatcherDeploymentUpdateFailed.String(), "Dispatcher Deployment Update Failed")
				channel.Status.MarkDispatcherFailed(event.DispatcherDeploymentUpdateFailed.String(), "Failed To Update Dispatcher Deployment: %v", err)
				logger.Info("Dispatcher Deployment Failed", zap.Error(err))
				return err
			}
		}
		// Propagate Status & Return Success
		channel.Status.PropagateDispatcherStatus(&updatedDeployment.Status)
		return nil
	}
}

// Finalize The Dispatcher Deployment
func (r *Reconciler) finalizeDispatcherDeployment(ctx context.Context, logger *zap.Logger, channel *kafkav1beta1.KafkaChannel) error {

	// Attempt To Get The Dispatcher Deployment Associated With The Specified Channel
	deployment, err := r.getDispatcherDeployment(channel)
	if deployment == nil || err != nil {

		// If The Deployment Was Not Found - Then Nothing To Do
		if errors.IsNotFound(err) {
			logger.Info("Dispatcher Deployment Not Found - Nothing To Finalize")
			return nil
		} else {
			logger.Error("Failed To Get Dispatcher Deployment For Finalization", zap.Error(err))
			return err
		}
	} else {

		// Clone The Deployment So As Not To Perturb Original
		deployment = deployment.DeepCopy()

		// Remove The Finalizer From The Dispatcher Deployment & Update
		util.RemoveFinalizer(r.finalizerName(), &deployment.ObjectMeta)
		deployment, err := r.kubeClientset.AppsV1().Deployments(deployment.Namespace).Update(ctx, deployment, metav1.UpdateOptions{})
		if err != nil {
			logger.Error("Failed To Remove Finalizer From Dispatcher Deployment", zap.Error(err))
			return err
		} else {
			logger.Info("Successfully Removed Finalizer From Dispatcher Deployment")
		}

		// Delete The Updated Dispatcher Deployment
		err = r.kubeClientset.AppsV1().Deployments(deployment.Namespace).Delete(ctx, deployment.Name, metav1.DeleteOptions{})
		if err != nil {
			logger.Error("Failed To Delete Dispatcher Deployment", zap.Error(err))
			return err
		} else {
			logger.Info("Successfully Deleted Dispatcher Deployment")
			return nil
		}
	}
}

// Get The Dispatcher Deployment Associated With The Specified Channel
func (r *Reconciler) getDispatcherDeployment(channel *kafkav1beta1.KafkaChannel) (*appsv1.Deployment, error) {

	// Get The Dispatcher Deployment Name For The Channel
	deploymentName := util.DispatcherDnsSafeName(channel)

	// Get The Dispatcher Deployment By Namespace / Name
	deployment, err := r.deploymentLister.Deployments(r.environment.SystemNamespace).Get(deploymentName)

	// Return The Results
	return deployment, err
}

// Create Dispatcher Deployment Model For The Specified Channel
func (r *Reconciler) newDispatcherDeployment(logger *zap.Logger, channel *kafkav1beta1.KafkaChannel) (*appsv1.Deployment, error) {

	// Get The Dispatcher Deployment Name For The Channel
	deploymentName := util.DispatcherDnsSafeName(channel)

	// Replicas Int Value For De-Referencing
	replicas := int32(r.config.Dispatcher.Replicas)

	// Create The Dispatcher Container Environment Variables
	envVars, err := r.dispatcherDeploymentEnvVars(channel)
	if err != nil {
		logger.Error("Failed To Create Dispatcher Deployment Environment Variables", zap.Error(err))
		return nil, err
	}

	// There is a difference between setting an entry in the limits or requests map to the zero-value
	// of a Quantity and not actually having that entry in the map at all.
	// If we want "no limit" or "no request" then the entry must not be present in the map.
	// Note: Since a "Quantity" type has no nil value, we use the Zero value to represent unlimited.
	resourceLimits := make(map[corev1.ResourceName]resource.Quantity)
	if !r.config.Dispatcher.MemoryLimit.IsZero() {
		resourceLimits[corev1.ResourceMemory] = r.config.Dispatcher.MemoryLimit
	}
	if !r.config.Dispatcher.CpuLimit.IsZero() {
		resourceLimits[corev1.ResourceCPU] = r.config.Dispatcher.CpuLimit
	}
	resourceRequests := make(map[corev1.ResourceName]resource.Quantity)
	if !r.config.Dispatcher.MemoryRequest.IsZero() {
		resourceRequests[corev1.ResourceMemory] = r.config.Dispatcher.MemoryRequest
	}
	if !r.config.Dispatcher.CpuRequest.IsZero() {
		resourceRequests[corev1.ResourceCPU] = r.config.Dispatcher.CpuRequest
	}

	// If either the limits or requests are an entirely-empty map, this will be translated to a nil
	// value in the resource itself, so pre-emptively set that here.  This prevents the CheckDeploymentChanged
	// function from returning a difference and restarting the dispatcher unnecessarily.
	if len(resourceLimits) == 0 {
		resourceLimits = nil
	}
	if len(resourceRequests) == 0 {
		resourceRequests = nil
	}

	// Create The Dispatcher's Deployment
	deployment := &appsv1.Deployment{
		TypeMeta: metav1.TypeMeta{
			APIVersion: appsv1.SchemeGroupVersion.String(),
			Kind:       constants.DeploymentKind,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      deploymentName,
			Namespace: r.environment.SystemNamespace,
			Labels: map[string]string{
				constants.AppLabel:                    deploymentName,    // Matches K8S Service Selector Key/Value Below
				constants.KafkaChannelDispatcherLabel: "true",            // Identifies the Deployment as being a KafkaChannel "Dispatcher"
				constants.KafkaChannelNameLabel:       channel.Name,      // Identifies the Deployment's Owning KafkaChannel's Name
				constants.KafkaChannelNamespaceLabel:  channel.Namespace, // Identifies the Deployment's Owning KafkaChannel's Namespace
			},
			// K8S Does NOT Support Cross-Namespace OwnerReferences
			// Instead Manage The Lifecycle Directly Via Finalizers (No K8S Garbage Collection)
			Finalizers: []string{r.finalizerName()},
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
							Image:           r.environment.DispatcherImage,
							Env:             envVars,
							ImagePullPolicy: corev1.PullIfNotPresent,
							Resources: corev1.ResourceRequirements{
								Limits:   resourceLimits,
								Requests: resourceRequests,
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

	// Return The Dispatcher's Deployment
	return deployment, nil
}

// Create The Dispatcher Container's Env Vars
func (r *Reconciler) dispatcherDeploymentEnvVars(channel *kafkav1beta1.KafkaChannel) ([]corev1.EnvVar, error) {

	// Get The TopicName For Specified Channel
	topicName := util.TopicName(channel)

	// Create The Dispatcher Deployment EnvVars
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
			Value: constants.DispatcherContainerName,
		},
		{
			Name:  commonenv.KnativeLoggingConfigMapNameEnvVarKey,
			Value: logging.ConfigMapName(),
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
			Name:  commonenv.ChannelKeyEnvVarKey,
			Value: util.ChannelKey(channel),
		},
		{
			Name:  commonenv.ServiceNameEnvVarKey,
			Value: util.DispatcherDnsSafeName(channel),
		},
		{
			Name:  commonenv.KafkaTopicEnvVarKey,
			Value: topicName,
		},
		{
			Name:  commonenv.ResyncPeriodMinutesEnvVarKey,
			Value: strconv.Itoa(int(r.environment.ResyncPeriod / time.Minute)),
		},
	}

	// If The Kafka Secret Env Var Is Specified Then Append Relevant Env Vars
	if len(r.kafkaSecret) <= 0 {

		// Received Invalid Kafka Secret - Cannot Proceed
		return nil, fmt.Errorf("invalid kafkaSecret for topic '%s'", topicName)

	} else {

		// Append The Secret Name As Env Var
		envVars = append(envVars, corev1.EnvVar{
			Name:  commonenv.KafkaSecretNameEnvVarKey,
			Value: r.kafkaSecret,
		})

		// Append The Secret Namespace As Env Var
		envVars = append(envVars, corev1.EnvVar{
			Name:  commonenv.KafkaSecretNamespaceEnvVarKey,
			Value: r.environment.SystemNamespace,
		})

	}

	// Return The Dispatcher Deployment EnvVars Array
	return envVars, nil
}

// Utility Function To Get The Finalizer Name For K8S Resources (Service, Deployment, etc.)
func (r *Reconciler) finalizerName() string {
	return util.KubernetesResourceFinalizerName(constants.KafkaChannelFinalizerSuffix)
}
