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

package resources

import (
	v1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"knative.dev/pkg/system"

	"knative.dev/eventing-kafka/pkg/common/constants"
	commonconstants "knative.dev/eventing-kafka/pkg/common/constants"
)

const (
	DispatcherContainerName = "dispatcher"
)

var (
	dispatcherName   = "kafka-ch-dispatcher"
	dispatcherLabels = map[string]string{
		"messaging.knative.dev/channel": "kafka-channel",
		"messaging.knative.dev/role":    "dispatcher",
	}
)

type DispatcherBuilder struct {
	deployment *v1.Deployment
	args       *DispatcherArgs
}

type DispatcherArgs struct {
	DispatcherScope     string
	DispatcherNamespace string
	Image               string
	Replicas            int32
	ServiceAccount      string
	ConfigMapHash       string
	OwnerRef            metav1.OwnerReference
}

// NewDispatcherBuilder returns a builder which builds from scratch a dispatcher deployment.
// Intended to be used when creating the dispatcher deployment for the first time.
func NewDispatcherBuilder() *DispatcherBuilder {
	b := &DispatcherBuilder{}
	b.deployment = dispatcherTemplate()
	return b
}

// NewDispatcherBuilderFromDeployment returns a builder which builds a dispatcher deployment from the given deployment.
// Intended to be used when updating an existing dispatcher deployment.
func NewDispatcherBuilderFromDeployment(d *v1.Deployment) *DispatcherBuilder {
	b := &DispatcherBuilder{}
	b.deployment = d
	return b
}

func (b *DispatcherBuilder) WithArgs(args *DispatcherArgs) *DispatcherBuilder {
	b.args = args
	return b
}

func (b *DispatcherBuilder) Build() *v1.Deployment {
	replicas := b.args.Replicas
	b.deployment.ObjectMeta.Namespace = b.args.DispatcherNamespace
	b.deployment.ObjectMeta.OwnerReferences = []metav1.OwnerReference{b.args.OwnerRef}
	b.deployment.Spec.Replicas = &replicas
	b.deployment.Spec.Template.ObjectMeta.Annotations = map[string]string{
		commonconstants.ConfigMapHashAnnotationKey: b.args.ConfigMapHash,
	}
	b.deployment.Spec.Template.Spec.ServiceAccountName = b.args.ServiceAccount

	for i, c := range b.deployment.Spec.Template.Spec.Containers {
		if c.Name == DispatcherContainerName {
			container := &b.deployment.Spec.Template.Spec.Containers[i]
			container.Image = b.args.Image
			if container.Env == nil {
				container.Env = makeEnv(b.args)
			}
		}
	}
	return b.deployment
}

func dispatcherTemplate() *v1.Deployment {

	return &v1.Deployment{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "apps/v1",
			Kind:       "Deployments",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: dispatcherName,
		},
		Spec: v1.DeploymentSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: dispatcherLabels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: dispatcherLabels,
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name: DispatcherContainerName,
							Ports: []corev1.ContainerPort{{
								Name:          "metrics",
								ContainerPort: 9090,
							}},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      constants.SettingsConfigMapName,
									MountPath: constants.SettingsConfigMapMountPath,
								},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: constants.SettingsConfigMapName,
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{
										Name: constants.SettingsConfigMapName,
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

func makeEnv(args *DispatcherArgs) []corev1.EnvVar {
	vars := []corev1.EnvVar{{
		Name:  system.NamespaceEnvKey,
		Value: system.Namespace(),
	}, {
		Name:  "METRICS_DOMAIN",
		Value: "knative.dev/eventing",
	}, {
		Name:  "CONFIG_LOGGING_NAME",
		Value: "config-logging",
	}, {
		Name:  "CONFIG_LEADERELECTION_NAME",
		Value: "config-leader-election",
	}}

	if args.DispatcherScope == "namespace" {
		vars = append(vars, corev1.EnvVar{
			Name: "NAMESPACE",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{
					FieldPath: "metadata.namespace",
				},
			},
		})
	}

	vars = append(vars, corev1.EnvVar{
		Name: "POD_NAME",
		ValueFrom: &corev1.EnvVarSource{
			FieldRef: &corev1.ObjectFieldSelector{
				APIVersion: "",
				FieldPath:  "metadata.name",
			},
		},
	}, corev1.EnvVar{
		Name:  "CONTAINER_NAME",
		Value: "dispatcher",
	})
	return vars
}
