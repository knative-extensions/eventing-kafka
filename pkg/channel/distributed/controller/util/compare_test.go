/*
Copyright 2021 The Knative Authors

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

package util

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/constants"
	logtesting "knative.dev/pkg/logging/testing"
)

type deploymentOption func(service *appsv1.Deployment)
type serviceOption func(*corev1.Service)

// Tests the CheckDeploymentChanged functionality.  Note that this is also tested fairly extensively
// as part of the various reconciler tests, and as such the deployment structs used here are somewhat trivial.
func TestCheckDeploymentChanged(t *testing.T) {
	logger := logtesting.TestLogger(t).Desugar()
	tests := []struct {
		name               string
		existingDeployment *appsv1.Deployment
		newDeployment      *appsv1.Deployment
		expectUpdated      bool
	}{
		{
			name:               "Different Image",
			existingDeployment: getBasicDeployment(),
			newDeployment:      getBasicDeployment(withDifferentImage),
			expectUpdated:      true,
		},
		{
			name:               "Missing Required Label",
			existingDeployment: getBasicDeployment(),
			newDeployment:      getBasicDeployment(withLabel),
			expectUpdated:      true,
		},
		{
			name:               "Missing Required Annotation",
			existingDeployment: getBasicDeployment(),
			newDeployment:      getBasicDeployment(withAnnotation),
			expectUpdated:      true,
		},
		{
			name:               "Missing Existing Container",
			existingDeployment: getBasicDeployment(withoutContainer),
			newDeployment:      getBasicDeployment(),
			expectUpdated:      true,
		},
		{
			name:               "Extra Existing Label",
			existingDeployment: getBasicDeployment(withLabel),
			newDeployment:      getBasicDeployment(),
		},
		{
			name:               "Extra Existing Annotation",
			existingDeployment: getBasicDeployment(withAnnotation),
			newDeployment:      getBasicDeployment(),
		},
		{
			name:               "Missing New Container",
			existingDeployment: getBasicDeployment(),
			newDeployment:      getBasicDeployment(withoutContainer),
		},
		{
			name:               "Multiple Existing Containers",
			existingDeployment: getBasicDeployment(withExtraContainer),
			newDeployment:      getBasicDeployment(),
		},
		{
			name:               "Multiple Existing Containers, Incorrect First",
			existingDeployment: getBasicDeployment(withExtraContainerFirst),
			newDeployment:      getBasicDeployment(),
		},
		{
			name:               "Multiple Existing Containers, Missing Required Annotation",
			existingDeployment: getBasicDeployment(withExtraContainer),
			newDeployment:      getBasicDeployment(withAnnotation),
			expectUpdated:      true,
		},
		{
			name:               "Multiple Existing Containers, Incorrect First, Missing Required Annotation",
			existingDeployment: getBasicDeployment(withExtraContainerFirst),
			newDeployment:      getBasicDeployment(withAnnotation),
			expectUpdated:      true,
		},
		{
			name:               "Container With Incorrect Name",
			existingDeployment: getBasicDeployment(withDifferentContainer),
			newDeployment:      getBasicDeployment(),
			expectUpdated:      true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			newDeployment := tt.newDeployment
			if newDeployment == nil {
				newDeployment = getBasicDeployment()
			}
			updatedDeployment, isUpdated := CheckDeploymentChanged(logger, tt.existingDeployment, newDeployment)
			assert.NotNil(t, updatedDeployment)
			assert.Equal(t, tt.expectUpdated, isUpdated)
		})
	}
}

// Tests the CheckServiceChanged functionality.  Note that this is also tested fairly extensively
// as part of the various reconciler tests, and as such the service structs used here are somewhat trivial.
func TestCheckServiceChanged(t *testing.T) {
	logger := logtesting.TestLogger(t).Desugar()
	tests := []struct {
		name            string
		existingService *corev1.Service
		newService      *corev1.Service
		expectPatch     bool
		expectUpdated   bool
	}{
		{
			name:            "Different Cluster IP",
			existingService: getBasicService(),
			newService:      getBasicService(withDifferentClusterIP),
		},
		{
			name:            "Missing Required Label",
			existingService: getBasicService(),
			newService:      getBasicService(withServiceLabel),
			expectUpdated:   true,
			expectPatch:     true,
		},
		{
			name:            "Missing Ports",
			existingService: getBasicService(withoutPorts),
			newService:      getBasicService(),
			expectUpdated:   true,
			expectPatch:     true,
		},
		{
			name:            "Extra Existing Label",
			existingService: getBasicService(withServiceLabel),
			newService:      getBasicService(),
		},
		{
			name:            "Empty Services",
			existingService: &corev1.Service{},
			newService:      &corev1.Service{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			newService := tt.newService
			if newService == nil {
				newService = getBasicService()
			}
			patch, isUpdated := CheckServiceChanged(logger, tt.existingService, newService)
			assert.Equal(t, tt.expectPatch, patch != nil)
			assert.Equal(t, tt.expectUpdated, isUpdated)
		})
	}
}

func TestCreateJsonPatch(t *testing.T) {
	logger := logtesting.TestLogger(t).Desugar()
	tests := []struct {
		name        string
		before      interface{}
		after       interface{}
		expectPatch bool
		expectOk    bool
	}{
		{
			name:   "Invalid Content",
			before: math.Inf(1),
			after:  math.Inf(1),
		},
		{
			name:   "No Difference",
			before: getBasicService(),
			after:  getBasicService(),
		},
		{
			name:        "Missing Ports",
			before:      getBasicService(withoutPorts),
			after:       getBasicService(),
			expectPatch: true,
			expectOk:    true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patch, ok := createJsonPatch(logger, tt.before, tt.after)
			assert.Equal(t, tt.expectPatch, patch != nil)
			assert.Equal(t, tt.expectOk, ok)
		})
	}
}

func getBasicDeployment(options ...deploymentOption) *appsv1.Deployment {
	deployment := &appsv1.Deployment{
		TypeMeta: metav1.TypeMeta{
			APIVersion: appsv1.SchemeGroupVersion.String(),
			Kind:       constants.DeploymentKind,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "TestDeployment",
			Namespace: "TestNamespace",
			Labels:    make(map[string]string),
		},
		Spec: appsv1.DeploymentSpec{
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: make(map[string]string),
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{Name: "TestContainerName"},
					},
				},
			},
		},
	}

	// Apply any desired customizations
	for _, option := range options {
		option(deployment)
	}

	return deployment
}

func withLabel(deployment *appsv1.Deployment) {
	deployment.Labels["TestLabelName"] = "TestLabelValue"
}

func withAnnotation(deployment *appsv1.Deployment) {
	deployment.Spec.Template.ObjectMeta.Annotations["TestAnnotationName"] = "TestAnnotationValue"
}

func withoutContainer(deployment *appsv1.Deployment) {
	deployment.Spec.Template.Spec.Containers = []corev1.Container{}
}

func withExtraContainer(deployment *appsv1.Deployment) {
	deployment.Spec.Template.Spec.Containers = append(
		deployment.Spec.Template.Spec.Containers, corev1.Container{
			Name: "TestExtraContainerName",
		})
}

func withExtraContainerFirst(deployment *appsv1.Deployment) {
	deployment.Spec.Template.Spec.Containers = append([]corev1.Container{{
		Name: "TestExtraContainerName",
	}},
		deployment.Spec.Template.Spec.Containers...)
}

func withDifferentContainer(deployment *appsv1.Deployment) {
	deployment.Spec.Template.Spec.Containers[0].Name = "TestDifferentContainerName"
}

func withDifferentImage(deployment *appsv1.Deployment) {
	deployment.Spec.Template.Spec.Containers[0].Image = "TestNewImage"
}

func getBasicService(options ...serviceOption) *corev1.Service {
	service := &corev1.Service{
		TypeMeta: metav1.TypeMeta{
			APIVersion: corev1.SchemeGroupVersion.String(),
			Kind:       constants.ServiceKind,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "TestService",
			Namespace: "TestNamespace",
			Labels:    make(map[string]string),
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{{
				Name: "TestServicePort",
			}},
		},
	}

	// Apply any desired customizations
	for _, option := range options {
		option(service)
	}

	return service
}

func withServiceLabel(service *corev1.Service) {
	service.Labels["TestLabelName"] = "TestLabelValue"
}

func withDifferentClusterIP(service *corev1.Service) {
	service.Spec.ClusterIP = "DifferentClusterIP"
}

func withoutPorts(service *corev1.Service) {
	service.Spec.Ports = nil
}
