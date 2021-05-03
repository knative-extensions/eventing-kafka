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
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"go.uber.org/zap"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"knative.dev/pkg/apis/duck"
)

// CheckDeploymentChanged Modifies A Deployment With New Fields (If Necessary)
// Returns True If Any Modifications Were Made
func CheckDeploymentChanged(logger *zap.Logger, oldDeployment, newDeployment *appsv1.Deployment) (*appsv1.Deployment, bool) {

	// Make a copy of the old labels and annotations so we don't inadvertently
	// modify the old deployment fields directly
	updatedLabels := make(map[string]string)
	for oldKey, oldValue := range oldDeployment.ObjectMeta.Labels {
		updatedLabels[oldKey] = oldValue
	}
	updatedAnnotations := make(map[string]string)
	for oldKey, oldValue := range oldDeployment.Spec.Template.ObjectMeta.Annotations {
		updatedAnnotations[oldKey] = oldValue
	}

	metadataChanged := false
	// Add any labels in the "new" deployment to the copy of the labels from the old deployment.
	for newKey, newValue := range newDeployment.ObjectMeta.Labels {
		oldValue, ok := oldDeployment.ObjectMeta.Labels[newKey]
		if !ok || oldValue != newValue {
			metadataChanged = true
			updatedLabels[newKey] = newValue
		}
	}

	// Add any annotations in the "new" deployment to the copy of the labels from the old deployment.
	// (In particular this will trigger on differences in "kafka.eventing.knative.dev/configmap-hash")
	for newKey, newValue := range newDeployment.Spec.Template.ObjectMeta.Annotations {
		oldValue, ok := oldDeployment.Spec.Template.ObjectMeta.Annotations[newKey]
		if !ok || oldValue != newValue {
			metadataChanged = true
			updatedAnnotations[newKey] = newValue
		}
	}

	// Fields intentionally ignored:
	//    Spec.Replicas - Since a HorizontalPodAutoscaler explicitly changes this value on the deployment directly

	// Verify everything in the container spec aside from some particular exceptions (see "ignoreFields" below)
	oldContainerCount := len(oldDeployment.Spec.Template.Spec.Containers)
	if oldContainerCount == 0 {
		// This is unlikely but if it happens, replace the entire old deployment with a proper one
		logger.Warn("Old Deployment Has No Containers - Replacing Entire Deployment")
		return newDeployment, true
	}
	if len(newDeployment.Spec.Template.Spec.Containers) != 1 {
		logger.Error("New Deployment Has Incorrect Number Of Containers And Cannot Be Used")
		return oldDeployment, false
	}

	newContainer := &newDeployment.Spec.Template.Spec.Containers[0]
	oldContainer := findContainer(oldDeployment, newContainer.Name)
	if oldContainer == nil {
		logger.Error("Old Deployment Does Not Have Same Container Name - Replacing Entire Deployment")
		return newDeployment, true
	}

	ignoreFields := []cmp.Option{
		// Ignore the fields in a Container struct which are not set directly by the distributed channel reconcilers
		// and ones that are acceptable to be changed manually (such as the ImagePullPolicy)
		cmpopts.IgnoreFields(*newContainer,
			"Lifecycle",
			"TerminationMessagePolicy",
			"ImagePullPolicy",
			"SecurityContext",
			"StartupProbe",
			"TerminationMessagePath",
			"Stdin",
			"StdinOnce",
			"TTY"),
		// Ignore some other fields buried inside otherwise-relevant ones, mainly "defaults that come from empty strings,"
		// as there is no reason to restart the deployments for those changes.
		cmpopts.IgnoreFields(corev1.ContainerPort{}, "Protocol"),         // "" -> "TCP"
		cmpopts.IgnoreFields(corev1.ObjectFieldSelector{}, "APIVersion"), // "" -> "v1"
		cmpopts.IgnoreFields(corev1.HTTPGetAction{}, "Scheme"),           // "" -> "HTTP" (from inside the probes; always HTTP)
	}

	containersEqual := cmp.Equal(oldContainer, newContainer, ignoreFields...)
	if containersEqual && !metadataChanged {
		// Nothing of interest changed, so just keep the old deployment
		return oldDeployment, false
	}

	// Create an updated deployment from the old one, but using the new Container field
	updatedDeployment := oldDeployment.DeepCopy()
	if metadataChanged {
		updatedDeployment.ObjectMeta.Labels = updatedLabels
		updatedDeployment.Spec.Template.ObjectMeta.Annotations = updatedAnnotations
	}
	if !containersEqual {
		updatedDeployment.Spec.Template.Spec.Containers[0] = *newContainer
		updatedDeployment.Spec.Template.Spec.Volumes = newDeployment.Spec.Template.Spec.Volumes
	}
	return updatedDeployment, true
}

// findContainer returns the Container with the given name in a Deployment, or nil if not found
func findContainer(deployment *appsv1.Deployment, name string) *corev1.Container {
	for _, container := range deployment.Spec.Template.Spec.Containers {
		if container.Name == name {
			return &container
		}
	}
	return nil
}

// CheckServiceChanged Modifies A Service With New Fields (If Necessary)
// Returns True If Any Modifications Were Made
func CheckServiceChanged(logger *zap.Logger, oldService, newService *corev1.Service) ([]byte, bool) {

	// Make a copy of the old labels so we don't inadvertently modify the old service fields directly
	updatedLabels := make(map[string]string)
	for oldKey, oldValue := range oldService.ObjectMeta.Labels {
		updatedLabels[oldKey] = oldValue
	}

	// Add any labels in the "new" service to the copy of the labels from the old service.
	// Annotations could be similarly updated, but there are currently no annotations being made
	// in new services anyway so it would serve no practical purpose at the moment.
	labelsChanged := false
	for newKey, newValue := range newService.ObjectMeta.Labels {
		oldValue, ok := oldService.ObjectMeta.Labels[newKey]
		if !ok || oldValue != newValue {
			labelsChanged = true
			updatedLabels[newKey] = newValue
		}
	}

	ignoreFields := []cmp.Option{
		// Ignore the fields in a Spec struct which are not set directly by the distributed channel reconcilers
		cmpopts.IgnoreFields(oldService.Spec, "ClusterIP", "Type", "SessionAffinity"),
		// Ignore some other fields buried inside otherwise-relevant ones, mainly "defaults that come from empty strings,"
		// as there is no reason to restart the deployments for those changes.
		cmpopts.IgnoreFields(corev1.ServicePort{}, "Protocol"), // "" -> "TCP"
	}

	// Verify everything in the service spec aside from some particular exceptions (see "ignoreFields" above)
	specEqual := cmp.Equal(oldService.Spec, newService.Spec, ignoreFields...)
	if specEqual && !labelsChanged {
		// Nothing of interest changed, so just keep the old service
		return nil, false
	}

	// Create an updated service from the old one, but using the new Spec field
	updatedService := oldService.DeepCopy()
	if labelsChanged {
		updatedService.ObjectMeta.Labels = updatedLabels
	}
	if !specEqual {
		updatedService.Spec = newService.Spec
	}

	// Some fields are immutable and need to be guaranteed identical before being used for patching purposes
	updatedService.Spec.ClusterIP = oldService.Spec.ClusterIP

	return createJsonPatch(logger, oldService, updatedService)
}

// createJsonPatch generates a byte array patch suitable for a Kubernetes Patch operation
// Returns false if a patch is unnecessary or impossible for the given interfaces
func createJsonPatch(logger *zap.Logger, before interface{}, after interface{}) ([]byte, bool) {
	// Create the JSON patch
	jsonPatch, err := duck.CreatePatch(before, after)
	if err != nil {
		logger.Error("Could not create service patch", zap.Error(err))
		return nil, false
	}

	if len(jsonPatch) == 0 {
		// Nothing significant changed to patch
		return nil, false
	}
	patch, err := jsonPatch.MarshalJSON()
	if err != nil {
		logger.Error("Could not marshal service patch", zap.Error(err))
		return nil, false
	}
	return patch, true
}
