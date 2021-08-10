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

package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// Shared Test Data
var (
	keyValueString = "key1=value1,key2=value2,key3=value3"
	keyValueMap    = map[string]string{"key1": "value1", "key2": "value2", "key3": "value3"}
)

// Test The EKKubernetesConfig.DeploymentAnnotationsMap() Functionality
func TestEKKubernetesConfig_DeploymentAnnotationsMap(t *testing.T) {
	assert.Equal(t, keyValueMap, EKKubernetesConfig{DeploymentAnnotations: keyValueString}.DeploymentAnnotationsMap())
}

// Test The EKKubernetesConfig.DeploymentLabelMap() Functionality
func TestEKKubernetesConfig_DeploymentLabelMap(t *testing.T) {
	assert.Equal(t, keyValueMap, EKKubernetesConfig{DeploymentLabels: keyValueString}.DeploymentLabelsMap())
}

// Test The EKKubernetesConfig.PodAnnotationsMap() Functionality
func TestEKKubernetesConfig_PodAnnotationsMap(t *testing.T) {
	assert.Equal(t, keyValueMap, EKKubernetesConfig{PodAnnotations: keyValueString}.PodAnnotationsMap())
}

// Test The EKKubernetesConfig.PodLabelMap() Functionality
func TestEKKubernetesConfig_PodLabelMap(t *testing.T) {
	assert.Equal(t, keyValueMap, EKKubernetesConfig{PodLabels: keyValueString}.PodLabelsMap())
}

// Test The EKKubernetesConfig.ServiceAnnotationsMap() Functionality
func TestEKKubernetesConfig_ServiceAnnotationsMap(t *testing.T) {
	assert.Equal(t, keyValueMap, EKKubernetesConfig{ServiceAnnotations: keyValueString}.ServiceAnnotationsMap())
}

// Test The EKKubernetesConfig.ServiceLabelMap() Functionality
func TestEKKubernetesConfig_ServiceLabelMap(t *testing.T) {
	assert.Equal(t, keyValueMap, EKKubernetesConfig{ServiceLabels: keyValueString}.ServiceLabelsMap())
}
