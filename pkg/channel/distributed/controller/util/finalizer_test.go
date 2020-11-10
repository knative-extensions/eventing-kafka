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

package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/constants"
)

// Test The KubernetesResourceFinalizerName() Functionality
func TestKubernetesResourceFinalizerName(t *testing.T) {
	const suffix = "TestSuffix"
	result := KubernetesResourceFinalizerName(suffix)
	assert.Equal(t, constants.EventingKafkaFinalizerPrefix+suffix, result)
}

// Test The HasFinalizer() Functionality
func TestHasFinalizer(t *testing.T) {

	// Test Data
	finalizer1 := "TestFinalizer1"
	finalizer2 := "TestFinalizer2"
	finalizer3 := "TestFinalizer3"
	objectMeta := &metav1.ObjectMeta{
		Finalizers: []string{finalizer1, finalizer2},
	}

	// Perform The Test
	assert.False(t, HasFinalizer(finalizer1, nil))
	assert.False(t, HasFinalizer(finalizer1, &metav1.ObjectMeta{}))
	assert.True(t, HasFinalizer(finalizer1, objectMeta))
	assert.True(t, HasFinalizer(finalizer2, objectMeta))
	assert.False(t, HasFinalizer(finalizer3, objectMeta))
}

// Test The RemoveFinalizer() Functionality
func TestRemoveFinalizer(t *testing.T) {

	// Test Data
	finalizer1 := "TestFinalizer1"
	finalizer2 := "TestFinalizer2"
	finalizer3 := "TestFinalizer3"
	objectMeta := &metav1.ObjectMeta{
		Finalizers: []string{finalizer1, finalizer2, finalizer3},
	}

	// Perform The Test
	RemoveFinalizer(finalizer2, objectMeta)

	// Verify The Results
	assert.Equal(t, 2, len(objectMeta.Finalizers))
	assert.Equal(t, finalizer1, objectMeta.Finalizers[0])
	assert.Equal(t, finalizer3, objectMeta.Finalizers[1])
}
