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

package k8s

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	commontesting "knative.dev/eventing-kafka/pkg/channel/distributed/common/testing"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/system"
)

// Test The LoggingContext() Functionality
func TestLoggingContext(t *testing.T) {

	// Test Data
	ctx := context.TODO()
	component := "TestComponent"

	// Setup Environment
	commontesting.SetTestEnvironment(t)

	// Create A Test Logging ConfigMap
	loggingConfigMap := &corev1.ConfigMap{
		TypeMeta: v1.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: corev1.SchemeGroupVersion.String(),
		},
		ObjectMeta: v1.ObjectMeta{
			Name:      logging.ConfigMapName(),
			Namespace: system.Namespace(),
		},
	}

	// Create The Fake K8S Client
	fakeK8sClient := fake.NewSimpleClientset(loggingConfigMap)

	ctx = context.WithValue(ctx, kubeclient.Key{}, fakeK8sClient)

	// Perform The Test (Initialize The Logging Context)
	resultContext := LoggingContext(ctx, component, fakeK8sClient)

	// Verify The Results
	assert.NotNil(t, resultContext)
	assert.Equal(t, fakeK8sClient, kubeclient.Get(resultContext))
	assert.NotNil(t, logging.FromContext(resultContext))

	// Log Something And Wait (Visual Test ; )
	logging.FromContext(ctx).Info("Test Logger")
	time.Sleep(1 * time.Second)
}
