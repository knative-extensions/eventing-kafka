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
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	kafkachannelv1beta1 "knative.dev/eventing-kafka/pkg/apis/messaging/v1beta1"
	commonenv "knative.dev/eventing-kafka/pkg/channel/distributed/common/env"
	commontesting "knative.dev/eventing-kafka/pkg/channel/distributed/common/testing"
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/constants"
	controllerenv "knative.dev/eventing-kafka/pkg/channel/distributed/controller/env"
	controllertesting "knative.dev/eventing-kafka/pkg/channel/distributed/controller/testing"
	fakeKafkaClient "knative.dev/eventing-kafka/pkg/client/injection/client/fake"
	_ "knative.dev/eventing-kafka/pkg/client/injection/informers/messaging/v1beta1/kafkachannel/fake" // Knative Fake Informer Injection
	"knative.dev/pkg/client/injection/kube/client/fake"
	_ "knative.dev/pkg/client/injection/kube/informers/apps/v1/deployment/fake" // Knative Fake Informer Injection
	_ "knative.dev/pkg/client/injection/kube/informers/core/v1/service/fake"    // Knative Fake Informer Injection
	"knative.dev/pkg/injection"
	"knative.dev/pkg/logging"
	logtesting "knative.dev/pkg/logging/testing"
)

// Test The NewController() Functionality
func TestNewController(t *testing.T) {

	// Populate Environment Variables For Testing
	populateEnvironmentVariables(t)

	// Create A Context With Test Logger
	logger := logtesting.TestLogger(t)
	ctx := logging.WithLogger(context.Background(), logger)

	// Register Fake Informers (See Injection "_" Imports Above!)
	ctx, fakeInformers := injection.Fake.SetupInformers(ctx, &rest.Config{})
	assert.NotNil(t, fakeInformers)

	// Add The Fake K8S Clientset To The Context (Populated With ConfigMap)
	configMap := commontesting.GetTestSaramaConfigMap(controllertesting.SaramaConfigYaml, controllertesting.ControllerConfigYaml)
	secret := controllertesting.NewKafkaSecret(controllertesting.WithKafkaSecretFinalizer)
	ctx, fakeClientset := fake.With(ctx, configMap, secret)
	assert.NotNil(t, fakeClientset)

	// Add The Fake Kafka Clientset To The Context (Empty)
	ctx, fakeKafkaClientset := fakeKafkaClient.With(ctx)
	assert.NotNil(t, fakeKafkaClientset)

	// Perform The Test (Create The KafkaChannel Controller)
	environment, err := controllerenv.GetEnvironment(logger.Desugar())
	assert.Nil(t, err)
	ctx = context.WithValue(ctx, controllerenv.Key{}, environment)
	controller := NewController(ctx, nil)

	// Verify The Results
	assert.NotNil(t, controller)
	assert.True(t, len(controller.Name) > 0)
	assert.NotNil(t, controller.Reconciler)
}

// Test The FilterKafkaChannelOwnerByReferenceOrLabel() Functionality
func TestFilterKafkaChannelOwnerByReferenceOrLabel(t *testing.T) {

	// Test Data
	kafkaChannelGVK := kafkachannelv1beta1.SchemeGroupVersion.WithKind(constants.KafkaChannelKind)
	serviceGVK := corev1.SchemeGroupVersion.WithKind(constants.ServiceKind)
	trueBool := true
	objName := "TestObjName"
	objNamespace := "TestObjNamespace"
	kafkaChannelName := "TestKafkaChannelName"
	kafkaChannelNamespace := "TestKafkaChannelNamespace"
	serviceName := "TestServiceName"

	// Define The TestCase Type
	type TestCase struct {
		only           bool
		name           string
		object         metav1.Object
		expectedResult bool
	}

	// Define The TestCases
	testCases := []TestCase{
		{
			name: "Valid OwnerReference",
			object: createMetaV1Object(metav1.ObjectMeta{
				Name:      objName,
				Namespace: objNamespace,
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion: kafkaChannelGVK.GroupVersion().String(),
						Kind:       kafkaChannelGVK.Kind,
						Name:       kafkaChannelName,
						Controller: &trueBool,
					},
				},
			}),
			expectedResult: true,
		},
		{
			name: "Invalid OwnerReference",
			object: createMetaV1Object(metav1.ObjectMeta{
				Name:      objName,
				Namespace: objNamespace,
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion: serviceGVK.GroupVersion().String(),
						Kind:       serviceGVK.Kind,
						Name:       serviceName,
						Controller: &trueBool,
					},
				},
			}),
			expectedResult: false,
		},
		{
			name: "Valid Labels",
			object: createMetaV1Object(metav1.ObjectMeta{
				Name:      objName,
				Namespace: objNamespace,
				Labels: map[string]string{
					constants.KafkaChannelNameLabel:      kafkaChannelName,
					constants.KafkaChannelNamespaceLabel: kafkaChannelNamespace,
				},
			}),
			expectedResult: true,
		},
		{
			name: "No OwnerReference Or Labels",
			object: createMetaV1Object(metav1.ObjectMeta{
				Name:      objName,
				Namespace: objNamespace,
			}),
			expectedResult: false,
		},
		{
			name:           "Non MetaV1 Object",
			object:         nil,
			expectedResult: false,
		},
	}

	// Filter To Those With "only" Flag (If Any Specified)
	filteredTestCases := make([]TestCase, 0)
	for _, testCase := range testCases {
		if testCase.only {
			filteredTestCases = append(filteredTestCases, testCase)
		}
	}
	if len(filteredTestCases) == 0 {
		filteredTestCases = testCases
	}

	// Execute The Individual Test Cases
	for _, testCase := range filteredTestCases {
		t.Run(testCase.name, func(t *testing.T) {

			// Get The Actual Filter Function
			filterFunc := FilterKafkaChannelOwnerByReferenceOrLabel()
			assert.NotNil(t, filterFunc)

			// Perform The Test
			actualResult := filterFunc(testCase.object)
			assert.Equal(t, testCase.expectedResult, actualResult)
		})
	}
}

// Test The Shutdown() Functionality
func TestShutdown(t *testing.T) {

	// Create A Mock AdminClient To Test Closing
	mockAdminClient := &controllertesting.MockAdminClient{}

	// Set The Package Level The Reconciler To Test Against
	rec = &Reconciler{adminClient: mockAdminClient}

	// Perform The Test
	Shutdown()

	// Verify The Results
	assert.True(t, mockAdminClient.CloseCalled())
}

// Utility Function For Populating Required Environment Variables For Testing
func populateEnvironmentVariables(t *testing.T) {
	commontesting.SetTestEnvironment(t)
	assert.Nil(t, os.Setenv(commonenv.ServiceAccountEnvVarKey, controllertesting.ServiceAccount))
	assert.Nil(t, os.Setenv(commonenv.MetricsDomainEnvVarKey, controllertesting.MetricsDomain))
	assert.Nil(t, os.Setenv(commonenv.MetricsPortEnvVarKey, strconv.Itoa(controllertesting.MetricsPort)))
	assert.Nil(t, os.Setenv(controllerenv.DispatcherImageEnvVarKey, controllertesting.DispatcherImage))
	assert.Nil(t, os.Setenv(controllerenv.ReceiverImageEnvVarKey, controllertesting.ReceiverImage))
	assert.Nil(t, os.Setenv(commonenv.ResyncPeriodMinutesEnvVarKey, strconv.Itoa(int(controllertesting.ResyncPeriod/time.Minute))))
}

// Utility Function For Creating A K8S Service With Specified ObjectMeta
func createMetaV1Object(objectMeta metav1.ObjectMeta) metav1.Object {
	service := corev1.Service{ObjectMeta: objectMeta}
	return service.GetObjectMeta()
}
