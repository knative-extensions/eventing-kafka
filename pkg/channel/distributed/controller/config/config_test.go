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

package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/api/resource"
	"knative.dev/eventing-kafka/pkg/channel/distributed/common/config"
)

// Test Constants
const (
	kafkaAdminType = "custom"

	defaultNumPartitions     = 7
	defaultReplicationFactor = 2
	defaultRetentionMillis   = 13579

	dispatcherReplicas      = 1
	dispatcherMemoryRequest = "20Mi"
	dispatcherCpuRequest    = "100m"
	dispatcherMemoryLimit   = "50Mi"
	dispatcherCpuLimit      = "300m"

	receiverReplicas      = 1
	receiverMemoryRequest = "10Mi"
	receiverCpuRquest     = "10m"
	receiverMemoryLimit   = "20Mi"
	receiverCpuLimit      = "100m"
)

// Define The TestCase Struct
type TestCase struct {
	name string

	// Config settings
	kafkaTopicDefaultNumPartitions     int32
	kafkaTopicDefaultReplicationFactor int16
	kafkaTopicDefaultRetentionMillis int64
	kafkaAdminType          string
	dispatcherCpuLimit      resource.Quantity
	dispatcherCpuRequest    resource.Quantity
	dispatcherMemoryLimit   resource.Quantity
	dispatcherMemoryRequest resource.Quantity
	dispatcherReplicas      int
	receiverCpuLimit        resource.Quantity
	receiverCpuRequest      resource.Quantity
	receiverMemoryLimit     resource.Quantity
	receiverMemoryRequest   resource.Quantity
	receiverReplicas        int

	expectedError error
}

// Get The Base / Valid Test Case - All Config Specified / No Errors
func getValidTestCase(name string) TestCase {
	return TestCase{
		name:                               name,
		kafkaTopicDefaultNumPartitions:     defaultNumPartitions,
		kafkaTopicDefaultReplicationFactor: defaultReplicationFactor,
		kafkaTopicDefaultRetentionMillis:   defaultRetentionMillis,
		kafkaAdminType:                     kafkaAdminType,
		dispatcherCpuLimit:                 resource.MustParse(dispatcherCpuLimit),
		dispatcherCpuRequest:               resource.MustParse(dispatcherCpuRequest),
		dispatcherMemoryLimit:              resource.MustParse(dispatcherMemoryLimit),
		dispatcherMemoryRequest:            resource.MustParse(dispatcherMemoryRequest),
		dispatcherReplicas:                 dispatcherReplicas,
		receiverCpuLimit:                   resource.MustParse(receiverCpuLimit),
		receiverCpuRequest:                 resource.MustParse(receiverCpuRquest),
		receiverMemoryLimit:                resource.MustParse(receiverMemoryLimit),
		receiverMemoryRequest:              resource.MustParse(receiverMemoryRequest),
		receiverReplicas:                   receiverReplicas,
		expectedError:                      nil,
	}
}

// Test All Permutations Of The VerifyConfiguration Functionality
func TestVerifyConfiguration(t *testing.T) {

	// Define The TestCases
	testCases := make([]TestCase, 0, 7)

	testCase := getValidTestCase("Valid Complete Config")
	testCases = append(testCases, testCase)

	testCase = getValidTestCase("Invalid Config - Kafka.Topic.DefaultNumPartitions")
	testCase.kafkaTopicDefaultNumPartitions = -1
	testCase.expectedError = ControllerConfigurationError("Kafka.Topic.DefaultNumPartitions must be > 0")
	testCases = append(testCases, testCase)

	testCase = getValidTestCase("Invalid Config - Kafka.Topic.DefaultReplicationFactor")
	testCase.kafkaTopicDefaultReplicationFactor = -1
	testCase.expectedError = ControllerConfigurationError("Kafka.Topic.DefaultReplicationFactor must be > 0")
	testCases = append(testCases, testCase)

	testCase = getValidTestCase("Invalid Config - Kafka.Topic.DefaultRetentionMillis")
	testCase.kafkaTopicDefaultRetentionMillis = -1
	testCase.expectedError = ControllerConfigurationError("Kafka.Topic.DefaultRetentionMillis must be > 0")
	testCases = append(testCases, testCase)

	testCase = getValidTestCase("Valid Config - Dispatcher.CpuLimit = Zero (unlimited)")
	testCase.dispatcherCpuLimit = resource.Quantity{}
	testCases = append(testCases, testCase)

	testCase = getValidTestCase("Valid Config - Dispatcher.MemoryLimit = Zero (unlimited)")
	testCase.dispatcherMemoryLimit = resource.Quantity{}
	testCases = append(testCases, testCase)

	testCase = getValidTestCase("Invalid Config - Dispatcher.Replicas")
	testCase.dispatcherReplicas = -1
	testCase.expectedError = ControllerConfigurationError("Dispatcher.Replicas must be > 0")
	testCases = append(testCases, testCase)

	testCase = getValidTestCase("Valid Config - Receiver.CpuLimit = Zero (unlimited)")
	testCase.receiverCpuLimit = resource.Quantity{}
	testCases = append(testCases, testCase)

	testCase = getValidTestCase("Valid Config - Receiver.MemoryLimit = Zero (unlimited)")
	testCase.receiverMemoryLimit = resource.Quantity{}
	testCases = append(testCases, testCase)

	testCase = getValidTestCase("Invalid Config - Receiver.Replicas")
	testCase.receiverReplicas = -1
	testCase.expectedError = ControllerConfigurationError("Receiver.Replicas must be > 0")
	testCases = append(testCases, testCase)

	testCase = getValidTestCase("Invalid Config - Kafka.Provider")
	testCase.kafkaAdminType = "invalidadmintype"
	testCase.expectedError = ControllerConfigurationError("Invalid / Unknown Kafka Admin Type: invalidadmintype")
	testCases = append(testCases, testCase)

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			testConfig := &config.EventingKafkaConfig{}
			testConfig.Kafka.Topic.DefaultNumPartitions = testCase.kafkaTopicDefaultNumPartitions
			testConfig.Kafka.Topic.DefaultReplicationFactor = testCase.kafkaTopicDefaultReplicationFactor
			testConfig.Kafka.Topic.DefaultRetentionMillis = testCase.kafkaTopicDefaultRetentionMillis
			testConfig.Kafka.AdminType = testCase.kafkaAdminType
			testConfig.Dispatcher.CpuLimit = testCase.dispatcherCpuLimit
			testConfig.Dispatcher.CpuRequest = testCase.dispatcherCpuRequest
			testConfig.Dispatcher.MemoryLimit = testCase.dispatcherMemoryLimit
			testConfig.Dispatcher.MemoryRequest = testCase.dispatcherMemoryRequest
			testConfig.Dispatcher.Replicas = testCase.dispatcherReplicas
			testConfig.Receiver.CpuLimit = testCase.receiverCpuLimit
			testConfig.Receiver.CpuRequest = testCase.receiverCpuRequest
			testConfig.Receiver.MemoryLimit = testCase.receiverMemoryLimit
			testConfig.Receiver.MemoryRequest = testCase.receiverMemoryRequest
			testConfig.Receiver.Replicas = testCase.receiverReplicas

			// Perform The Test
			err := VerifyConfiguration(testConfig)

			// Verify The Results
			if testCase.expectedError == nil {
				assert.Nil(t, err)
				assert.Equal(t, testCase.kafkaTopicDefaultNumPartitions, testConfig.Kafka.Topic.DefaultNumPartitions)
				assert.Equal(t, testCase.kafkaTopicDefaultReplicationFactor, testConfig.Kafka.Topic.DefaultReplicationFactor)
				assert.Equal(t, testCase.kafkaTopicDefaultRetentionMillis, testConfig.Kafka.Topic.DefaultRetentionMillis)
				assert.Equal(t, testCase.kafkaAdminType, testConfig.Kafka.AdminType)
				assert.Equal(t, testCase.dispatcherCpuLimit, testConfig.Dispatcher.CpuLimit)
				assert.Equal(t, testCase.dispatcherCpuRequest, testConfig.Dispatcher.CpuRequest)
				assert.Equal(t, testCase.dispatcherMemoryLimit, testConfig.Dispatcher.MemoryLimit)
				assert.Equal(t, testCase.dispatcherMemoryRequest, testConfig.Dispatcher.MemoryRequest)
				assert.Equal(t, testCase.dispatcherReplicas, testConfig.Dispatcher.Replicas)
				assert.Equal(t, testCase.receiverCpuLimit, testConfig.Receiver.CpuLimit)
				assert.Equal(t, testCase.receiverCpuRequest, testConfig.Receiver.CpuRequest)
				assert.Equal(t, testCase.receiverMemoryLimit, testConfig.Receiver.MemoryLimit)
				assert.Equal(t, testCase.receiverMemoryRequest, testConfig.Receiver.MemoryRequest)
				assert.Equal(t, testCase.receiverReplicas, testConfig.Receiver.Replicas)
			} else {
				assert.Equal(t, testCase.expectedError, err)
			}
		})
	}
}
