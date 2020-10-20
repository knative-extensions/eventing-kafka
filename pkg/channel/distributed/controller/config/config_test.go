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

	channelReplicas      = 1
	channelMemoryRequest = "10Mi"
	channelCpuRquest     = "10m"
	channelMemoryLimit   = "20Mi"
	channelCpuLimit      = "100m"
)

// Define The TestCase Struct
type TestCase struct {
	name string

	// Config settings
	kafkaTopicDefaultNumPartitions     int32
	kafkaTopicDefaultReplicationFactor int16
	kafkaTopicDefaultRetentionMillis   int64
	kafkaAdminType                     string
	dispatcherCpuLimit                 resource.Quantity
	dispatcherCpuRequest               resource.Quantity
	dispatcherMemoryLimit              resource.Quantity
	dispatcherMemoryRequest            resource.Quantity
	dispatcherReplicas                 int
	channelCpuLimit                    resource.Quantity
	channelCpuRequest                  resource.Quantity
	channelMemoryLimit                 resource.Quantity
	channelMemoryRequest               resource.Quantity
	channelReplicas                    int

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
		channelCpuLimit:                    resource.MustParse(channelCpuLimit),
		channelCpuRequest:                  resource.MustParse(channelCpuRquest),
		channelMemoryLimit:                 resource.MustParse(channelMemoryLimit),
		channelMemoryRequest:               resource.MustParse(channelMemoryRequest),
		channelReplicas:                    channelReplicas,
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

	testCase = getValidTestCase("Invalid Config - Dispatcher.CpuLimit")
	testCase.dispatcherCpuLimit = resource.Quantity{}
	testCase.expectedError = ControllerConfigurationError("Dispatcher.CpuLimit must be nonzero")
	testCases = append(testCases, testCase)

	testCase = getValidTestCase("Invalid Config - Dispatcher.CpuRequest")
	testCase.dispatcherCpuRequest = resource.Quantity{}
	testCase.expectedError = ControllerConfigurationError("Dispatcher.CpuRequest must be nonzero")
	testCases = append(testCases, testCase)

	testCase = getValidTestCase("Invalid Config - Dispatcher.MemoryLimit")
	testCase.dispatcherMemoryLimit = resource.Quantity{}
	testCase.expectedError = ControllerConfigurationError("Dispatcher.MemoryLimit must be nonzero")
	testCases = append(testCases, testCase)

	testCase = getValidTestCase("Invalid Config - Dispatcher.MemoryRequest")
	testCase.dispatcherMemoryRequest = resource.Quantity{}
	testCase.expectedError = ControllerConfigurationError("Dispatcher.MemoryRequest must be nonzero")
	testCases = append(testCases, testCase)

	testCase = getValidTestCase("Invalid Config - Dispatcher.Replicas")
	testCase.dispatcherReplicas = -1
	testCase.expectedError = ControllerConfigurationError("Dispatcher.Replicas must be > 0")
	testCases = append(testCases, testCase)

	testCase = getValidTestCase("Invalid Config - Receiver.CpuLimit")
	testCase.channelCpuLimit = resource.Quantity{}
	testCase.expectedError = ControllerConfigurationError("Receiver.CpuLimit must be nonzero")
	testCases = append(testCases, testCase)

	testCase = getValidTestCase("Invalid Config - Receiver.CpuRequest")
	testCase.channelCpuRequest = resource.Quantity{}
	testCase.expectedError = ControllerConfigurationError("Receiver.CpuRequest must be nonzero")
	testCases = append(testCases, testCase)

	testCase = getValidTestCase("Invalid Config - Receiver.MemoryLimit")
	testCase.channelMemoryLimit = resource.Quantity{}
	testCase.expectedError = ControllerConfigurationError("Receiver.MemoryLimit must be nonzero")
	testCases = append(testCases, testCase)

	testCase = getValidTestCase("Invalid Config - Receiver.MemoryRequest")
	testCase.channelMemoryRequest = resource.Quantity{}
	testCase.expectedError = ControllerConfigurationError("Receiver.MemoryRequest must be nonzero")
	testCases = append(testCases, testCase)

	testCase = getValidTestCase("Invalid Config - Receiver.Replicas")
	testCase.channelReplicas = -1
	testCase.expectedError = ControllerConfigurationError("Receiver.Replicas must be > 0")
	testCases = append(testCases, testCase)

	testCase = getValidTestCase("Invalid Config - Kafka.Provider")
	testCase.kafkaAdminType = "invalidadmintype"
	testCase.expectedError = ControllerConfigurationError("Invalid / Unknown Kafka Admin Type: invalidadmintype")
	testCases = append(testCases, testCase)

	// Loop Over All The TestCases
	for _, testCase := range testCases {

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
		testConfig.Receiver.CpuLimit = testCase.channelCpuLimit
		testConfig.Receiver.CpuRequest = testCase.channelCpuRequest
		testConfig.Receiver.MemoryLimit = testCase.channelMemoryLimit
		testConfig.Receiver.MemoryRequest = testCase.channelMemoryRequest
		testConfig.Receiver.Replicas = testCase.channelReplicas

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
			assert.Equal(t, testCase.channelCpuLimit, testConfig.Receiver.CpuLimit)
			assert.Equal(t, testCase.channelCpuRequest, testConfig.Receiver.CpuRequest)
			assert.Equal(t, testCase.channelMemoryLimit, testConfig.Receiver.MemoryLimit)
			assert.Equal(t, testCase.channelMemoryRequest, testConfig.Receiver.MemoryRequest)
			assert.Equal(t, testCase.channelReplicas, testConfig.Receiver.Replicas)
		} else {
			assert.Equal(t, testCase.expectedError, err)
		}

	}
}
