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

package env

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"testing"
	"time"

	"knative.dev/pkg/system"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"knative.dev/eventing-kafka/pkg/channel/distributed/common/env"
)

// Test Constants
const (
	systemNamespace     = "test-system-namespace"
	metricsPort         = "9999"
	metricsDomain       = "kafka-eventing"
	healthPort          = "1234"
	resyncPeriodMinutes = "3600"
	kafkaBrokers        = "TestKafkaBrokers"
	serviceName         = "TestServiceName"
	kafkaUsername       = "TestKafkaUsername"
	kafkaPassword       = "TestKafkaPassword"
	podName             = "TestPod"
	containerName       = "TestContainer"
)

// Define The TestCase Struct
type TestCase struct {
	name                 string
	systemNamespace      string
	metricsPort          string
	metricsDomain        string
	healthPort           string
	resyncPeriodMinutes  string
	kafkaBrokers         string
	serviceName          string
	kafkaUsername        string
	kafkaPassword        string
	podName              string
	containerName        string
	expectedError        error
	expectedResyncPeriod string
}

// Test All Permutations Of The GetEnvironment() Functionality
func TestGetEnvironment(t *testing.T) {

	// Get A Logger Reference For Testing
	logger := getLogger()

	// Define The TestCases
	testCases := make([]TestCase, 0, 20)
	testCase := getValidTestCase("Valid Complete Config")
	testCases = append(testCases, testCase)

	testCase = getValidTestCase("Missing Required Config - SystemNamespace")
	testCase.systemNamespace = ""
	testCase.expectedError = getMissingRequiredEnvironmentVariableError(system.NamespaceEnvKey)
	testCases = append(testCases, testCase)

	testCase = getValidTestCase("Missing Required Config - MetricsDomain")
	testCase.metricsDomain = ""
	testCase.expectedError = getMissingRequiredEnvironmentVariableError(env.MetricsDomainEnvVarKey)
	testCases = append(testCases, testCase)

	testCase = getValidTestCase("Missing Required Config - MetricsPort")
	testCase.metricsPort = ""
	testCase.expectedError = getMissingRequiredEnvironmentVariableError(env.MetricsPortEnvVarKey)
	testCases = append(testCases, testCase)

	testCase = getValidTestCase("Invalid Config - MetricsPort")
	testCase.metricsPort = "NAN"
	testCase.expectedError = getInvalidIntegerEnvironmentVariableError(testCase.metricsPort, env.MetricsPortEnvVarKey)
	testCases = append(testCases, testCase)

	testCase = getValidTestCase("Missing Required Config - HealthPort")
	testCase.healthPort = ""
	testCase.expectedError = getMissingRequiredEnvironmentVariableError(env.HealthPortEnvVarKey)
	testCases = append(testCases, testCase)

	testCase = getValidTestCase("Invalid Config - HealthPort")
	testCase.healthPort = "NAN"
	testCase.expectedError = getInvalidIntegerEnvironmentVariableError(testCase.healthPort, env.HealthPortEnvVarKey)
	testCases = append(testCases, testCase)

	testCase = getValidTestCase("Invalid Config - ResyncPeriodMinutes")
	testCase.resyncPeriodMinutes = "NAN"
	testCase.expectedError = getInvalidIntegerEnvironmentVariableError(testCase.resyncPeriodMinutes, env.ResyncPeriodMinutesEnvVarKey)
	testCases = append(testCases, testCase)

	testCase = getValidTestCase("Valid Config - Default ResyncPeriodMinutes")
	testCase.resyncPeriodMinutes = ""
	testCase.expectedResyncPeriod = "600" // 10 hours - default value
	testCases = append(testCases, testCase)

	testCase = getValidTestCase("Missing Required Config - KafkaBrokers")
	testCase.kafkaBrokers = ""
	testCase.expectedError = getMissingRequiredEnvironmentVariableError(env.KafkaBrokerEnvVarKey)
	testCases = append(testCases, testCase)

	testCase = getValidTestCase("Missing Required Config - ServiceName")
	testCase.serviceName = ""
	testCase.expectedError = getMissingRequiredEnvironmentVariableError(env.ServiceNameEnvVarKey)
	testCases = append(testCases, testCase)

	testCase = getValidTestCase("Missing Required Config - PodName")
	testCase.podName = ""
	testCase.expectedError = getMissingRequiredEnvironmentVariableError(env.PodNameEnvVarKey)
	testCases = append(testCases, testCase)

	testCase = getValidTestCase("Missing Required Config - ContainerName")
	testCase.containerName = ""
	testCase.expectedError = getMissingRequiredEnvironmentVariableError(env.ContainerNameEnvVarKey)
	testCases = append(testCases, testCase)

	// Loop Over All The TestCases
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {

			// (Re)Setup The Environment Variables From TestCase
			os.Clearenv()
			assertSetenv(t, system.NamespaceEnvKey, testCase.systemNamespace)
			assertSetenv(t, env.MetricsDomainEnvVarKey, testCase.metricsDomain)
			assertSetenvNonempty(t, env.MetricsPortEnvVarKey, testCase.metricsPort)
			assertSetenvNonempty(t, env.HealthPortEnvVarKey, testCase.healthPort)
			assertSetenv(t, env.KafkaBrokerEnvVarKey, testCase.kafkaBrokers)
			assertSetenv(t, env.ServiceNameEnvVarKey, testCase.serviceName)
			assertSetenv(t, env.KafkaUsernameEnvVarKey, testCase.kafkaUsername)
			assertSetenv(t, env.KafkaPasswordEnvVarKey, testCase.kafkaPassword)
			assertSetenv(t, env.PodNameEnvVarKey, testCase.podName)
			assertSetenv(t, env.ContainerNameEnvVarKey, testCase.containerName)
			assertSetenvNonempty(t, env.ResyncPeriodMinutesEnvVarKey, testCase.resyncPeriodMinutes)

			// Perform The Test
			environment, err := GetEnvironment(logger)

			// Verify The Results
			if testCase.expectedError == nil {

				assert.Nil(t, err)
				assert.NotNil(t, environment)
				assert.Equal(t, testCase.systemNamespace, environment.SystemNamespace)
				assert.Equal(t, testCase.metricsPort, strconv.Itoa(environment.MetricsPort))
				assert.Equal(t, testCase.healthPort, strconv.Itoa(environment.HealthPort))
				assert.Equal(t, testCase.kafkaBrokers, environment.KafkaBrokers)
				assert.Equal(t, testCase.serviceName, environment.ServiceName)
				assert.Equal(t, testCase.kafkaUsername, environment.KafkaUsername)
				assert.Equal(t, testCase.kafkaPassword, environment.KafkaPassword)
				assert.Equal(t, testCase.podName, environment.PodName)
				assert.Equal(t, testCase.containerName, environment.ContainerName)
				assert.Equal(t, testCase.expectedResyncPeriod, strconv.Itoa(int(environment.ResyncPeriod/time.Minute)))

			} else {
				assert.Equal(t, testCase.expectedError, err)
				assert.Nil(t, environment)
			}
		})
	}
}

func assertSetenv(t *testing.T, envKey string, value string) {
	assert.Nil(t, os.Setenv(envKey, value))
}

func assertSetenvNonempty(t *testing.T, envKey string, value string) {
	if len(value) > 0 {
		assertSetenv(t, envKey, value)
	}
}

// Get The Base / Valid Test Case - All Config Specified / No Errors
func getValidTestCase(name string) TestCase {
	return TestCase{
		name:                 name,
		systemNamespace:      systemNamespace,
		metricsPort:          metricsPort,
		metricsDomain:        metricsDomain,
		healthPort:           healthPort,
		resyncPeriodMinutes:  resyncPeriodMinutes,
		kafkaBrokers:         kafkaBrokers,
		serviceName:          serviceName,
		kafkaUsername:        kafkaUsername,
		kafkaPassword:        kafkaPassword,
		podName:              podName,
		containerName:        containerName,
		expectedResyncPeriod: resyncPeriodMinutes,
		expectedError:        nil,
	}
}

// Get The Expected Error Message For A Missing Required Environment Variable
func getMissingRequiredEnvironmentVariableError(envVarKey string) error {
	return fmt.Errorf("missing required environment variable '%s'", envVarKey)
}

// Get The Expected Error Message For An Invalid Integer Environment Variable
func getInvalidIntegerEnvironmentVariableError(value string, envVarKey string) error {
	return fmt.Errorf("invalid (non int) value '%s' for environment variable '%s'", value, envVarKey)
}

// Initialize The Logger - Fatal Exit Upon Error
func getLogger() *zap.Logger {
	logger, err := zap.NewProduction() // For Now Just Use The Default Zap Production Logger
	if err != nil {
		log.Fatalf("Failed To Create New Zap Production Logger: %+v", err)
	}
	return logger
}
