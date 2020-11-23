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
	"context"
	"fmt"
	"knative.dev/pkg/system"
	"log"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"knative.dev/eventing-kafka/pkg/channel/distributed/common/env"
)

// Test Constants
const (
	systemNamespace     = "test-system-namespace"
	serviceAccount      = "TestServiceAccount"
	metricsPort         = "9999"
	metricsDomain       = "example.com/kafka-eventing"
	resyncPeriodMinutes = "3600"

	defaultKafkaConsumers = "5"

	dispatcherImage = "TestDispatcherImage"

	receiverImage = "TestReceiverImage"
)

// Define The TestCase Struct
type TestCase struct {
	name                  string
	systemNamespace       string
	serviceAccount        string
	metricsPort           string
	metricsDomain         string
	resyncPeriodMinutes   string
	defaultKafkaConsumers string
	dispatcherImage       string
	channelImage          string
	expectedError         error
	expectedResyncPeriod  string
}

// Test All Permutations Of The GetEnvironment() Functionality
func TestGetEnvironment(t *testing.T) {

	// Get A Logger Reference For Testing
	logger := getLogger()

	// Define The TestCases
	testCases := make([]TestCase, 0, 30)
	testCase := getValidTestCase("Valid Complete Config")
	testCases = append(testCases, testCase)

	testCase = getValidTestCase("Missing Required Config - SystemNamespace")
	testCase.systemNamespace = ""
	testCase.expectedError = getMissingRequiredEnvironmentVariableError(system.NamespaceEnvKey)
	testCases = append(testCases, testCase)

	testCase = getValidTestCase("Missing Required Config - ServiceAccount")
	testCase.serviceAccount = ""
	testCase.expectedError = getMissingRequiredEnvironmentVariableError(env.ServiceAccountEnvVarKey)
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
	testCase.expectedError = getInvalidIntEnvironmentVariableError(testCase.metricsPort, env.MetricsPortEnvVarKey)
	testCases = append(testCases, testCase)

	testCase = getValidTestCase("Missing Required Config - DispatcherImage")
	testCase.dispatcherImage = ""
	testCase.expectedError = getMissingRequiredEnvironmentVariableError(DispatcherImageEnvVarKey)
	testCases = append(testCases, testCase)

	testCase = getValidTestCase("Missing Required Config - ReceiverImage")
	testCase.channelImage = ""
	testCase.expectedError = getMissingRequiredEnvironmentVariableError(ReceiverImageEnvVarKey)
	testCases = append(testCases, testCase)

	testCase = getValidTestCase("Invalid Config - ResyncPeriodMinutes")
	testCase.resyncPeriodMinutes = "NAN"
	testCase.expectedError = getInvalidIntEnvironmentVariableError(testCase.resyncPeriodMinutes, env.ResyncPeriodMinutesEnvVarKey)
	testCases = append(testCases, testCase)

	testCase = getValidTestCase("Valid Config - Default ResyncPeriodMinutes")
	testCase.resyncPeriodMinutes = ""
	testCase.expectedResyncPeriod = "600" // 10 hours - default value
	testCases = append(testCases, testCase)

	// Loop Over All The TestCases
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			// (Re)Setup The Environment Variables From TestCase
			setupTestEnvironment(t, testCase)

			// Perform The Test
			environment, err := GetEnvironment(logger)

			// Verify The Results
			if testCase.expectedError == nil {

				assert.Nil(t, err)
				assert.NotNil(t, environment)
				assert.Equal(t, testCase.systemNamespace, environment.SystemNamespace)
				assert.Equal(t, testCase.serviceAccount, environment.ServiceAccount)
				assert.Equal(t, testCase.metricsPort, strconv.Itoa(environment.MetricsPort))
				assert.Equal(t, testCase.channelImage, environment.ReceiverImage)
				assert.Equal(t, testCase.dispatcherImage, environment.DispatcherImage)
				assert.Equal(t, testCase.expectedResyncPeriod, strconv.Itoa(int(environment.ResyncPeriod/time.Minute)))

			} else {
				assert.Equal(t, testCase.expectedError, err)
				assert.Nil(t, environment)
			}
		})
	}
}

// Test that the FromContext function loads an Environment struct from a Context properly
func TestFromContext(t *testing.T) {
	ctx := context.TODO()
	environment, err := FromContext(ctx)
	assert.Nil(t, environment)
	assert.NotNil(t, err)
	ctx = context.WithValue(ctx, Key{}, &Environment{SystemNamespace: "test-namespace"})
	environment, err = FromContext(ctx)
	assert.NotNil(t, environment)
	assert.Nil(t, err)
	assert.Equal(t, "test-namespace", environment.SystemNamespace)
}

// Add TestCase Variables To The Environment
func setupTestEnvironment(t *testing.T, testCase TestCase) {
	os.Clearenv()
	assertSetenv(t, system.NamespaceEnvKey, testCase.systemNamespace)
	assertSetenv(t, env.ServiceAccountEnvVarKey, testCase.serviceAccount)
	assertSetenv(t, env.MetricsDomainEnvVarKey, testCase.metricsDomain)
	assertSetenvNonempty(t, env.MetricsPortEnvVarKey, testCase.metricsPort)
	assertSetenv(t, DispatcherImageEnvVarKey, testCase.dispatcherImage)
	assertSetenv(t, ReceiverImageEnvVarKey, testCase.channelImage)
	assertSetenvNonempty(t, env.ResyncPeriodMinutesEnvVarKey, testCase.resyncPeriodMinutes)
}

// Get The Base / Valid Test Case - All Config Specified / No Errors
func getValidTestCase(name string) TestCase {
	return TestCase{
		name:                  name,
		serviceAccount:        serviceAccount,
		systemNamespace:       systemNamespace,
		metricsPort:           metricsPort,
		metricsDomain:         metricsDomain,
		resyncPeriodMinutes:   resyncPeriodMinutes,
		defaultKafkaConsumers: defaultKafkaConsumers,
		dispatcherImage:       dispatcherImage,
		channelImage:          receiverImage,
		expectedError:         nil,
		expectedResyncPeriod:  resyncPeriodMinutes,
	}
}

// Get The Expected Error Message For A Missing Required Environment Variable
func getMissingRequiredEnvironmentVariableError(envVarKey string) error {
	return fmt.Errorf("missing required environment variable '%s'", envVarKey)
}

// Get The Expected Error Message For An Invalid Integer Environment Variable
func getInvalidIntEnvironmentVariableError(value string, envVarKey string) error {
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

func assertSetenv(t *testing.T, envKey string, value string) {
	assert.Nil(t, os.Setenv(envKey, value))
}

func assertSetenvNonempty(t *testing.T, envKey string, value string) {
	if len(value) > 0 {
		assertSetenv(t, envKey, value)
	}
}
