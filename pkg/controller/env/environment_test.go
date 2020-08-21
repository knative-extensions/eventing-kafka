package env

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	commonenv "knative.dev/eventing-kafka/pkg/common/env"
)

// Test Constants
const (
	serviceAccount = "TestServiceAccount"
	metricsPort    = "9999"
	metricsDomain  = "example.com/kafka-eventing"
	kafkaProvider  = "confluent"

	defaultKafkaConsumers = "5"

	dispatcherImage = "TestDispatcherImage"

	channelImage = "TestChannelImage"
)

// Define The TestCase Struct
type TestCase struct {
	name                  string
	serviceAccount        string
	metricsPort           string
	metricsDomain         string
	kafkaProvider         string
	defaultKafkaConsumers string
	dispatcherImage       string
	channelImage          string
	expectedError         error
}

// Test All Permutations Of The GetEnvironment() Functionality
func TestGetEnvironment(t *testing.T) {

	// Get A Logger Reference For Testing
	logger := getLogger()

	// Define The TestCases
	testCases := make([]TestCase, 0, 30)
	testCase := getValidTestCase("Valid Complete Config")
	testCases = append(testCases, testCase)

	testCase = getValidTestCase("Missing Required Config - ServiceAccount")
	testCase.serviceAccount = ""
	testCase.expectedError = getMissingRequiredEnvironmentVariableError(commonenv.ServiceAccountEnvVarKey)
	testCases = append(testCases, testCase)

	testCase = getValidTestCase("Missing Required Config - MetricsDomain")
	testCase.metricsDomain = ""
	testCase.expectedError = getMissingRequiredEnvironmentVariableError(commonenv.MetricsDomainEnvVarKey)
	testCases = append(testCases, testCase)

	testCase = getValidTestCase("Missing Required Config - MetricsPort")
	testCase.metricsPort = ""
	testCase.expectedError = getMissingRequiredEnvironmentVariableError(commonenv.MetricsPortEnvVarKey)
	testCases = append(testCases, testCase)

	testCase = getValidTestCase("Invalid Config - MetricsPort")
	testCase.metricsPort = "NAN"
	testCase.expectedError = getInvalidIntEnvironmentVariableError(testCase.metricsPort, commonenv.MetricsPortEnvVarKey)
	testCases = append(testCases, testCase)

	testCase = getValidTestCase("Missing Required Config - DispatcherImage")
	testCase.dispatcherImage = ""
	testCase.expectedError = getMissingRequiredEnvironmentVariableError(DispatcherImageEnvVarKey)
	testCases = append(testCases, testCase)

	testCase = getValidTestCase("Missing Required Config - ChannelImage")
	testCase.channelImage = ""
	testCase.expectedError = getMissingRequiredEnvironmentVariableError(ChannelImageEnvVarKey)
	testCases = append(testCases, testCase)

	// Loop Over All The TestCases
	for _, testCase := range testCases {

		// (Re)Setup The Environment Variables From TestCase
		os.Clearenv()
		assertSetenv(t, commonenv.ServiceAccountEnvVarKey, testCase.serviceAccount)
		assertSetenv(t, commonenv.MetricsDomainEnvVarKey, testCase.metricsDomain)
		assertSetenvNonempty(t, commonenv.MetricsPortEnvVarKey, testCase.metricsPort)
		assertSetenv(t, DispatcherImageEnvVarKey, testCase.dispatcherImage)
		assertSetenv(t, ChannelImageEnvVarKey, testCase.channelImage)

		// Perform The Test
		environment, err := GetEnvironment(logger)

		// Verify The Results
		if testCase.expectedError == nil {

			assert.Nil(t, err)
			assert.NotNil(t, environment)
			assert.Equal(t, testCase.serviceAccount, environment.ServiceAccount)
			assert.Equal(t, testCase.metricsPort, strconv.Itoa(environment.MetricsPort))
			assert.Equal(t, testCase.channelImage, environment.ChannelImage)
			assert.Equal(t, testCase.dispatcherImage, environment.DispatcherImage)

		} else {
			assert.Equal(t, testCase.expectedError, err)
			assert.Nil(t, environment)
		}

	}
}

// Get The Base / Valid Test Case - All Config Specified / No Errors
func getValidTestCase(name string) TestCase {
	return TestCase{
		name:                  name,
		serviceAccount:        serviceAccount,
		metricsPort:           metricsPort,
		metricsDomain:         metricsDomain,
		kafkaProvider:         kafkaProvider,
		defaultKafkaConsumers: defaultKafkaConsumers,
		dispatcherImage:       dispatcherImage,
		channelImage:          channelImage,
		expectedError:         nil,
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
