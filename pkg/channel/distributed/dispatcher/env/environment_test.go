package env

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	commonenv "knative.dev/eventing-kafka/pkg/channel/distributed/common/env"
)

// Test Constants
const (
	metricsPort   = "9999"
	metricsDomain = "kafka-eventing"
	healthPort    = "1234"
	kafkaBrokers  = "TestKafkaBrokers"
	kafkaTopic    = "TestKafkaTopic"
	channelKey    = "TestChannelKey"
	serviceName   = "TestServiceName"
	kafkaUsername = "TestKafkaUsername"
	kafkaPassword = "TestKafkaPassword"
)

// Define The TestCase Struct
type TestCase struct {
	name          string
	metricsPort   string
	metricsDomain string
	healthPort    string
	kafkaBrokers  string
	kafkaTopic    string
	channelKey    string
	serviceName   string
	kafkaUsername string
	kafkaPassword string
	expectedError error
}

// Test All Permutations Of The GetEnvironment() Functionality
func TestGetEnvironment(t *testing.T) {

	// Get A Logger Reference For Testing
	logger := getLogger()

	// Define The TestCases
	testCases := make([]TestCase, 0, 20)
	testCase := getValidTestCase("Valid Complete Config")
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

	testCase = getValidTestCase("Missing Required Config - HealthPort")
	testCase.healthPort = ""
	testCase.expectedError = getMissingRequiredEnvironmentVariableError(commonenv.HealthPortEnvVarKey)
	testCases = append(testCases, testCase)

	testCase = getValidTestCase("Invalid Config - HealthPort")
	testCase.healthPort = "NAN"
	testCase.expectedError = getInvalidIntEnvironmentVariableError(testCase.healthPort, commonenv.HealthPortEnvVarKey)
	testCases = append(testCases, testCase)

	testCase = getValidTestCase("Missing Required Config - KafkaBrokers")
	testCase.kafkaBrokers = ""
	testCase.expectedError = getMissingRequiredEnvironmentVariableError(commonenv.KafkaBrokerEnvVarKey)
	testCases = append(testCases, testCase)

	testCase = getValidTestCase("Missing Required Config - KafkaTopic")
	testCase.kafkaTopic = ""
	testCase.expectedError = getMissingRequiredEnvironmentVariableError(commonenv.KafkaTopicEnvVarKey)
	testCases = append(testCases, testCase)

	testCase = getValidTestCase("Missing Required Config - ChannelKey")
	testCase.channelKey = ""
	testCase.expectedError = getMissingRequiredEnvironmentVariableError(commonenv.ChannelKeyEnvVarKey)
	testCases = append(testCases, testCase)

	testCase = getValidTestCase("Missing Required Config - ServiceName")
	testCase.serviceName = ""
	testCase.expectedError = getMissingRequiredEnvironmentVariableError(commonenv.ServiceNameEnvVarKey)
	testCases = append(testCases, testCase)

	// Loop Over All The TestCases
	for _, testCase := range testCases {

		// (Re)Setup The Environment Variables From TestCase
		os.Clearenv()
		assertSetenv(t, commonenv.MetricsDomainEnvVarKey, testCase.metricsDomain)
		assertSetenvNonempty(t, commonenv.MetricsPortEnvVarKey, testCase.metricsPort)
		assertSetenvNonempty(t, commonenv.HealthPortEnvVarKey, testCase.healthPort)
		assertSetenv(t, commonenv.KafkaBrokerEnvVarKey, testCase.kafkaBrokers)
		assertSetenv(t, commonenv.KafkaTopicEnvVarKey, testCase.kafkaTopic)
		assertSetenv(t, commonenv.ChannelKeyEnvVarKey, testCase.channelKey)
		assertSetenv(t, commonenv.ServiceNameEnvVarKey, testCase.serviceName)
		assertSetenv(t, commonenv.KafkaUsernameEnvVarKey, testCase.kafkaUsername)
		assertSetenv(t, commonenv.KafkaPasswordEnvVarKey, testCase.kafkaPassword)

		// Perform The Test
		environment, err := GetEnvironment(logger)

		// Verify The Results
		if testCase.expectedError == nil {

			assert.Nil(t, err)
			assert.NotNil(t, environment)
			assert.Equal(t, testCase.metricsPort, strconv.Itoa(environment.MetricsPort))
			assert.Equal(t, testCase.healthPort, strconv.Itoa(environment.HealthPort))
			assert.Equal(t, testCase.kafkaBrokers, environment.KafkaBrokers)
			assert.Equal(t, testCase.kafkaTopic, environment.KafkaTopic)
			assert.Equal(t, testCase.channelKey, environment.ChannelKey)
			assert.Equal(t, testCase.serviceName, environment.ServiceName)
			assert.Equal(t, testCase.kafkaUsername, environment.KafkaUsername)
			assert.Equal(t, testCase.kafkaPassword, environment.KafkaPassword)

		} else {
			assert.Equal(t, testCase.expectedError, err)
			assert.Nil(t, environment)
		}

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
		name:          name,
		metricsPort:   metricsPort,
		metricsDomain: metricsDomain,
		healthPort:    healthPort,
		kafkaBrokers:  kafkaBrokers,
		kafkaTopic:    kafkaTopic,
		channelKey:    channelKey,
		serviceName:   serviceName,
		kafkaUsername: kafkaUsername,
		kafkaPassword: kafkaPassword,
		expectedError: nil,
	}
}

// Get The Expected Error Message For A Missing Required Environment Variable
func getMissingRequiredEnvironmentVariableError(envVarKey string) error {
	return fmt.Errorf("missing required environment variable '%s'", envVarKey)
}

// Get The Expected Error Message For An Invalid Int Environment Variable
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
