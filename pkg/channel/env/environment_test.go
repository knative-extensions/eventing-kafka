package env

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"knative.dev/eventing-kafka/pkg/common/config"
	"knative.dev/eventing-kafka/pkg/common/env"
)

// Test Constants
const (
	metricsPort   = "9999"
	metricsDomain = "kafka-eventing"
	healthPort    = "1234"
	kafkaBrokers  = "TestKafkaBrokers"
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

	testCase = getValidTestCase("Missing Required Config - KafkaBrokers")
	testCase.kafkaBrokers = ""
	testCase.expectedError = getMissingRequiredEnvironmentVariableError(env.KafkaBrokerEnvVarKey)
	testCases = append(testCases, testCase)

	testCase = getValidTestCase("Missing Required Config - ServiceName")
	testCase.serviceName = ""
	testCase.expectedError = getMissingRequiredEnvironmentVariableError(env.ServiceNameEnvVarKey)
	testCases = append(testCases, testCase)

	// Loop Over All The TestCases
	for _, testCase := range testCases {

		// (Re)Setup The Environment Variables From TestCase
		os.Clearenv()
		assertSetenv(t, env.MetricsDomainEnvVarKey, testCase.metricsDomain)
		assertSetenvNonempty(t, env.MetricsPortEnvVarKey, testCase.metricsPort)
		assertSetenvNonempty(t, env.HealthPortEnvVarKey, testCase.healthPort)
		assertSetenv(t, env.KafkaBrokerEnvVarKey, testCase.kafkaBrokers)
		assertSetenv(t, env.ServiceNameEnvVarKey, testCase.serviceName)
		assertSetenv(t, env.KafkaUsernameEnvVarKey, testCase.kafkaUsername)
		assertSetenv(t, env.KafkaPasswordEnvVarKey, testCase.kafkaPassword)

		// Perform The Test
		environment, err := GetEnvironment(logger)

		// Verify The Results
		if testCase.expectedError == nil {

			assert.Nil(t, err)
			assert.NotNil(t, environment)
			assert.Equal(t, testCase.metricsPort, strconv.Itoa(environment.MetricsPort))
			assert.Equal(t, testCase.healthPort, strconv.Itoa(environment.HealthPort))
			assert.Equal(t, testCase.kafkaBrokers, environment.KafkaBrokers)
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

func TestApplyOverrides(t *testing.T) {
	var ekConfig config.EventingKafkaConfig

	assertSetenv(t, env.MetricsDomainEnvVarKey, metricsDomain)
	assertSetenvNonempty(t, env.MetricsPortEnvVarKey, metricsPort)
	assertSetenvNonempty(t, env.HealthPortEnvVarKey, healthPort)
	assertSetenv(t, env.KafkaBrokerEnvVarKey, kafkaBrokers)
	assertSetenv(t, env.ServiceNameEnvVarKey, serviceName)
	assertSetenv(t, env.KafkaUsernameEnvVarKey, kafkaUsername)
	assertSetenv(t, env.KafkaPasswordEnvVarKey, kafkaPassword)

	environment, err := GetEnvironment(getLogger())
	assert.Nil(t, err)

	ApplyOverrides(&ekConfig, environment)

	assert.Equal(t, ekConfig.Metrics.Port, environment.MetricsPort)
	assert.Equal(t, ekConfig.Metrics.Domain, environment.MetricsDomain)
	assert.Equal(t, ekConfig.Health.Port, environment.HealthPort)
	assert.Equal(t, ekConfig.Kafka.Brokers, environment.KafkaBrokers)
	assert.Equal(t, ekConfig.Kafka.ServiceName, environment.ServiceName)
	assert.Equal(t, ekConfig.Kafka.Username, environment.KafkaUsername)
	assert.Equal(t, ekConfig.Kafka.Password, environment.KafkaPassword)
}
