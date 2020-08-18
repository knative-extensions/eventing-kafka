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
	commonenv "knative.dev/eventing-kafka/pkg/common/env"
	"knative.dev/eventing-kafka/pkg/dispatcher/constants"
)

// Test Constants
const (
	metricsPort          = "9999"
	metricsDomain        = "kafka-eventing"
	healthPort           = "1234"
	exponentialBackoff   = "true"
	expBackoffPresent    = "true"
	maxRetryTime         = "1234567890"
	initialRetryInterval = "2345678901"
	kafkaBrokers         = "TestKafkaBrokers"
	kafkaTopic           = "TestKafkaTopic"
	channelKey           = "TestChannelKey"
	serviceName          = "TestServiceName"
	kafkaUsername        = "TestKafkaUsername"
	kafkaPassword        = "TestKafkaPassword"
)

// Define The TestCase Struct
type TestCase struct {
	name                 string
	metricsPort          string
	metricsDomain        string
	healthPort           string
	exponentialBackoff   string
	expBackoffPresent    string
	maxRetryTime         string
	initialRetryInterval string
	kafkaBrokers         string
	kafkaTopic           string
	channelKey           string
	serviceName          string
	kafkaUsername        string
	kafkaPassword        string
	expectedError        error
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
		name:                 name,
		metricsPort:          metricsPort,
		metricsDomain:        metricsDomain,
		healthPort:           healthPort,
		exponentialBackoff:   exponentialBackoff,
		expBackoffPresent:    expBackoffPresent,
		maxRetryTime:         maxRetryTime,
		initialRetryInterval: initialRetryInterval,
		kafkaBrokers:         kafkaBrokers,
		kafkaTopic:           kafkaTopic,
		channelKey:           channelKey,
		serviceName:          serviceName,
		kafkaUsername:        kafkaUsername,
		kafkaPassword:        kafkaPassword,
		expectedError:        nil,
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

// Define The VerifyTestCase Struct
type VerifyTestCase struct {
	name string

	// Environment settings
	envMetricsPort   int
	envMetricsDomain string
	envHealthPort    int

	// Config settings
	metricsPort                  int
	metricsDomain                string
	healthPort                   int
	exponentialBackoff           *bool
	maxRetryTime                 int64
	initialRetryInterval         int64
	expectedMetricsPort          int
	expectedMetricsDomain        string
	expectedHealthPort           int
	expectedExponentialBackoff   bool
	expectedMaxRetryTime         int64
	expectedInitialRetryInterval int64

	expectedError error
}

// Convenience function for testing to allow inline string-to-int conversions
func atoiOrZero(str string) int64 {
	intOut, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		return 0
	}
	return intOut
}

// Get The Base / Valid Test Case - All Config Specified / No Errors
func getValidVerifyTestCase(name string) VerifyTestCase {
	backoff, _ := strconv.ParseBool(exponentialBackoff)
	return VerifyTestCase{
		name:                         name,
		envMetricsPort:               int(atoiOrZero(metricsPort)),
		envMetricsDomain:             metricsDomain,
		envHealthPort:                int(atoiOrZero(healthPort)),
		metricsPort:                  int(atoiOrZero(metricsPort)),
		metricsDomain:                metricsDomain,
		healthPort:                   int(atoiOrZero(healthPort)),
		exponentialBackoff:           &backoff,
		maxRetryTime:                 atoiOrZero(maxRetryTime),
		initialRetryInterval:         atoiOrZero(initialRetryInterval),
		expectedMetricsPort:          int(atoiOrZero(metricsPort)),
		expectedMetricsDomain:        metricsDomain,
		expectedHealthPort:           int(atoiOrZero(healthPort)),
		expectedExponentialBackoff:   backoff,
		expectedMaxRetryTime:         atoiOrZero(maxRetryTime),
		expectedInitialRetryInterval: atoiOrZero(initialRetryInterval),
		expectedError:                nil,
	}
}

func getValidEnvironment(t *testing.T) *Environment {
	assertSetenv(t, commonenv.MetricsDomainEnvVarKey, metricsDomain)
	assertSetenvNonempty(t, commonenv.MetricsPortEnvVarKey, metricsPort)
	assertSetenvNonempty(t, commonenv.HealthPortEnvVarKey, healthPort)
	assertSetenv(t, commonenv.ExponentialBackoffEnvVarKey, exponentialBackoff)
	assertSetenvNonempty(t, commonenv.MaxRetryTimeEnvVarKey, maxRetryTime)
	assertSetenvNonempty(t, commonenv.InitialRetryIntervalEnvVarKey, initialRetryInterval)
	assertSetenv(t, commonenv.KafkaBrokerEnvVarKey, kafkaBrokers)
	assertSetenv(t, commonenv.KafkaTopicEnvVarKey, kafkaTopic)
	assertSetenv(t, commonenv.ChannelKeyEnvVarKey, channelKey)
	assertSetenv(t, commonenv.ServiceNameEnvVarKey, serviceName)
	assertSetenv(t, commonenv.KafkaUsernameEnvVarKey, kafkaUsername)
	assertSetenv(t, commonenv.KafkaPasswordEnvVarKey, kafkaPassword)

	environment, err := GetEnvironment(getLogger())
	assert.Nil(t, err)
	return environment
}

// Test All Permutations Of The VerifyOverrides() Functionality
func TestVerifyOverrides_Validation(t *testing.T) {

	// Define The TestCases
	testCases := make([]VerifyTestCase, 0, 7)
	testCase := getValidVerifyTestCase("Valid Complete Config")
	testCases = append(testCases, testCase)

	testCase = getValidVerifyTestCase("Invalid Config - MetricsPort")
	testCase.metricsPort = -1
	testCase.envMetricsPort = -1
	testCase.expectedError = DispatcherConfigurationError("Metrics.Port must be > 0")
	testCases = append(testCases, testCase)

	testCase = getValidVerifyTestCase("Invalid Config - MetricsPort - Overridden")
	testCase.metricsPort = -1
	testCase.expectedError = nil
	testCases = append(testCases, testCase)

	testCase = getValidVerifyTestCase("Invalid Config - MetricsDomain")
	testCase.metricsDomain = ""
	testCase.envMetricsDomain = ""
	testCase.expectedError = DispatcherConfigurationError("Metrics.Domain must not be empty")
	testCases = append(testCases, testCase)

	testCase = getValidVerifyTestCase("Invalid Config - MetricsDomain - Overridden")
	testCase.metricsDomain = ""
	testCase.expectedError = nil
	testCases = append(testCases, testCase)

	testCase = getValidVerifyTestCase("Invalid Config - HealthPort")
	testCase.healthPort = -1
	testCase.envHealthPort = -1
	testCase.expectedError = DispatcherConfigurationError("Health.Port must be > 0")
	testCases = append(testCases, testCase)

	testCase = getValidVerifyTestCase("Invalid Config - HealthPort - Overridden")
	testCase.healthPort = -1
	testCase.expectedError = nil
	testCases = append(testCases, testCase)

	testCase = getValidVerifyTestCase("Invalid Config - RetryExponentialBackoff")
	testCase.expectedExponentialBackoff = constants.DefaultExponentialBackoff
	testCase.exponentialBackoff = nil // This setting should be changed to DefaultExponentialBackoff
	testCase.expectedError = nil
	testCases = append(testCases, testCase)

	testCase = getValidVerifyTestCase("Invalid Config - RetryTimeMillis")
	testCase.maxRetryTime = -1
	testCase.expectedMaxRetryTime = constants.DefaultEventRetryTimeMillisMax
	testCase.expectedError = nil // This setting should be changed to DefaultEventRetryTimeMillisMax
	testCases = append(testCases, testCase)

	testCase = getValidVerifyTestCase("Optional Config - RetryInitialIntervalMillis")
	testCase.initialRetryInterval = -1
	testCase.expectedInitialRetryInterval = constants.DefaultEventRetryInitialIntervalMillis
	testCase.expectedError = nil // This setting should be changed to DefaultEventRetryInitialIntervalMillis
	testCases = append(testCases, testCase)

	// Loop Over All The TestCases
	for _, testCase := range testCases {

		environment := getValidEnvironment(t)
		environment.MetricsPort = testCase.envMetricsPort
		environment.MetricsDomain = testCase.envMetricsDomain
		environment.HealthPort = testCase.envHealthPort
		testConfig := &config.EventingKafkaConfig{}
		testConfig.Metrics.Port = testCase.metricsPort
		testConfig.Metrics.Domain = testCase.metricsDomain
		testConfig.Health.Port = testCase.healthPort
		testConfig.Dispatcher.RetryInitialIntervalMillis = testCase.initialRetryInterval
		testConfig.Dispatcher.RetryTimeMillis = testCase.maxRetryTime
		testConfig.Dispatcher.RetryExponentialBackoff = testCase.exponentialBackoff

		// Perform The Test
		err := VerifyOverrides(testConfig, environment)

		// Verify The Results
		if testCase.expectedError == nil {
			assert.Nil(t, err)
			assert.NotNil(t, environment)
			assert.Equal(t, testCase.expectedMetricsPort, testConfig.Metrics.Port)
			assert.Equal(t, testCase.expectedMetricsDomain, testConfig.Metrics.Domain)
			assert.Equal(t, testCase.expectedHealthPort, testConfig.Health.Port)
			assert.Equal(t, testCase.expectedInitialRetryInterval, testConfig.Dispatcher.RetryInitialIntervalMillis)
			assert.Equal(t, testCase.expectedMaxRetryTime, testConfig.Dispatcher.RetryTimeMillis)
			assert.Equal(t, testCase.expectedExponentialBackoff, *testConfig.Dispatcher.RetryExponentialBackoff)
		} else {
			assert.Equal(t, testCase.expectedError, err)
		}

	}
}

func TestApplyOverrides_EnvironmentSettings(t *testing.T) {
	var ekConfig config.EventingKafkaConfig

	environment := getValidEnvironment(t)

	err := VerifyOverrides(&ekConfig, environment)
	assert.Nil(t, err)

	assert.Equal(t, environment.MetricsPort, ekConfig.Metrics.Port)
	assert.Equal(t, environment.MetricsDomain, ekConfig.Metrics.Domain)
	assert.Equal(t, constants.DefaultExponentialBackoff, *ekConfig.Dispatcher.RetryExponentialBackoff)
	assert.Equal(t, environment.HealthPort, ekConfig.Health.Port)
	assert.Equal(t, int64(constants.DefaultEventRetryTimeMillisMax), ekConfig.Dispatcher.RetryTimeMillis)
	assert.Equal(t, int64(constants.DefaultEventRetryInitialIntervalMillis), ekConfig.Dispatcher.RetryInitialIntervalMillis)
	assert.Equal(t, environment.KafkaBrokers, ekConfig.Kafka.Brokers)
	assert.Equal(t, environment.KafkaTopic, ekConfig.Kafka.Topic.Name)
	assert.Equal(t, environment.ChannelKey, ekConfig.Kafka.ChannelKey)
	assert.Equal(t, environment.ServiceName, ekConfig.Kafka.ServiceName)
	assert.Equal(t, environment.KafkaUsername, ekConfig.Kafka.Username)
	assert.Equal(t, environment.KafkaPassword, ekConfig.Kafka.Password)
}
