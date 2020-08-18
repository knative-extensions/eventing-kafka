package env

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/api/resource"
	"knative.dev/eventing-kafka/pkg/common/config"
	commonenv "knative.dev/eventing-kafka/pkg/common/env"
)

// Test Constants
const (
	serviceAccount = "TestServiceAccount"
	metricsPort    = "9999"
	metricsDomain  = "example.com/kafka-eventing"
	kafkaProvider  = "confluent"

	defaultNumPartitions     = "7"
	defaultReplicationFactor = "2"
	defaultRetentionMillis   = "13579"
	defaultKafkaConsumers    = "5"

	dispatcherImage         = "TestDispatcherImage"
	dispatcherReplicas      = "1"
	dispatcherMemoryRequest = "20Mi"
	dispatcherCpuRequest    = "100m"
	dispatcherMemoryLimit   = "50Mi"
	dispatcherCpuLimit      = "300m"

	channelImage         = "TestChannelImage"
	channelReplicas      = "1"
	channelMemoryRequest = "10Mi"
	channelCpuRquest     = "10m"
	channelMemoryLimit   = "20Mi"
	channelCpuLimit      = "100m"
)

// Define The TestCase Struct
type TestCase struct {
	name                     string
	serviceAccount           string
	metricsPort              string
	metricsDomain            string
	kafkaProvider            string
	defaultNumPartitions     string
	defaultReplicationFactor string
	defaultRetentionMillis   string
	defaultKafkaConsumers    string
	dispatcherImage          string
	channelImage             string
	expectedError            error
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

// Define The VerifyTestCase Struct
type VerifyTestCase struct {
	name string

	// Environment settings
	envMetricsPort   int
	envMetricsDomain string

	// Config settings
	metricsPort                        int
	metricsDomain                      string
	expectedMetricsPort                int
	expectedMetricsDomain              string
	kafkaTopicDefaultNumPartitions     int32
	kafkaTopicDefaultReplicationFactor int16
	kafkaTopicDefaultRetentionMillis   int64
	kafkaProvider                      string
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
	return VerifyTestCase{
		name:                               name,
		envMetricsPort:                     int(atoiOrZero(metricsPort)),
		envMetricsDomain:                   metricsDomain,
		metricsPort:                        int(atoiOrZero(metricsPort)),
		metricsDomain:                      metricsDomain,
		expectedMetricsPort:                int(atoiOrZero(metricsPort)),
		expectedMetricsDomain:              metricsDomain,
		kafkaTopicDefaultNumPartitions:     int32(atoiOrZero(defaultNumPartitions)),
		kafkaTopicDefaultReplicationFactor: int16(atoiOrZero(defaultReplicationFactor)),
		kafkaTopicDefaultRetentionMillis:   atoiOrZero(defaultRetentionMillis),
		kafkaProvider:                      kafkaProvider,
		dispatcherCpuLimit:                 resource.MustParse(dispatcherCpuLimit),
		dispatcherCpuRequest:               resource.MustParse(dispatcherCpuRequest),
		dispatcherMemoryLimit:              resource.MustParse(dispatcherMemoryLimit),
		dispatcherMemoryRequest:            resource.MustParse(dispatcherMemoryRequest),
		dispatcherReplicas:                 int(atoiOrZero(dispatcherReplicas)),
		channelCpuLimit:                    resource.MustParse(channelCpuLimit),
		channelCpuRequest:                  resource.MustParse(channelCpuRquest),
		channelMemoryLimit:                 resource.MustParse(channelMemoryLimit),
		channelMemoryRequest:               resource.MustParse(channelMemoryRequest),
		channelReplicas:                    int(atoiOrZero(channelReplicas)),
		expectedError:                      nil,
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

func getValidEnvironment(t *testing.T) *Environment {
	assertSetenv(t, commonenv.ServiceAccountEnvVarKey, serviceAccount)
	assertSetenv(t, commonenv.MetricsDomainEnvVarKey, metricsDomain)
	assertSetenvNonempty(t, commonenv.MetricsPortEnvVarKey, metricsPort)
	assertSetenvNonempty(t, DefaultNumPartitionsEnvVarKey, defaultNumPartitions)
	assertSetenv(t, DefaultReplicationFactorEnvVarKey, defaultReplicationFactor)
	assertSetenv(t, DefaultRetentionMillisEnvVarKey, defaultRetentionMillis)
	assertSetenv(t, DispatcherImageEnvVarKey, dispatcherImage)
	assertSetenv(t, ChannelImageEnvVarKey, channelImage)

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
	testCase.expectedError = ControllerConfigurationError("Metrics.Port must be > 0")
	testCases = append(testCases, testCase)

	testCase = getValidVerifyTestCase("Invalid Config - MetricsPort - Overridden")
	testCase.metricsPort = -1
	testCase.expectedError = nil
	testCases = append(testCases, testCase)

	testCase = getValidVerifyTestCase("Invalid Config - MetricsDomain")
	testCase.metricsDomain = ""
	testCase.envMetricsDomain = ""
	testCase.expectedError = ControllerConfigurationError("Metrics.Domain must not be empty")
	testCases = append(testCases, testCase)

	testCase = getValidVerifyTestCase("Invalid Config - MetricsDomain - Overridden")
	testCase.metricsDomain = ""
	testCase.expectedError = nil
	testCases = append(testCases, testCase)

	testCase = getValidVerifyTestCase("Invalid Config - Kafka.Topic.DefaultNumPartitions")
	testCase.kafkaTopicDefaultNumPartitions = -1
	testCase.expectedError = ControllerConfigurationError("Kafka.Topic.DefaultNumPartitions must be > 0")
	testCases = append(testCases, testCase)

	testCase = getValidVerifyTestCase("Invalid Config - Kafka.Topic.DefaultReplicationFactor")
	testCase.kafkaTopicDefaultReplicationFactor = -1
	testCase.expectedError = ControllerConfigurationError("Kafka.Topic.DefaultReplicationFactor must be > 0")
	testCases = append(testCases, testCase)

	testCase = getValidVerifyTestCase("Invalid Config - Kafka.Topic.DefaultRetentionMillis")
	testCase.kafkaTopicDefaultRetentionMillis = -1
	testCase.expectedError = ControllerConfigurationError("Kafka.Topic.DefaultRetentionMillis must be > 0")
	testCases = append(testCases, testCase)

	testCase = getValidVerifyTestCase("Invalid Config - Dispatcher.CpuLimit")
	testCase.dispatcherCpuLimit = resource.Quantity{}
	testCase.expectedError = ControllerConfigurationError("Dispatcher.CpuLimit must be nonzero")
	testCases = append(testCases, testCase)

	testCase = getValidVerifyTestCase("Invalid Config - Dispatcher.CpuRequest")
	testCase.dispatcherCpuRequest = resource.Quantity{}
	testCase.expectedError = ControllerConfigurationError("Dispatcher.CpuRequest must be nonzero")
	testCases = append(testCases, testCase)

	testCase = getValidVerifyTestCase("Invalid Config - Dispatcher.MemoryLimit")
	testCase.dispatcherMemoryLimit = resource.Quantity{}
	testCase.expectedError = ControllerConfigurationError("Dispatcher.MemoryLimit must be nonzero")
	testCases = append(testCases, testCase)

	testCase = getValidVerifyTestCase("Invalid Config - Dispatcher.MemoryRequest")
	testCase.dispatcherMemoryRequest = resource.Quantity{}
	testCase.expectedError = ControllerConfigurationError("Dispatcher.MemoryRequest must be nonzero")
	testCases = append(testCases, testCase)

	testCase = getValidVerifyTestCase("Invalid Config - Dispatcher.Replicas")
	testCase.dispatcherReplicas = -1
	testCase.expectedError = ControllerConfigurationError("Dispatcher.Replicas must be > 0")
	testCases = append(testCases, testCase)

	testCase = getValidVerifyTestCase("Invalid Config - Channel.CpuLimit")
	testCase.channelCpuLimit = resource.Quantity{}
	testCase.expectedError = ControllerConfigurationError("Channel.CpuLimit must be nonzero")
	testCases = append(testCases, testCase)

	testCase = getValidVerifyTestCase("Invalid Config - Channel.CpuRequest")
	testCase.channelCpuRequest = resource.Quantity{}
	testCase.expectedError = ControllerConfigurationError("Channel.CpuRequest must be nonzero")
	testCases = append(testCases, testCase)

	testCase = getValidVerifyTestCase("Invalid Config - Channel.MemoryLimit")
	testCase.channelMemoryLimit = resource.Quantity{}
	testCase.expectedError = ControllerConfigurationError("Channel.MemoryLimit must be nonzero")
	testCases = append(testCases, testCase)

	testCase = getValidVerifyTestCase("Invalid Config - Channel.MemoryRequest")
	testCase.channelMemoryRequest = resource.Quantity{}
	testCase.expectedError = ControllerConfigurationError("Channel.MemoryRequest must be nonzero")
	testCases = append(testCases, testCase)

	testCase = getValidVerifyTestCase("Invalid Config - Channel.Replicas")
	testCase.channelReplicas = -1
	testCase.expectedError = ControllerConfigurationError("Channel.Replicas must be > 0")
	testCases = append(testCases, testCase)

	testCase = getValidVerifyTestCase("Invalid Config - Kafka.Provider")
	testCase.kafkaProvider = "invalidprovider"
	testCase.expectedError = ControllerConfigurationError("Invalid / Unknown KafkaProvider: invalidprovider")
	testCases = append(testCases, testCase)

	// Loop Over All The TestCases
	for _, testCase := range testCases {

		environment := getValidEnvironment(t)
		environment.MetricsPort = testCase.envMetricsPort
		environment.MetricsDomain = testCase.envMetricsDomain
		testConfig := &config.EventingKafkaConfig{}
		testConfig.Metrics.Port = testCase.metricsPort
		testConfig.Metrics.Domain = testCase.metricsDomain
		testConfig.Kafka.Topic.DefaultNumPartitions = testCase.kafkaTopicDefaultNumPartitions
		testConfig.Kafka.Topic.DefaultReplicationFactor = testCase.kafkaTopicDefaultReplicationFactor
		testConfig.Kafka.Topic.DefaultRetentionMillis = testCase.kafkaTopicDefaultRetentionMillis
		testConfig.Kafka.Provider = testCase.kafkaProvider
		testConfig.Dispatcher.CpuLimit = testCase.dispatcherCpuLimit
		testConfig.Dispatcher.CpuRequest = testCase.dispatcherCpuRequest
		testConfig.Dispatcher.MemoryLimit = testCase.dispatcherMemoryLimit
		testConfig.Dispatcher.MemoryRequest = testCase.dispatcherMemoryRequest
		testConfig.Dispatcher.Replicas = testCase.dispatcherReplicas
		testConfig.Channel.CpuLimit = testCase.channelCpuLimit
		testConfig.Channel.CpuRequest = testCase.channelCpuRequest
		testConfig.Channel.MemoryLimit = testCase.channelMemoryLimit
		testConfig.Channel.MemoryRequest = testCase.channelMemoryRequest
		testConfig.Channel.Replicas = testCase.channelReplicas

		// Perform The Test
		err := VerifyOverrides(testConfig, environment)

		// Verify The Results
		if testCase.expectedError == nil {
			assert.Nil(t, err)
			assert.NotNil(t, environment)
			assert.Equal(t, testCase.expectedMetricsPort, testConfig.Metrics.Port)
			assert.Equal(t, testCase.expectedMetricsDomain, testConfig.Metrics.Domain)
			assert.Equal(t, testCase.kafkaTopicDefaultNumPartitions, testConfig.Kafka.Topic.DefaultNumPartitions)
			assert.Equal(t, testCase.kafkaTopicDefaultReplicationFactor, testConfig.Kafka.Topic.DefaultReplicationFactor)
			assert.Equal(t, testCase.kafkaTopicDefaultRetentionMillis, testConfig.Kafka.Topic.DefaultRetentionMillis)
			assert.Equal(t, testCase.kafkaProvider, testConfig.Kafka.Provider)
			assert.Equal(t, testCase.dispatcherCpuLimit, testConfig.Dispatcher.CpuLimit)
			assert.Equal(t, testCase.dispatcherCpuRequest, testConfig.Dispatcher.CpuRequest)
			assert.Equal(t, testCase.dispatcherMemoryLimit, testConfig.Dispatcher.MemoryLimit)
			assert.Equal(t, testCase.dispatcherMemoryRequest, testConfig.Dispatcher.MemoryRequest)
			assert.Equal(t, testCase.dispatcherReplicas, testConfig.Dispatcher.Replicas)
			assert.Equal(t, testCase.channelCpuLimit, testConfig.Channel.CpuLimit)
			assert.Equal(t, testCase.channelCpuRequest, testConfig.Channel.CpuRequest)
			assert.Equal(t, testCase.channelMemoryLimit, testConfig.Channel.MemoryLimit)
			assert.Equal(t, testCase.channelMemoryRequest, testConfig.Channel.MemoryRequest)
			assert.Equal(t, testCase.channelReplicas, testConfig.Channel.Replicas)
		} else {
			assert.Equal(t, testCase.expectedError, err)
		}

	}
}
