package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/api/resource"
	"knative.dev/eventing-kafka/pkg/common/config"
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

	// Environment settings
	envMetricsPort   int
	envMetricsDomain string

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

	testCase = getValidTestCase("Invalid Config - Channel.CpuLimit")
	testCase.channelCpuLimit = resource.Quantity{}
	testCase.expectedError = ControllerConfigurationError("Channel.CpuLimit must be nonzero")
	testCases = append(testCases, testCase)

	testCase = getValidTestCase("Invalid Config - Channel.CpuRequest")
	testCase.channelCpuRequest = resource.Quantity{}
	testCase.expectedError = ControllerConfigurationError("Channel.CpuRequest must be nonzero")
	testCases = append(testCases, testCase)

	testCase = getValidTestCase("Invalid Config - Channel.MemoryLimit")
	testCase.channelMemoryLimit = resource.Quantity{}
	testCase.expectedError = ControllerConfigurationError("Channel.MemoryLimit must be nonzero")
	testCases = append(testCases, testCase)

	testCase = getValidTestCase("Invalid Config - Channel.MemoryRequest")
	testCase.channelMemoryRequest = resource.Quantity{}
	testCase.expectedError = ControllerConfigurationError("Channel.MemoryRequest must be nonzero")
	testCases = append(testCases, testCase)

	testCase = getValidTestCase("Invalid Config - Channel.Replicas")
	testCase.channelReplicas = -1
	testCase.expectedError = ControllerConfigurationError("Channel.Replicas must be > 0")
	testCases = append(testCases, testCase)

	testCase = getValidTestCase("Invalid Config - Kafka.Provider")
	testCase.kafkaAdminType = "invalidprovider"
	testCase.expectedError = ControllerConfigurationError("Invalid / Unknown KafkaProvider: invalidprovider")
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
		testConfig.Channel.CpuLimit = testCase.channelCpuLimit
		testConfig.Channel.CpuRequest = testCase.channelCpuRequest
		testConfig.Channel.MemoryLimit = testCase.channelMemoryLimit
		testConfig.Channel.MemoryRequest = testCase.channelMemoryRequest
		testConfig.Channel.Replicas = testCase.channelReplicas

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
