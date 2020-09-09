package config

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"knative.dev/eventing-kafka/pkg/channel/distributed/common/config"
	"knative.dev/eventing-kafka/pkg/channel/distributed/dispatcher/constants"
)

// Test Constants
const (
	exponentialBackoff   = "true"
	maxRetryTime         = 1234567890
	initialRetryInterval = 2345678901
)

// Define The TestCase Struct
type TestCase struct {
	name string

	// Config settings
	exponentialBackoff           *bool
	maxRetryTime                 int64
	initialRetryInterval         int64
	expectedExponentialBackoff   bool
	expectedMaxRetryTime         int64
	expectedInitialRetryInterval int64

	expectedError error
}

// Get The Base / Valid Test Case - All Config Specified / No Errors
func getValidVerifyTestCase(name string) TestCase {
	backoff, _ := strconv.ParseBool(exponentialBackoff)
	return TestCase{
		name:                         name,
		exponentialBackoff:           &backoff,
		maxRetryTime:                 maxRetryTime,
		initialRetryInterval:         initialRetryInterval,
		expectedMaxRetryTime:         maxRetryTime,
		expectedInitialRetryInterval: initialRetryInterval,
		expectedExponentialBackoff:   backoff,
		expectedError:                nil,
	}
}

// Test All Permutations Of The VerifyConfiguration Functionality
func TestVerifyConfiguration(t *testing.T) {

	// Define The TestCases
	testCases := make([]TestCase, 0, 7)
	testCase := getValidVerifyTestCase("Valid Complete Config")
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

		testConfig := &config.EventingKafkaConfig{}
		testConfig.Dispatcher.RetryInitialIntervalMillis = testCase.initialRetryInterval
		testConfig.Dispatcher.RetryTimeMillis = testCase.maxRetryTime
		testConfig.Dispatcher.RetryExponentialBackoff = testCase.exponentialBackoff

		// Perform The Test
		err := VerifyConfiguration(testConfig)

		// Verify The Results
		if testCase.expectedError == nil {
			assert.Nil(t, err)
			assert.Equal(t, testCase.expectedInitialRetryInterval, testConfig.Dispatcher.RetryInitialIntervalMillis)
			assert.Equal(t, testCase.expectedMaxRetryTime, testConfig.Dispatcher.RetryTimeMillis)
			assert.Equal(t, testCase.expectedExponentialBackoff, *testConfig.Dispatcher.RetryExponentialBackoff)
		} else {
			assert.Equal(t, testCase.expectedError, err)
		}

	}
}
