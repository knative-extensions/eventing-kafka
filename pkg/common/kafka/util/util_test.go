package util

import (
	"fmt"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
	"knative.dev/eventing-kafka/pkg/common/kafka/constants"
)

// Test Enabling Sarama Logging
func TestEnableSaramaLogging(t *testing.T) {

	// Restore Sarama Logger After Test
	saramaLoggerPlaceholder := sarama.Logger
	defer func() {
		sarama.Logger = saramaLoggerPlaceholder
	}()

	// Perform The Test
	EnableSaramaLogging()

	// Verify Results (Not Much Is Possible)
	sarama.Logger.Print("TestMessage")
}

// Test The UpdateSaramaConfig() Functionality
func TestUpdateSaramaConfig(t *testing.T) {

	// Test Data
	clientId := "TestClientId"
	username := "TestUsername"
	password := "TestPassword"

	// Perform The Test
	config := sarama.NewConfig()
	UpdateSaramaConfig(config, clientId, username, password)

	// Verify The Results
	assert.Equal(t, clientId, config.ClientID)
	assert.Equal(t, username, config.Net.SASL.User)
	assert.Equal(t, password, config.Net.SASL.Password)
}

// Test The TopicName() Functionality
func TestTopicName(t *testing.T) {

	// Test Data
	name := "TestName"
	namespace := "TestNamespace"

	// Perform The Test
	actualTopicName := TopicName(namespace, name)

	// Verify The Results
	expectedTopicName := namespace + "." + name
	assert.Equal(t, expectedTopicName, actualTopicName)
}

// Test The AppendChannelServiceNameSuffix() Functionality
func TestAppendChannelServiceNameSuffix(t *testing.T) {

	// Test Data
	channelName := "TestChannelName"

	// Perform The Test
	actualResult := AppendKafkaChannelServiceNameSuffix(channelName)

	// Verify The Results
	expectedResult := fmt.Sprintf("%s-%s", channelName, constants.KafkaChannelServiceNameSuffix)
	assert.Equal(t, expectedResult, actualResult)
}

// Test The TrimKafkaChannelServiceNameSuffix() Functionality
func TestTrimKafkaChannelServiceNameSuffix(t *testing.T) {

	// Test Data
	channelName := "TestChannelName"
	channelServiceName := fmt.Sprintf("%s-%s", channelName, constants.KafkaChannelServiceNameSuffix)

	// Perform The Test
	actualResult := TrimKafkaChannelServiceNameSuffix(channelServiceName)

	// Verify The Results
	expectedResult := channelName
	assert.Equal(t, expectedResult, actualResult)
}
