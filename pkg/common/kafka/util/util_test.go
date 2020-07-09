package util

import (
	"crypto/tls"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
	"knative.dev/eventing-kafka/pkg/common/kafka/constants"
	"testing"
)

// Test The AddAddSaslAuthentication() Functionality
func TestAddSaslAuthentication(t *testing.T) {

	// Test Data
	username := "TestUsername"
	password := "TestPassword"
	config := sarama.NewConfig()

	// Perform The Test
	AddSaslAuthentication(config, username, password)

	// Verify The Results
	assert.Equal(t, username, config.Net.SASL.User)
	assert.Equal(t, password, config.Net.SASL.Password)
	assert.True(t, config.Net.SASL.Enable)
	assert.Equal(t, sarama.SASLMechanism(sarama.SASLTypePlaintext), config.Net.SASL.Mechanism)
	assert.True(t, config.Net.TLS.Enable)
	assert.True(t, config.Net.TLS.Config.InsecureSkipVerify)
	assert.Equal(t, tls.NoClientCert, config.Net.TLS.Config.ClientAuth)
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
