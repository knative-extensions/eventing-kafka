package consumer

import (
	"crypto/tls"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
	"knative.dev/eventing-kafka/pkg/common/kafka/constants"
	kafkatesting "knative.dev/eventing-kafka/pkg/common/kafka/testing"
)

// Test Constants
const (
	ClientId      = "TestClientId"
	GroupId       = "TestGrouId"
	KafkaUsername = "TestUsername"
	KafkaPassword = "TestPassword"
)

// Test The CreateConsumerGroup() Functionality
func TestCreateConsumerGroup(t *testing.T) {

	// Test Data
	brokers := []string{"TestBroker"}

	// Replace The NewConsumerGroupWrapper With Mock For Testing & Restore After Test
	newConsumerGroupWrapperPlaceholder := NewConsumerGroupWrapper
	NewConsumerGroupWrapper = func(brokersArg []string, groupIdArg string, configArg *sarama.Config) (sarama.ConsumerGroup, error) {
		assert.Equal(t, brokers, brokersArg)
		assert.Equal(t, GroupId, groupIdArg)
		assert.NotNil(t, configArg)
		assert.Equal(t, ClientId, configArg.ClientID)
		assert.Equal(t, KafkaUsername, configArg.Net.SASL.User)
		assert.Equal(t, KafkaPassword, configArg.Net.SASL.Password)
		assert.Equal(t, constants.ConfigKafkaVersion, configArg.Version)
		assert.Equal(t, constants.ConfigNetKeepAlive, configArg.Net.KeepAlive)
		assert.True(t, configArg.Consumer.Return.Errors)
		assert.True(t, configArg.Net.SASL.Enable)
		assert.Equal(t, sarama.SASLMechanism(sarama.SASLTypePlaintext), configArg.Net.SASL.Mechanism)
		assert.True(t, configArg.Net.TLS.Enable)
		assert.False(t, configArg.Net.TLS.Config.InsecureSkipVerify)
		assert.Equal(t, tls.NoClientCert, configArg.Net.TLS.Config.ClientAuth)
		assert.Equal(t, constants.ConfigMetadataRefreshFrequency, configArg.Metadata.RefreshFrequency)
		assert.Equal(t, constants.ConfigConsumerOffsetsAutoCommitInterval, configArg.Consumer.Offsets.AutoCommit.Interval)
		assert.Equal(t, constants.ConfigConsumerOffsetsRetention, configArg.Consumer.Offsets.Retention)
		assert.True(t, configArg.Consumer.Return.Errors)
		return kafkatesting.NewMockConsumerGroup(t), nil
	}
	defer func() {
		NewConsumerGroupWrapper = newConsumerGroupWrapperPlaceholder
	}()

	// Perform The Test
	config := sarama.NewConfig()
	UpdateConfig(config, ClientId, KafkaUsername, KafkaPassword)
	consumerGroup, registry, err := CreateConsumerGroup(brokers, config, GroupId)

	// Verify The Results
	assert.Nil(t, err)
	assert.NotNil(t, consumerGroup)
	assert.NotNil(t, registry)
}

func TestUpdateConfig(t *testing.T) {
	config := sarama.NewConfig()
	UpdateConfig(config, ClientId, KafkaUsername, KafkaPassword)
	assert.Equal(t, ClientId, config.ClientID)
	assert.Equal(t, KafkaUsername, config.Net.SASL.User)
	assert.Equal(t, KafkaPassword, config.Net.SASL.Password)
}
