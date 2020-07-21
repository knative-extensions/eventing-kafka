package consumer

import (
	"crypto/tls"
	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
	"knative.dev/eventing-kafka/pkg/common/kafka/constants"
	kafkatesting "knative.dev/eventing-kafka/pkg/common/kafka/testing"
	"testing"
)

// Test The CreateConsumerGroup() Functionality
func TestCreateConsumerGroup(t *testing.T) {

	// Test Data
	clientId := "TestClientId"
	brokers := []string{"TestBroker"}
	groupId := "TestGroupId"
	username := "TestUsername"
	password := "TestPassword"

	// Replace The NewConsumerGroupWrapper With Mock For Testing & Restore After Test
	newConsumerGroupWrapperPlaceholder := NewConsumerGroupWrapper
	NewConsumerGroupWrapper = func(brokersArg []string, groupIdArg string, configArg *sarama.Config) (sarama.ConsumerGroup, error) {
		assert.Equal(t, brokers, brokersArg)
		assert.Equal(t, groupId, groupIdArg)
		assert.NotNil(t, configArg)
		assert.Equal(t, clientId, configArg.ClientID)
		assert.Equal(t, constants.ConfigKafkaVersion, configArg.Version)
		assert.Equal(t, constants.ConfigNetKeepAlive, configArg.Net.KeepAlive)
		assert.True(t, configArg.Consumer.Return.Errors)
		assert.Equal(t, username, configArg.Net.SASL.User)
		assert.Equal(t, password, configArg.Net.SASL.Password)
		assert.True(t, configArg.Net.SASL.Enable)
		assert.Equal(t, sarama.SASLMechanism(sarama.SASLTypePlaintext), configArg.Net.SASL.Mechanism)
		assert.True(t, configArg.Net.TLS.Enable)
		assert.False(t, configArg.Net.TLS.Config.InsecureSkipVerify)
		assert.Equal(t, tls.NoClientCert, configArg.Net.TLS.Config.ClientAuth)
		assert.Equal(t, constants.ConfigMetadataRefreshFrequency, configArg.Metadata.RefreshFrequency)
		assert.Equal(t, constants.ConfigConsumerOffsetsAutoCommitInterval, configArg.Consumer.Offsets.AutoCommit.Interval)
		assert.Equal(t, constants.ConfigConsumerOffsetsRetention, configArg.Consumer.Offsets.Retention)
		return kafkatesting.NewMockConsumerGroup(t), nil
	}
	defer func() {
		NewConsumerGroupWrapper = newConsumerGroupWrapperPlaceholder
	}()

	// Perform The Test
	consumerGroup, registry, err := CreateConsumerGroup(clientId, brokers, groupId, username, password)

	// Verify The Results
	assert.Nil(t, err)
	assert.NotNil(t, consumerGroup)
	assert.NotNil(t, registry)
}
