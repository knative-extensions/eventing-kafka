package consumer

import (
	"strconv"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
	"knative.dev/eventing-kafka/pkg/common/kafka/constants"
	kafkasarama "knative.dev/eventing-kafka/pkg/common/kafka/sarama"
	kafkatesting "knative.dev/eventing-kafka/pkg/common/kafka/testing"
	commontesting "knative.dev/eventing-kafka/pkg/common/testing"
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
		assert.Equal(t, constants.ConfigKafkaVersionDefault, configArg.Version)
		assert.Equal(t, commontesting.ConfigNetKeepAlive, strconv.FormatInt(int64(configArg.Net.KeepAlive), 10))
		assert.True(t, configArg.Consumer.Return.Errors)
		assert.Equal(t, commontesting.ConfigMetadataRefreshFrequency, strconv.FormatInt(int64(configArg.Metadata.RefreshFrequency), 10))
		assert.Equal(t, commontesting.ConfigConsumerOffsetsAutoCommitInterval, strconv.FormatInt(int64(configArg.Consumer.Offsets.AutoCommit.Interval), 10))
		assert.Equal(t, commontesting.ConfigConsumerOffsetsRetention, strconv.FormatInt(int64(configArg.Consumer.Offsets.Retention), 10))
		return kafkatesting.NewMockConsumerGroup(t), nil
	}
	defer func() {
		NewConsumerGroupWrapper = newConsumerGroupWrapperPlaceholder
	}()

	// Perform The Test
	config := commontesting.GetDefaultSaramaConfig(t, kafkasarama.NewSaramaConfig())
	kafkasarama.UpdateSaramaConfig(config, ClientId, KafkaUsername, KafkaPassword)
	consumerGroup, registry, err := CreateConsumerGroup(brokers, config, GroupId)

	// Verify The Results
	assert.Nil(t, err)
	assert.NotNil(t, consumerGroup)
	assert.NotNil(t, registry)
}

// Test that the UpdateSaramaConfig sets values as expected
func TestUpdateConfig(t *testing.T) {
	config := sarama.NewConfig()
	kafkasarama.UpdateSaramaConfig(config, ClientId, KafkaUsername, KafkaPassword)
	assert.Equal(t, ClientId, config.ClientID)
	assert.Equal(t, KafkaUsername, config.Net.SASL.User)
	assert.Equal(t, KafkaPassword, config.Net.SASL.Password)
}
