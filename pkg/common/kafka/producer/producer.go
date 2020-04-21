package producer

import (
	"errors"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"knative.dev/eventing-kafka/pkg/common/kafka/constants"
	"knative.dev/eventing-kafka/pkg/common/kafka/util"
)

// Confluent Client Doesn't Code To Interfaces Or Provide Mocks So We're Wrapping Our Usage Of The Producer For Testing
type ProducerInterface interface {
	String() string
	Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error
	Events() chan kafka.Event
	ProduceChannel() chan *kafka.Message
	Len() int
	Flush(timeoutMs int) int
	Close()
}

// Create A Kafka Producer (Optional Authentication)
func CreateProducer(brokers string, username string, password string) (ProducerInterface, error) {

	// Validate Parameters
	if len(brokers) <= 0 {
		return nil, errors.New("required parameters not provided")
	}

	// Create The Kafka Producer Configuration
	configMap := getBaseProducerConfigMap(brokers)

	// Append SASL Authentication If Specified
	if len(username) > 0 && len(password) > 0 {
		util.AddSaslAuthentication(configMap, constants.ConfigPropertySaslMechanismsPlain, username, password)
	}

	// Enable Kafka Producer Debug Logging (Debug Only - Do Not Commit Enabled!)
	// util.AddDebugFlags(configMap, "broker,topic,msg,protocol,security")

	// Create The Kafka Producer Via Wrapper & Return Results
	return NewProducerWrapper(configMap)
}

// Kafka Function Reference Variable To Facilitate Mocking In Unit Tests
var NewProducerWrapper = func(configMap *kafka.ConfigMap) (ProducerInterface, error) {
	return kafka.NewProducer(configMap)
}

// Utility Function For Returning The Base/Common Kafka ConfigMap (Values Shared By All Connections)
func getBaseProducerConfigMap(brokers string) *kafka.ConfigMap {
	return &kafka.ConfigMap{
		constants.ConfigPropertyBootstrapServers:      brokers,
		constants.ConfigPropertyPartitioner:           constants.ConfigPropertyPartitionerValue,
		constants.ConfigPropertyIdempotence:           constants.ConfigPropertyIdempotenceValue,
		constants.ConfigPropertyStatisticsInterval:    constants.ConfigPropertyStatisticsIntervalValue,
		constants.ConfigPropertySocketKeepAliveEnable: constants.ConfigPropertySocketKeepAliveEnableValue,
		constants.ConfigPropertyMetadataMaxAgeMs:      constants.ConfigPropertyMetadataMaxAgeMsValue,
		constants.ConfigPropertyRequestTimeoutMs:      constants.ConfigPropertyRequestTimeoutMsValue,
	}
}
