package test

import (
	"github.com/Shopify/sarama"
	"github.com/cloudevents/sdk-go/v2/binding"
	"github.com/stretchr/testify/assert"
	"testing"
)

// Utility Function For Creating A CloudEvents sdk-go BindingMessage
func CreateBindingMessage(cloudEventVersion string, partitionKey string) binding.Message {
	return binding.ToMessage(CreateCloudEvent(cloudEventVersion, partitionKey))
}

// Utility Function For Validating A Kafka Message Header Is Present With Specified Value
func ValidateProducerMessageHeaderValueEquality(t *testing.T, headers []sarama.RecordHeader, headerKey string, headerValue string) {
	header := GetProducerMessageHeader(t, headers, headerKey)
	assert.NotNil(t, header)
	assert.Equal(t, headerKey, string(header.Key))
	assert.Equal(t, headerValue, string(header.Value))
}

// Utility Function For Validating A Kafka Message Header Is Present With Some Value
func ValidateProducerMessageHeaderValuePresent(t *testing.T, headers []sarama.RecordHeader, headerKey string) {
	header := GetProducerMessageHeader(t, headers, headerKey)
	assert.NotNil(t, header)
	assert.Equal(t, headerKey, string(header.Key))
	assert.NotEmpty(t, header.Value)
}

// Utility Function For Acquiring A Kafka Message Header With The Specified Key
func GetProducerMessageHeader(t *testing.T, headers []sarama.RecordHeader, headerKey string) *sarama.RecordHeader {
	assert.NotNil(t, headers)
	if len(headerKey) > 0 {
		for _, header := range headers {
			assert.NotNil(t, header)
			if string(header.Key) == headerKey {
				return &header
			}
		}
	}
	return nil
}
