package test

import (
	"github.com/Shopify/sarama"
	"github.com/cloudevents/sdk-go/v2/binding"
	"github.com/stretchr/testify/assert"
	"testing"
)

// Utility Function For Creating A CloudEvents sdk-go BindingMessage
func CreateBindingMessage(cloudEventVersion string) binding.Message {
	return binding.ToMessage(CreateCloudEvent(cloudEventVersion))
}

// Utility Function For Validating A Kafka Message Header
func ValidateProducerMessageHeader(t *testing.T, headers []sarama.RecordHeader, headerKey string, headerValue string) {
	found := false
	assert.NotNil(t, headers)
	if len(headerKey) > 0 {
		for _, header := range headers {
			assert.NotNil(t, header)
			if string(header.Key) == headerKey {
				assert.Equal(t, headerValue, string(header.Value))
				found = true
				break
			}
		}
	}
	assert.True(t, found)
}
