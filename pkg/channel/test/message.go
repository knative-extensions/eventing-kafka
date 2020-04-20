package test

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/assert"
	"testing"
)

// Utility Function For Validating A Kafka Message Header
func ValidateKafkaMessageHeader(t *testing.T, headers []kafka.Header, headerKey string, headerValue string) {
	found := false
	assert.NotNil(t, headers)
	if len(headerKey) > 0 {
		for _, header := range headers {
			assert.NotNil(t, header)
			if header.Key == headerKey {
				assert.Equal(t, headerValue, string(header.Value))
				found = true
				break
			}
		}
	}
	assert.True(t, found)
}
