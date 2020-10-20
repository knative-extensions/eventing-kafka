/*
Copyright 2020 The Knative Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package testing

import (
	"testing"

	"github.com/Shopify/sarama"
	"github.com/cloudevents/sdk-go/v2/binding"
	"github.com/stretchr/testify/assert"
)

// Utility Function For Creating A CloudEvents sdk-go BindingMessage
func CreateBindingMessage(cloudEventVersion string) binding.Message {
	return binding.ToMessage(CreateCloudEvent(cloudEventVersion))
}

// Utility Function For Validating A Kafka Message Header Is Present With Specified Value
func ValidateProducerMessageHeader(t *testing.T, headers []sarama.RecordHeader, headerKey string, headerValue string) {
	header := GetProducerMessageHeader(t, headers, headerKey)
	assert.NotNil(t, header)
	assert.Equal(t, headerKey, string(header.Key))
	assert.Equal(t, headerValue, string(header.Value))
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
