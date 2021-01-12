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

package util

import (
	"errors"
	"fmt"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
)

// Test The PromoteErrorToTopicError() Functionality
func TestPromoteErrorToTopicError(t *testing.T) {

	// Test Nil Error
	topicError := PromoteErrorToTopicError(nil)
	assert.Nil(t, topicError)

	// Test Unknown Error Message
	unknownErrorMessage := "UnknownErrorMessage"
	topicError = PromoteErrorToTopicError(errors.New(unknownErrorMessage))
	assert.NotNil(t, topicError)
	assert.Equal(t, sarama.ErrUnknown, topicError.Err)
	assert.Equal(t, unknownErrorMessage, *topicError.ErrMsg)

	// Test Valid KError Message Promotion
	assert.True(t, minKError < maxKError)
	for kError := minKError; kError <= maxKError; kError++ {
		topicError = PromoteErrorToTopicError(errors.New(kError.Error()))
		assert.NotNil(t, topicError)
		assert.Equal(t, kError, topicError.Err)
		assert.Equal(t, "Promoted To TopicError Based On Error Message Match", *topicError.ErrMsg)
	}

	// Test Invalid KError Message Promotion
	invalidKError := maxKError + 1
	topicError = PromoteErrorToTopicError(invalidKError)
	assert.NotNil(t, topicError)
	assert.Equal(t, sarama.ErrUnknown, topicError.Err)
	assert.Equal(t, fmt.Sprint("Unknown error, how did this happen? Error code = ", int(invalidKError)), *topicError.ErrMsg)

	// Test Valid TopicError
	topicErrorMessage := "TopicErrorMessage"
	topicError = PromoteErrorToTopicError(&sarama.TopicError{Err: sarama.ErrInvalidConfig, ErrMsg: &topicErrorMessage})
	assert.NotNil(t, topicError)
	assert.Equal(t, sarama.ErrInvalidConfig, topicError.Err)
	assert.Equal(t, topicErrorMessage, *topicError.ErrMsg)
}

// Test The NewUnknownTopicError() Functionality
func TestNewUnknownTopicError(t *testing.T) {

	// Test Data
	errMsg := "test error message"

	// Perform The Test
	topicError := NewUnknownTopicError(errMsg)

	// Verify The Results
	assert.NotNil(t, topicError)
	assert.Equal(t, sarama.ErrUnknown, topicError.Err)
	assert.Equal(t, errMsg, *topicError.ErrMsg)
}

// Test The NewTopicError() Functionality
func TestNewTopicError(t *testing.T) {

	// Test Data
	errMsg := "test error message"

	// Perform The Test
	topicError := NewTopicError(sarama.ErrInvalidConfig, errMsg)

	// Verify The Results
	assert.NotNil(t, topicError)
	assert.Equal(t, sarama.ErrInvalidConfig, topicError.Err)
	assert.Equal(t, errMsg, *topicError.ErrMsg)
}
