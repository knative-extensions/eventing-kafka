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
	"github.com/Shopify/sarama"
)

// Constants
const (
	// Note - Update These When Sarama KErrors Change
	minKError = sarama.ErrUnknown
	maxKError = sarama.ErrFencedInstancedId
)

// Utility Function To Up-Convert Basic Errors Into TopicErrors (With Message Matching For Pertinent Errors)
func PromoteErrorToTopicError(err error) *sarama.TopicError {
	if err == nil {
		return nil
	} else {
		switch err := err.(type) {
		case *sarama.TopicError:
			return err
		default:
			for kError := minKError; kError <= maxKError; kError++ {
				if err.Error() == kError.Error() {
					return NewTopicError(kError, "Promoted To TopicError Based On Error Message Match")
				}
			}
			return NewTopicError(sarama.ErrUnknown, err.Error())
		}
	}
}

// Utility Function For Creating A New ErrUnknownTopicError With Specified Message
func NewUnknownTopicError(message string) *sarama.TopicError {
	return NewTopicError(sarama.ErrUnknown, message)
}

// Utility Function For Creating A Sarama TopicError With Specified Kafka Error Code (ErrNoError == Success)
func NewTopicError(kError sarama.KError, message string) *sarama.TopicError {
	return &sarama.TopicError{
		Err:    kError,
		ErrMsg: &message,
	}
}
