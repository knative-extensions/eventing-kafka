/*
Copyright 2021 The Knative Authors

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

package commands

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewConsumerGroupAsyncCommand(t *testing.T) {

	// Test Data
	commandId := int64(1234)
	topicName := "TestTopicName"
	groupId := "TestGroupId"

	// Define The Test Cases
	tests := []struct {
		name string
		lock *CommandLock
	}{
		{
			name: "With Lock",
			lock: NewCommandLock("TestLockToken", 1*time.Minute, true, true),
		},
		{
			name: "Without Lock",
			lock: nil,
		},
	}

	// Execute The Test Cases
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {

			// Perform The Test
			consumerGroupAsyncCommand := NewConsumerGroupAsyncCommand(commandId, topicName, groupId, test.lock)

			// Verify The Results
			assert.NotNil(t, consumerGroupAsyncCommand)
			assert.Equal(t, ConsumerGroupAsyncCommandVersion, consumerGroupAsyncCommand.Version)
			assert.Equal(t, commandId, consumerGroupAsyncCommand.CommandId)
			assert.Equal(t, topicName, consumerGroupAsyncCommand.TopicName)
			assert.Equal(t, groupId, consumerGroupAsyncCommand.GroupId)
			assert.Equal(t, test.lock, consumerGroupAsyncCommand.Lock)
		})
	}
}

func TestConsumerGroupAsyncCommand_MarshalUnmarshal(t *testing.T) {

	// Test Data
	commandId := int64(1234)
	topicName := "TestTopicName"
	groupId := "TestGroupId"

	// Define The Test Cases
	tests := []struct {
		name string
		lock *CommandLock
	}{
		{
			name: "With Lock",
			lock: NewCommandLock("TestLockToken", 1*time.Minute, true, true),
		},
		{
			name: "Without Lock",
			lock: nil,
		},
	}

	// Execute The Test Cases
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {

			// Create A ConsumerGroupAsyncCommand To Test
			origConsumerGroupAsyncCommand := NewConsumerGroupAsyncCommand(commandId, topicName, groupId, test.lock)

			// Perform The Test (Marshal & Unmarshal Round Trip)
			binaryData, err := origConsumerGroupAsyncCommand.MarshalBinary()
			assert.Nil(t, err)
			newConsumerGroupAsyncCommand := &ConsumerGroupAsyncCommand{}
			err = newConsumerGroupAsyncCommand.UnmarshalBinary(binaryData)
			assert.Nil(t, err)

			// Verify The Results
			assert.Equal(t, ConsumerGroupAsyncCommandVersion, newConsumerGroupAsyncCommand.Version)
			assert.Equal(t, origConsumerGroupAsyncCommand.CommandId, newConsumerGroupAsyncCommand.CommandId)
			assert.Equal(t, origConsumerGroupAsyncCommand.TopicName, newConsumerGroupAsyncCommand.TopicName)
			assert.Equal(t, origConsumerGroupAsyncCommand.GroupId, newConsumerGroupAsyncCommand.GroupId)
			assert.Equal(t, origConsumerGroupAsyncCommand.Lock, newConsumerGroupAsyncCommand.Lock)
		})
	}
}

func TestConsumerGroupAsyncCommand_SerializedId(t *testing.T) {

	// Create A ConsumerGroupAsyncCommand To Test
	consumerGroupAsyncCommand := NewConsumerGroupAsyncCommand(int64(1234), "TestTopicName", "TestGroupId", nil)

	// Perform The Test
	serializedId := consumerGroupAsyncCommand.SerializedId()

	// Verify The Results (1234 serialized)
	assert.Equal(t, []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x4, 0xd2}, serializedId)
}
