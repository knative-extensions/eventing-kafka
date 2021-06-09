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

	"github.com/stretchr/testify/assert"
)

func TestNewConsumerGroupAsyncCommand(t *testing.T) {

	// Test Data
	commandId := int64(1234)
	topicName := "TestTopicName"
	groupId := "TestGroupId"

	// Perform The Test
	consumerGroupAsyncCommand := NewConsumerGroupAsyncCommand(commandId, topicName, groupId)

	// Verify The Results
	assert.NotNil(t, consumerGroupAsyncCommand)
	assert.Equal(t, ConsumerGroupAsyncCommandVersion, consumerGroupAsyncCommand.Version)
	assert.Equal(t, commandId, consumerGroupAsyncCommand.CommandId)
	assert.Equal(t, topicName, consumerGroupAsyncCommand.TopicName)
	assert.Equal(t, groupId, consumerGroupAsyncCommand.GroupId)
}

func TestConsumerGroupAsyncCommand_MarshalUnmarshal(t *testing.T) {

	// Create A ConsumerGroupAsyncCommand To Test
	origConsumerGroupAsyncCommand := NewConsumerGroupAsyncCommand(int64(1234), "TestTopicName", "TestGroupId")

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
}

func TestConsumerGroupAsyncCommand_SerializedId(t *testing.T) {

	// Create A ConsumerGroupAsyncCommand To Test
	consumerGroupAsyncCommand := NewConsumerGroupAsyncCommand(int64(1234), "TestTopicName", "TestGroupId1")

	// Perform The Test
	serializedId := consumerGroupAsyncCommand.SerializedId()

	// Verify The Results (1234 serialized)
	assert.Equal(t, []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x4, 0xd2}, serializedId)
}
