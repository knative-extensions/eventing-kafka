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
	"encoding/json"
	"fmt"

	ctrl "knative.dev/control-protocol/pkg"
	ctrlmessage "knative.dev/control-protocol/pkg/message"
)

const (
	ConsumerGroupAsyncCommandVersion int16 = 1 // Basic AsyncCommand Compatibility Check

	StopConsumerGroupOpCode        ctrl.OpCode = 10
	StopConsumerGroupResultOpCode  ctrl.OpCode = 11
	StartConsumerGroupOpCode       ctrl.OpCode = 12
	StartConsumerGroupResultOpCode ctrl.OpCode = 13
)

// Verify The ConsumerGroupAsyncCommand Implements The Control-Protocol AsyncCommand Interface
var _ ctrlmessage.AsyncCommand = (*ConsumerGroupAsyncCommand)(nil)

// ConsumerGroupAsyncCommand implements an AsyncCommand for handling ConsumerGroup related operations.
type ConsumerGroupAsyncCommand struct {
	Version   int16        `json:"version"`
	CommandId int64        `json:"commandId"`
	TopicName string       `json:"topicName"`
	GroupId   string       `json:"groupId"`
	Lock      *CommandLock `json:"lock,omitempty"`
}

// NewConsumerGroupAsyncCommand constructs and returns a new ConsumerGroupAsyncCommand.
func NewConsumerGroupAsyncCommand(commandId int64, topicName string, groupId string, lock *CommandLock) *ConsumerGroupAsyncCommand {
	return &ConsumerGroupAsyncCommand{
		Version:   ConsumerGroupAsyncCommandVersion, // Only One Version For Now - Validate Compatibility In Handler ; )
		CommandId: commandId,
		TopicName: topicName,
		GroupId:   groupId,
		Lock:      lock,
	}
}

// MarshalBinary implements the Control-Protocol AsyncCommand interface.
func (s *ConsumerGroupAsyncCommand) MarshalBinary() (data []byte, err error) {
	return json.Marshal(s)
}

// UnmarshalBinary implements the Control-Protocol AsyncCommand interface.
func (s *ConsumerGroupAsyncCommand) UnmarshalBinary(data []byte) error {
	fmt.Printf("EDV: Received:\n%s\n", string(data))
	return json.Unmarshal(data, &s)
}

// SerializedId implements the Control-Protocol AsyncCommand interface.
func (s *ConsumerGroupAsyncCommand) SerializedId() []byte {
	return ctrlmessage.Int64CommandId(s.CommandId)
}
