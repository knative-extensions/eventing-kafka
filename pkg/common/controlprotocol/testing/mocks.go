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

package testing

import (
	ctrl "knative.dev/control-protocol/pkg"
	"knative.dev/control-protocol/pkg/message"
	ctrlservice "knative.dev/control-protocol/pkg/service"
	"knative.dev/eventing-kafka/pkg/common/controlprotocol"
	commontesting "knative.dev/eventing-kafka/pkg/common/testing"
)

//
// Mock Control-Protocol ServerHandler
//

// MockServerHandler is a mock of the ServerHandler that only stores results from AddAsyncHandler
type MockServerHandler struct {
	// Export the Router field so that tests can verify handlers in it directly
	Router ctrlservice.MessageRouter
}

var _ controlprotocol.ServerHandler = (*MockServerHandler)(nil)

func (s *MockServerHandler) AddAsyncHandler(opcode ctrl.OpCode, resultOpcode ctrl.OpCode, payloadType message.AsyncCommand, handler controlprotocol.AsyncHandlerFunc) {
	s.Router[opcode] = ctrlservice.NewAsyncCommandHandler(&commontesting.MockControlProtocolService{}, payloadType, resultOpcode, handler)
}

func (s *MockServerHandler) Shutdown()                                               {}
func (s *MockServerHandler) AddSyncHandler(_ ctrl.OpCode, _ ctrl.MessageHandlerFunc) {}
func (s *MockServerHandler) RemoveHandler(_ ctrl.OpCode)                             {}

func GetMockServerHandler() *MockServerHandler {
	return &MockServerHandler{Router: make(ctrlservice.MessageRouter)}
}
