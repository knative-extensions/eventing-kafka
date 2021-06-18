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
	"context"
	"encoding"

	"github.com/stretchr/testify/mock"
	ctrl "knative.dev/control-protocol/pkg"
	"knative.dev/control-protocol/pkg/message"
	ctrlservice "knative.dev/control-protocol/pkg/service"
)

//
// Mock Control-Protocol ServerHandler
//

// MockServerHandler is a mock of the ServerHandler that only stores results from AddAsyncHandler
type MockServerHandler struct {
	mock.Mock
	// Export the Router field so that tests can verify handlers in it directly
	Router  ctrlservice.MessageRouter
	Service *MockControlProtocolService
}

func (s *MockServerHandler) AddAsyncHandler(opcode ctrl.OpCode, resultOpcode ctrl.OpCode, payloadType message.AsyncCommand,
	handler func(ctx context.Context, commandMessage ctrlservice.AsyncCommandMessage)) {
	_ = s.Called(opcode, resultOpcode, payloadType, handler)
	s.Router[opcode] = ctrlservice.NewAsyncCommandHandler(s.Service, payloadType, resultOpcode, handler)
}

func (s *MockServerHandler) Shutdown() {
	_ = s.Called()
}

func (s *MockServerHandler) AddSyncHandler(opcode ctrl.OpCode, handler ctrl.MessageHandlerFunc) {
	_ = s.Called(opcode, handler)
}

func (s *MockServerHandler) RemoveHandler(opcode ctrl.OpCode) {
	_ = s.Called(opcode)
}

func GetMockServerHandler() *MockServerHandler {
	return &MockServerHandler{
		Router:  make(ctrlservice.MessageRouter),
		Service: &MockControlProtocolService{},
	}
}

//
// Mock Control-Protocol Service
//

// MockControlProtocolService is a stub-only mock of the Control Protocol Service
type MockControlProtocolService struct {
	mock.Mock
}

var _ ctrl.Service = (*MockControlProtocolService)(nil)

func (m *MockControlProtocolService) SendAndWaitForAck(opcode ctrl.OpCode, payload encoding.BinaryMarshaler) error {
	args := m.Called(opcode, payload)
	return args.Error(0)
}

func (m *MockControlProtocolService) MessageHandler(handler ctrl.MessageHandler) {
	_ = m.Called(handler)
}

func (m *MockControlProtocolService) ErrorHandler(handler ctrl.ErrorHandler) {
	_ = m.Called(handler)
}
