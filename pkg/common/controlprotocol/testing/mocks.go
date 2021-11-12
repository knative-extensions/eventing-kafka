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
	"time"

	"github.com/stretchr/testify/mock"
	"k8s.io/apimachinery/pkg/types"
	ctrl "knative.dev/control-protocol/pkg"
	ctrlmessage "knative.dev/control-protocol/pkg/message"
	ctrlreconciler "knative.dev/control-protocol/pkg/reconciler"
	ctrlservice "knative.dev/control-protocol/pkg/service"
)

//
// Mock Control-Protocol ConnectionPool
//

var _ ctrlreconciler.ControlPlaneConnectionPool = (*MockConnectionPool)(nil)

type MockConnectionPool struct {
	mock.Mock
}

func (c *MockConnectionPool) GetConnectedHosts(key string) []string {
	args := c.Called(key)
	return args.Get(0).([]string)
}

func (c *MockConnectionPool) GetServices(key string) map[string]ctrl.Service {
	args := c.Called(key)
	return args.Get(0).(map[string]ctrl.Service)
}

func (c *MockConnectionPool) ResolveControlInterface(key string, host string) (string, ctrl.Service) {
	args := c.Called(key, host)
	return args.String(0), args.Get(1).(ctrl.Service)
}

func (c *MockConnectionPool) RemoveConnection(ctx context.Context, key string, host string) {
	c.Called(ctx, key, host)
}

func (c *MockConnectionPool) RemoveAllConnections(ctx context.Context, key string) {
	c.Called(ctx, key)
}

func (c *MockConnectionPool) Close(ctx context.Context) {
	c.Called(ctx)
}

func (c *MockConnectionPool) ReconcileConnections(ctx context.Context, key string, wantConnections []string, newServiceCb func(string, ctrl.Service), oldServiceCb func(string)) (map[string]ctrl.Service, error) {
	args := c.Called(ctx, key, wantConnections, newServiceCb, oldServiceCb)
	return args.Get(0).(map[string]ctrl.Service), args.Error(1)
}

func (c *MockConnectionPool) DialControlService(ctx context.Context, key string, host string) (string, ctrl.Service, error) {
	args := c.Called(ctx, key, host)
	return args.String(0), args.Get(1).(ctrl.Service), args.Error(2)
}

//
// Mock Control-Protocol AsyncCommandNotificationStore
//

var _ ctrlreconciler.AsyncCommandNotificationStore = (*MockAsyncCommandNotificationStore)(nil)

type MockAsyncCommandNotificationStore struct {
	mock.Mock
}

func (a *MockAsyncCommandNotificationStore) GetCommandResult(srcName types.NamespacedName, pod string, command ctrlmessage.AsyncCommand) *ctrlmessage.AsyncCommandResult {
	args := a.Called(srcName, pod, command)
	return args.Get(0).(*ctrlmessage.AsyncCommandResult)
}

func (a *MockAsyncCommandNotificationStore) CleanPodsNotifications(srcName types.NamespacedName) {
	a.Called(srcName)
}

func (a *MockAsyncCommandNotificationStore) CleanPodNotification(srcName types.NamespacedName, pod string) {
	a.Called(srcName, pod)
}

func (a *MockAsyncCommandNotificationStore) MessageHandler(srcName types.NamespacedName, pod string) ctrl.MessageHandler {
	args := a.Called(srcName, pod)
	return args.Get(0).(ctrl.MessageHandler)
}

//
// Mock Control-Protocol Service
//

var _ ctrl.Service = (*MockService)(nil)

type MockService struct {
	mock.Mock
}

func (s *MockService) SendAndWaitForAck(opcode ctrl.OpCode, payload encoding.BinaryMarshaler) error {
	args := s.Called(opcode, payload)
	return args.Error(0)
}

func (s *MockService) MessageHandler(handler ctrl.MessageHandler) {
	s.Called(handler)
}

func (s *MockService) ErrorHandler(handler ctrl.ErrorHandler) {
	s.Called(handler)
}

//
// Mock Control-Protocol ServerHandler
//

// MockServerHandler is a mock of the ServerHandler that only stores results from AddAsyncHandler
type MockServerHandler struct {
	mock.Mock
	// Export the Router field so that tests can verify handlers in it directly
	Router  ctrlservice.MessageRouter
	Service *MockService
}

func (s *MockServerHandler) AddAsyncHandler(opcode ctrl.OpCode, resultOpcode ctrl.OpCode, payloadType ctrlmessage.AsyncCommand,
	handler func(ctx context.Context, commandMessage ctrlservice.AsyncCommandMessage)) {
	_ = s.Called(opcode, resultOpcode, payloadType, handler)
	s.Router[opcode] = ctrlservice.NewAsyncCommandHandler(s.Service, payloadType, resultOpcode, handler)
}

func (s *MockServerHandler) Shutdown(timeout time.Duration) {
	_ = s.Called(timeout)
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
		Service: &MockService{},
	}
}
