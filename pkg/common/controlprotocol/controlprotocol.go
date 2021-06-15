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

package controlprotocol

import (
	"context"

	"go.uber.org/zap"
	ctrl "knative.dev/control-protocol/pkg"
	"knative.dev/control-protocol/pkg/message"
	"knative.dev/control-protocol/pkg/network"
	ctrlservice "knative.dev/control-protocol/pkg/service"
	"knative.dev/pkg/logging"
)

type AsyncHandlerFunc func(ctx context.Context, commandMessage ctrlservice.AsyncCommandMessage)

// startServerWrapper wraps the Control Protocol initialization call to facilitate
// unit testing without needing to start live TCP servers
var startServerWrapper = network.StartInsecureControlServer

// ServerHandler defines the interface for adding and removing sync or async handlers from a
// control-protocol server
type ServerHandler interface {
	Shutdown()
	AddAsyncHandler(opcode ctrl.OpCode, resultOpcode ctrl.OpCode, payloadType message.AsyncCommand, handler AsyncHandlerFunc)
	AddSyncHandler(opcode ctrl.OpCode, handler ctrl.MessageHandlerFunc)
	RemoveHandler(opcode ctrl.OpCode)
}

// kafkaConsumerGroupManagerImpl is the primary implementation of a KafkaConsumerGroupManager
type serverHandlerImpl struct {
	router       ctrlservice.MessageRouter
	server       *network.ControlServer
	cancelServer context.CancelFunc
}

// Verify that the serverHandlerImpl implements the ServerHandler interface
var _ ServerHandler = (*serverHandlerImpl)(nil)

// NewServerHandler starts a control-protocol server on the specified port and returns
// the serverHandlerImpl as a ServerHandler interface
func NewServerHandler(ctx context.Context, port int) (ServerHandler, error) {
	logger := logging.FromContext(ctx)
	serverCtx, serverCancelFn := context.WithCancel(ctx)

	// Create a new control-protocol server that will listen on the given port
	controlServer, err := startServerWrapper(serverCtx, network.WithPort(port))

	if err != nil {
		logger.Error("Failed to start control-protocol server", zap.Error(err))
		serverCancelFn()
		return nil, err
	}

	messageRouter := make(ctrlservice.MessageRouter)
	controlServer.MessageHandler(messageRouter)

	return serverHandlerImpl{
		server:       controlServer,
		router:       messageRouter,
		cancelServer: serverCancelFn,
	}, nil
}

// Shutdown cancels the control-protocol server and wait for it to stop
func (s serverHandlerImpl) Shutdown() {
	s.cancelServer()
	waitChannelClosed(s.server.ClosedCh())
}

// waitChannelClosed waits for close() to be called on a channel, unless it is nil
// (a nil channel blocks forever)
func waitChannelClosed(channel <-chan struct{}) {
	if channel != nil {
		<-channel
	}
}

// AddAsyncHandler will add an async handler for the given opcode to the message router used by the
// control-protocol server, which will send a message using the resultOpcode when it finishes.
func (s serverHandlerImpl) AddAsyncHandler(opcode ctrl.OpCode, resultOpcode ctrl.OpCode, payloadType message.AsyncCommand, handler AsyncHandlerFunc) {
	s.router[opcode] = ctrlservice.NewAsyncCommandHandler(s.server, payloadType, resultOpcode, handler)
}

// AddSyncHandler will add a handler to the control-protocol server for the given opcode
func (s serverHandlerImpl) AddSyncHandler(opcode ctrl.OpCode, handler ctrl.MessageHandlerFunc) {
	s.router[opcode] = handler
}

// RemoveHandler will delete a handler (sync or async) from the internal message router
func (s serverHandlerImpl) RemoveHandler(opcode ctrl.OpCode) {
	delete(s.router, opcode)
}
