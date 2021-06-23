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
	"sync"
	"time"

	ctrl "knative.dev/control-protocol/pkg"
	"knative.dev/control-protocol/pkg/message"
	"knative.dev/control-protocol/pkg/network"
	ctrlservice "knative.dev/control-protocol/pkg/service"
)

const (
	// ServerPort is the port used when initializing the control-protocol server
	ServerPort = 8085
)

// AsyncHandlerFunc is an alias that matches the type used in the control-protocol's NewAsyncCommandHandler function
type AsyncHandlerFunc = func(ctx context.Context, commandMessage ctrlservice.AsyncCommandMessage)

// startServerWrapper wraps the Control Protocol initialization call to facilitate
// unit testing without needing to start live TCP servers
var startServerWrapper = network.StartInsecureControlServer

// ServerHandler defines the interface for adding and removing sync or async handlers from a
// control-protocol server
type ServerHandler interface {
	Shutdown(timeout time.Duration)
	AddAsyncHandler(opcode ctrl.OpCode, resultOpcode ctrl.OpCode, payloadType message.AsyncCommand, handler AsyncHandlerFunc)
	AddSyncHandler(opcode ctrl.OpCode, handler ctrl.MessageHandlerFunc)
	RemoveHandler(opcode ctrl.OpCode)
}

// serverHandlerImpl is the primary implementation of a ServerHandler
type serverHandlerImpl struct {
	router       ctrlservice.MessageRouter
	server       *network.ControlServer
	cancelServer context.CancelFunc
	routerLock   sync.RWMutex
}

// Verify that the serverHandlerImpl implements the ServerHandler interface
var _ ServerHandler = (*serverHandlerImpl)(nil)

// NewServerHandler starts a control-protocol server on the specified port and returns
// the serverHandlerImpl as a ServerHandler interface
func NewServerHandler(ctx context.Context, port int) (ServerHandler, error) {
	serverCtx, serverCancelFn := context.WithCancel(ctx)

	// Create a new control-protocol server that will listen on the given port
	controlServer, err := startServerWrapper(serverCtx, network.WithPort(port))

	if err != nil {
		serverCancelFn()
		return nil, err
	}

	serverHandler := &serverHandlerImpl{
		server:       controlServer,
		cancelServer: serverCancelFn,
		router:       make(ctrlservice.MessageRouter),
		routerLock:   sync.RWMutex{},
	}
	serverHandler.setHandler()
	return serverHandler, nil
}

// Shutdown cancels the control-protocol server and wait for it to stop
func (s *serverHandlerImpl) Shutdown(timeout time.Duration) {
	s.cancelServer()
	select {
	case <-s.server.ClosedCh():
	case <-time.After(timeout):
	}
}

// AddAsyncHandler will add an async handler for the given opcode to the message router used by the
// control-protocol server, which will send a message using the resultOpcode when it finishes.
func (s *serverHandlerImpl) AddAsyncHandler(opcode ctrl.OpCode, resultOpcode ctrl.OpCode, payloadType message.AsyncCommand, handler AsyncHandlerFunc) {
	s.routerLock.Lock()
	s.router[opcode] = ctrlservice.NewAsyncCommandHandler(s.server, payloadType, resultOpcode, handler)
	s.routerLock.Unlock()
	s.setHandler()
}

// AddSyncHandler will add a handler to the control-protocol server for the given opcode
func (s *serverHandlerImpl) AddSyncHandler(opcode ctrl.OpCode, handler ctrl.MessageHandlerFunc) {
	s.routerLock.Lock()
	s.router[opcode] = handler
	s.routerLock.Unlock()
	s.setHandler()
}

// RemoveHandler will delete a handler (sync or async) from the internal message router
func (s *serverHandlerImpl) RemoveHandler(opcode ctrl.OpCode) {
	s.routerLock.Lock()
	delete(s.router, opcode)
	s.routerLock.Unlock()
	s.setHandler()
}

// setHandler re-sets the MessageHandler on the internal control-protocol service to a copy of the router
func (s *serverHandlerImpl) setHandler() {
	// Invoke the MessageHandler on the control-protocol service with a copy of our router map, to avoid it being
	// changed in this ServerHandlerImpl while in active use as a MessageHandler.
	routerCopy := make(ctrlservice.MessageRouter, len(s.router))
	s.routerLock.RLock()
	for opcode := range s.router {
		routerCopy[opcode] = s.router[opcode]
	}
	s.routerLock.RUnlock()
	s.server.MessageHandler(routerCopy)
}
