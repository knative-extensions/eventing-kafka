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
	"fmt"

	"go.uber.org/zap"
	ctrl "knative.dev/control-protocol/pkg"
	"knative.dev/control-protocol/pkg/network"
	ctrlservice "knative.dev/control-protocol/pkg/service"
	"knative.dev/pkg/logging"
)

type AsyncHandlerFunc func(ctx context.Context, commandMessage ctrlservice.AsyncCommandMessage)

type ServerHandler interface {
	Stop()
	AddAsyncHandler(opcode ctrl.OpCode, handler AsyncHandlerFunc)
	AddSyncHandler(opcode ctrl.OpCode, handler ctrl.MessageHandlerFunc)
	RemoveHandler(opcode ctrl.OpCode)
}

// kafkaConsumerGroupManagerImpl is the primary implementation of a KafkaConsumerGroupManager
type serverHandlerImpl struct {
	router       ctrlservice.MessageRouter
	server       *network.ControlServer
	cancelServer context.CancelFunc
}

func NewServerHandler() (ServerHandler, error) {
	logger, _ := zap.NewDevelopment()
	ctx := logging.WithLogger(context.TODO(), logger.Sugar())
	serverCtx, serverCancelFn := context.WithCancel(ctx)

	controlServer, err := network.StartInsecureControlServer(serverCtx, network.WithPort(9999))

	if err != nil {
		logger.Error("Failed to start control-protocol server", zap.Error(err))
	}

	messageRouter := make(ctrlservice.MessageRouter)
	controlServer.MessageHandler(messageRouter)

	return serverHandlerImpl{
		server:       controlServer,
		router:       messageRouter,
		cancelServer: serverCancelFn,
	}, nil
}

func (s serverHandlerImpl) Stop() {
	// Cancel the server and wait for it to stop
	s.cancelServer()
	<-s.server.ClosedCh()
}

func (s serverHandlerImpl) AddAsyncHandler(opcode ctrl.OpCode, handler AsyncHandlerFunc) {
	fmt.Printf("EDV: AddAsyncHandler(%d)\n", opcode)
	s.router[opcode] = ctrlservice.NewAsyncCommandHandler(s.server, &eventingKafkaAsyncCommand{}, opcode, handler)
}

func (s serverHandlerImpl) AddSyncHandler(opcode ctrl.OpCode, handler ctrl.MessageHandlerFunc) {
	fmt.Printf("EDV: AddSyncHandler(%d)\n", opcode)
	s.router[opcode] = handler
}

func (s serverHandlerImpl) RemoveHandler(opcode ctrl.OpCode) {
	delete(s.router, opcode)
}

var _ ServerHandler = (*serverHandlerImpl)(nil)

type eventingKafkaAsyncCommand struct {
}

func (eventingKafkaAsyncCommand) MarshalBinary() (data []byte, err error) {
	return []byte("unimplemented"), nil
}

func (eventingKafkaAsyncCommand) UnmarshalBinary(_ []byte) error {
	return nil
}

func (eventingKafkaAsyncCommand) SerializedId() []byte {
	return []byte("unimplementedId")
}
