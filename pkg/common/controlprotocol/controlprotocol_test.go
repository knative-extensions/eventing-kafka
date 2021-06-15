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
	"sync"
	"testing"

	commontesting "knative.dev/eventing-kafka/pkg/common/testing"

	"github.com/stretchr/testify/assert"
	ctrl "knative.dev/control-protocol/pkg"
	"knative.dev/control-protocol/pkg/network"
	ctrlservice "knative.dev/control-protocol/pkg/service"

	"knative.dev/eventing-kafka/pkg/common/controlprotocol/commands"
)

func TestNewServerHandler(t *testing.T) {

	saveStartServer := startServerWrapper
	defer func() { startServerWrapper = saveStartServer }()

	for _, testCase := range []struct {
		name      string
		serverErr error
	}{
		{
			name: "No Error",
		},
		{
			name:      "Server Error",
			serverErr: fmt.Errorf("test error"),
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			startServerWrapper = func(_ context.Context, _ ...network.ControlServerOption) (*network.ControlServer, error) {
				return &network.ControlServer{Service: commontesting.MockControlProtocolService{}}, testCase.serverErr
			}

			handler, err := NewServerHandler(context.Background(), 12345)
			if testCase.serverErr == nil {
				assert.NotNil(t, handler)
			}
			assert.Equal(t, testCase.serverErr, err)
		})
	}
}

func TestWaitChannelClosed(t *testing.T) {
	var c chan struct{}
	waitChannelClosed(c) // Null channel should not block
	c = make(chan struct{})
	waitGroup := sync.WaitGroup{}
	waitGroup.Add(1)
	go func() {
		waitChannelClosed(c)
		waitGroup.Done()
	}()
	close(c)
	waitGroup.Wait()
}

func TestHandlers(t *testing.T) {
	saveStartServer := startServerWrapper
	defer func() { startServerWrapper = saveStartServer }()

	startServerWrapper = func(_ context.Context, _ ...network.ControlServerOption) (*network.ControlServer, error) {
		return &network.ControlServer{Service: commontesting.MockControlProtocolService{}}, nil
	}
	handler, err := NewServerHandler(context.Background(), 12345)
	assert.NotNil(t, handler)
	assert.Nil(t, err)

	// Extract the unexported implementation of the ServerHandler for testing the router directly
	impl := handler.(serverHandlerImpl)

	handler.AddSyncHandler(ctrl.OpCode(1), func(ctx context.Context, message ctrl.ServiceMessage) {})
	assert.NotNil(t, impl.router[ctrl.OpCode(1)])

	handler.AddAsyncHandler(ctrl.OpCode(2), ctrl.OpCode(3), &commands.ConsumerGroupAsyncCommand{},
		func(ctx context.Context, commandMessage ctrlservice.AsyncCommandMessage) {})
	assert.NotNil(t, impl.router[ctrl.OpCode(2)])

	handler.RemoveHandler(ctrl.OpCode(1))
	handler.RemoveHandler(ctrl.OpCode(2))
	assert.Nil(t, impl.router[ctrl.OpCode(1)])
	assert.Nil(t, impl.router[ctrl.OpCode(2)])

	handler.Shutdown()
}
