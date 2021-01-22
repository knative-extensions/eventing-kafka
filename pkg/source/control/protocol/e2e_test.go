package protocol

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"knative.dev/pkg/logging"
)

func TestStartClientAndServer(t *testing.T) {
	ctx, cancelFn := context.WithCancel(context.TODO())
	t.Cleanup(cancelFn)

	_, err := StartControlServer(ctx)
	require.NoError(t, err)
	_, err = StartControlClient(ctx, "localhost")
	require.NoError(t, err)
}

func TestE2EServerToClient(t *testing.T) {
	ctx, cancelFn := context.WithCancel(context.TODO())
	t.Cleanup(cancelFn)

	server, err := StartControlServer(ctx)
	require.NoError(t, err)

	server.ErrorHandler(ErrorHandlerFunc(func(ctx context.Context, err error) {
		require.NoError(t, err)
	}))

	client, err := StartControlClient(ctx, "localhost")
	require.NoError(t, err)

	wg := sync.WaitGroup{}
	wg.Add(1)

	client.ErrorHandler(ErrorHandlerFunc(func(ctx context.Context, err error) {
		require.NoError(t, err)
	}))
	client.InboundMessageHandler(ControlMessageHandlerFunc(func(ctx context.Context, message ControlMessage) {
		require.Equal(t, uint8(1), message.Headers().OpCode())
		require.Equal(t, "Funky!", string(message.Payload()))
		message.Ack()
		wg.Done()
	}))

	require.NoError(t, server.SendBinaryAndWaitForAck(1, []byte("Funky!")))

	wg.Wait()
}

func TestE2EClientToServer(t *testing.T) {
	ctx, cancelFn := context.WithCancel(context.TODO())
	t.Cleanup(cancelFn)

	server, err := StartControlServer(ctx)
	require.NoError(t, err)
	client, err := StartControlClient(ctx, "localhost")
	require.NoError(t, err)

	client.ErrorHandler(ErrorHandlerFunc(func(ctx context.Context, err error) {
		require.NoError(t, err)
	}))

	wg := sync.WaitGroup{}
	wg.Add(1)

	server.ErrorHandler(ErrorHandlerFunc(func(ctx context.Context, err error) {
		require.NoError(t, err)
	}))
	server.InboundMessageHandler(ControlMessageHandlerFunc(func(ctx context.Context, message ControlMessage) {
		require.Equal(t, uint8(1), message.Headers().OpCode())
		require.Equal(t, "Funky!", string(message.Payload()))
		message.Ack()
		wg.Done()
	}))

	require.NoError(t, client.SendBinaryAndWaitForAck(1, []byte("Funky!")))

	wg.Wait()
}

func TestE2EServerToClientAndBack(t *testing.T) {
	ctx, cancelFn := context.WithCancel(context.TODO())
	t.Cleanup(cancelFn)

	server, err := StartControlServer(ctx)
	require.NoError(t, err)

	client, err := StartControlClient(ctx, "localhost")
	require.NoError(t, err)

	wg := sync.WaitGroup{}
	wg.Add(6)

	server.ErrorHandler(ErrorHandlerFunc(func(ctx context.Context, err error) {
		require.NoError(t, err)
	}))
	client.ErrorHandler(ErrorHandlerFunc(func(ctx context.Context, err error) {
		require.NoError(t, err)
	}))

	server.InboundMessageHandler(ControlMessageHandlerFunc(func(ctx context.Context, message ControlMessage) {
		require.Equal(t, uint8(2), message.Headers().OpCode())
		require.Equal(t, "Funky2!", string(message.Payload()))
		message.Ack()
		wg.Done()
	}))
	client.InboundMessageHandler(ControlMessageHandlerFunc(func(ctx context.Context, message ControlMessage) {
		require.Equal(t, uint8(1), message.Headers().OpCode())
		require.Equal(t, "Funky!", string(message.Payload()))
		message.Ack()
		wg.Done()
	}))

	require.NoError(t, server.SendBinaryAndWaitForAck(1, []byte("Funky!")))
	require.NoError(t, client.SendBinaryAndWaitForAck(2, []byte("Funky2!")))
	require.NoError(t, server.SendBinaryAndWaitForAck(1, []byte("Funky!")))
	require.NoError(t, client.SendBinaryAndWaitForAck(2, []byte("Funky2!")))
	require.NoError(t, server.SendBinaryAndWaitForAck(1, []byte("Funky!")))
	require.NoError(t, client.SendBinaryAndWaitForAck(2, []byte("Funky2!")))

	wg.Wait()
}

func TestE2EClientToServerWithClientStop(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	ctx := logging.WithLogger(context.TODO(), logger.Sugar())
	clientCtx, clientCancelFn := context.WithCancel(ctx)
	serverCtx, serverCancelFn := context.WithCancel(ctx)
	t.Cleanup(clientCancelFn)
	t.Cleanup(serverCancelFn)

	server, err := StartControlServer(serverCtx)
	require.NoError(t, err)
	client, err := StartControlClient(clientCtx, "localhost")
	require.NoError(t, err)

	client.ErrorHandler(ErrorHandlerFunc(func(ctx context.Context, err error) {
		require.NoError(t, err)
	}))

	wg := sync.WaitGroup{}
	wg.Add(2)

	server.ErrorHandler(ErrorHandlerFunc(func(ctx context.Context, err error) {
		require.NoError(t, err)
	}))
	server.InboundMessageHandler(ControlMessageHandlerFunc(func(ctx context.Context, message ControlMessage) {
		require.Equal(t, uint8(1), message.Headers().OpCode())
		require.Equal(t, "Funky!", string(message.Payload()))
		message.Ack()
		wg.Done()
	}))

	// Send a message, close the client, restart it and send another message

	require.NoError(t, client.SendBinaryAndWaitForAck(1, []byte("Funky!")))

	clientCancelFn()

	time.Sleep(1 * time.Second)

	clientCtx2, clientCancelFn2 := context.WithCancel(ctx)
	t.Cleanup(clientCancelFn2)
	client2, err := StartControlClient(clientCtx2, "localhost")
	require.NoError(t, err)

	client2.ErrorHandler(ErrorHandlerFunc(func(ctx context.Context, err error) {
		require.NoError(t, err)
	}))

	require.NoError(t, client2.SendBinaryAndWaitForAck(1, []byte("Funky!")))

	wg.Wait()
}

func TestE2EClientToServerWithServerStop(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	ctx := logging.WithLogger(context.TODO(), logger.Sugar())
	clientCtx, clientCancelFn := context.WithCancel(ctx)
	serverCtx, serverCancelFn := context.WithCancel(ctx)
	t.Cleanup(clientCancelFn)
	t.Cleanup(serverCancelFn)

	server, err := StartControlServer(serverCtx)
	require.NoError(t, err)
	client, err := StartControlClient(clientCtx, "localhost")
	require.NoError(t, err)

	client.ErrorHandler(ErrorHandlerFunc(func(ctx context.Context, err error) {
		require.NoError(t, err)
	}))

	var wg sync.WaitGroup
	wg.Add(2)

	server.ErrorHandler(ErrorHandlerFunc(func(ctx context.Context, err error) {
		require.NoError(t, err)
	}))
	server.InboundMessageHandler(ControlMessageHandlerFunc(func(ctx context.Context, message ControlMessage) {
		require.Equal(t, uint8(1), message.Headers().OpCode())
		require.Equal(t, "Funky!", string(message.Payload()))
		message.Ack()
		wg.Done()
	}))

	// Send a message, close the server, restart it and send another message

	require.NoError(t, client.SendBinaryAndWaitForAck(1, []byte("Funky!")))

	serverCancelFn()

	time.Sleep(500 * time.Millisecond)

	serverCtx2, serverCancelFn2 := context.WithCancel(ctx)
	t.Cleanup(serverCancelFn2)
	server2, err := StartControlServer(serverCtx2)
	require.NoError(t, err)

	server2.ErrorHandler(ErrorHandlerFunc(func(ctx context.Context, err error) {
		require.NoError(t, err)
	}))
	server2.InboundMessageHandler(ControlMessageHandlerFunc(func(ctx context.Context, message ControlMessage) {
		require.Equal(t, uint8(1), message.Headers().OpCode())
		require.Equal(t, "Funky!", string(message.Payload()))
		message.Ack()
		wg.Done()
	}))

	require.NoError(t, client.SendBinaryAndWaitForAck(1, []byte("Funky!")))

	wg.Wait()
}

func TestE2ETryToBreak(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	ctx := logging.WithLogger(context.TODO(), logger.Sugar())
	ctx, cancelFn := context.WithCancel(ctx)
	t.Cleanup(cancelFn)

	server, err := StartControlServer(ctx)
	require.NoError(t, err)

	client, err := StartControlClient(ctx, "localhost")
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(1000)

	server.ErrorHandler(ErrorHandlerFunc(func(ctx context.Context, err error) {
		require.NoError(t, err)
	}))
	client.ErrorHandler(ErrorHandlerFunc(func(ctx context.Context, err error) {
		require.NoError(t, err)
	}))

	processed := atomic.NewInt32(0)

	server.InboundMessageHandler(ControlMessageHandlerFunc(func(ctx context.Context, message ControlMessage) {
		require.Equal(t, uint8(2), message.Headers().OpCode())
		require.Equal(t, "Funky2!", string(message.Payload()))
		message.Ack()
		wg.Done()
		processed.Inc()
	}))
	client.InboundMessageHandler(ControlMessageHandlerFunc(func(ctx context.Context, message ControlMessage) {
		require.Equal(t, uint8(1), message.Headers().OpCode())
		require.Equal(t, "Funky!", string(message.Payload()))
		message.Ack()
		wg.Done()
		processed.Inc()
	}))

	for i := 0; i < 1000; i++ {
		if i%2 == 0 {
			go func() {
				require.NoError(t, server.SendBinaryAndWaitForAck(1, []byte("Funky!")))
			}()
		} else {
			go func() {
				require.NoError(t, client.SendBinaryAndWaitForAck(2, []byte("Funky2!")))
			}()
		}
	}

	wg.Wait()

	logger.Sugar().Infof("Processed: %d", processed.Load())
}
