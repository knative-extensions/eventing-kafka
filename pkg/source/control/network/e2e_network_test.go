package network

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"knative.dev/pkg/logging"

	"knative.dev/eventing-kafka/pkg/source/control/service"
)

type mockMessage string

func (m mockMessage) MarshalBinary() (data []byte, err error) {
	return []byte(m), nil
}

func TestTLSConf(t *testing.T) {
	serverTLSConf, clientTLSDialer := testTLSConf(t, context.TODO())
	require.NotNil(t, serverTLSConf)
	require.NotNil(t, clientTLSDialer)
}

func TestStartClientAndServer(t *testing.T) {
	_, _, _, _ = mustSetupWithTLS(t)
}

func TestServerToClient(t *testing.T) {
	_, server, _, client := mustSetupWithTLS(t)
	runSendReceiveTest(t, server, client)
}

func TestClientToServer(t *testing.T) {
	_, server, _, client := mustSetupWithTLS(t)
	runSendReceiveTest(t, client, server)
}

func TestNoopMessageHandlerAcks(t *testing.T) {
	_, server, _, _ := mustSetupWithTLS(t)
	require.NoError(t, server.SendAndWaitForAck(10, mockMessage("Funky!")))
}

func TestInsecureServerToClient(t *testing.T) {
	_, server, _, client := mustSetupInsecure(t)
	runSendReceiveTest(t, server, client)
}

func TestInsecureClientToServer(t *testing.T) {
	_, server, _, client := mustSetupInsecure(t)
	runSendReceiveTest(t, client, server)
}

func TestServerToClientAndBack(t *testing.T) {
	_, server, _, client := mustSetupWithTLS(t)

	wg := sync.WaitGroup{}
	wg.Add(6)

	server.InboundMessageHandler(service.ControlMessageHandlerFunc(func(ctx context.Context, message service.ControlMessage) {
		require.Equal(t, uint8(2), message.Headers().OpCode())
		require.Equal(t, "Funky2!", string(message.Payload()))
		message.Ack()
		wg.Done()
	}))
	client.InboundMessageHandler(service.ControlMessageHandlerFunc(func(ctx context.Context, message service.ControlMessage) {
		require.Equal(t, uint8(1), message.Headers().OpCode())
		require.Equal(t, "Funky!", string(message.Payload()))
		message.Ack()
		wg.Done()
	}))

	require.NoError(t, server.SendAndWaitForAck(1, mockMessage("Funky!")))
	require.NoError(t, client.SendAndWaitForAck(2, mockMessage("Funky2!")))
	require.NoError(t, server.SendAndWaitForAck(1, mockMessage("Funky!")))
	require.NoError(t, client.SendAndWaitForAck(2, mockMessage("Funky2!")))
	require.NoError(t, server.SendAndWaitForAck(1, mockMessage("Funky!")))
	require.NoError(t, client.SendAndWaitForAck(2, mockMessage("Funky2!")))

	wg.Wait()
}

func TestClientToServerWithClientStop(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	ctx := logging.WithLogger(context.TODO(), logger.Sugar())
	serverTLSConf, clientTLSDialer := testTLSConf(t, ctx)

	clientCtx, clientCancelFn := context.WithCancel(ctx)
	serverCtx, serverCancelFn := context.WithCancel(ctx)
	t.Cleanup(clientCancelFn)
	t.Cleanup(serverCancelFn)

	server, closedServerSignal, err := StartControlServer(serverCtx, serverTLSConf)
	require.NoError(t, err)
	t.Cleanup(func() {
		serverCancelFn()
		<-closedServerSignal
	})

	client, err := StartControlClient(clientCtx, clientTLSDialer, "localhost")
	require.NoError(t, err)

	wg := sync.WaitGroup{}
	wg.Add(2)

	server.InboundMessageHandler(service.ControlMessageHandlerFunc(func(ctx context.Context, message service.ControlMessage) {
		require.Equal(t, uint8(1), message.Headers().OpCode())
		require.Equal(t, "Funky!", string(message.Payload()))
		message.Ack()
		wg.Done()
	}))

	// Send a message, close the client, restart it and send another message

	require.NoError(t, client.SendAndWaitForAck(1, mockMessage("Funky!")))

	clientCancelFn()

	time.Sleep(1 * time.Second)

	clientCtx2, clientCancelFn2 := context.WithCancel(ctx)
	t.Cleanup(clientCancelFn2)
	client2, err := StartControlClient(clientCtx2, clientTLSDialer, "localhost")
	require.NoError(t, err)

	require.NoError(t, client2.SendAndWaitForAck(1, mockMessage("Funky!")))

	wg.Wait()
}

func TestClientToServerWithServerStop(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	ctx := logging.WithLogger(context.TODO(), logger.Sugar())
	serverTLSConf, clientTLSDialer := testTLSConf(t, ctx)

	clientCtx, clientCancelFn := context.WithCancel(ctx)
	serverCtx, serverCancelFn := context.WithCancel(ctx)
	t.Cleanup(clientCancelFn)
	t.Cleanup(serverCancelFn)

	server, closedServerSignal, err := StartControlServer(serverCtx, serverTLSConf)
	require.NoError(t, err)

	client, err := StartControlClient(clientCtx, clientTLSDialer, "localhost")
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(2)

	server.InboundMessageHandler(service.ControlMessageHandlerFunc(func(ctx context.Context, message service.ControlMessage) {
		require.Equal(t, uint8(1), message.Headers().OpCode())
		require.Equal(t, "Funky!", string(message.Payload()))
		message.Ack()
		wg.Done()
	}))

	// Send a message, close the server, restart it and send another message

	require.NoError(t, client.SendAndWaitForAck(1, mockMessage("Funky!")))

	serverCancelFn()

	<-closedServerSignal

	serverCtx2, serverCancelFn2 := context.WithCancel(ctx)
	server2, closedServerSignal, err := StartControlServer(serverCtx2, serverTLSConf)
	require.NoError(t, err)
	t.Cleanup(func() {
		serverCancelFn2()
		<-closedServerSignal
	})

	server2.InboundMessageHandler(service.ControlMessageHandlerFunc(func(ctx context.Context, message service.ControlMessage) {
		require.Equal(t, uint8(1), message.Headers().OpCode())
		require.Equal(t, "Funky!", string(message.Payload()))
		message.Ack()
		wg.Done()
	}))

	require.NoError(t, client.SendAndWaitForAck(1, mockMessage("Funky!")))

	wg.Wait()
}

func TestManyMessages(t *testing.T) {
	ctx, server, _, client := mustSetupWithTLS(t)

	var wg sync.WaitGroup
	wg.Add(1000 * 2)

	processed := atomic.NewInt32(0)

	server.InboundMessageHandler(service.ControlMessageHandlerFunc(func(ctx context.Context, message service.ControlMessage) {
		require.Equal(t, uint8(2), message.Headers().OpCode())
		require.Equal(t, "Funky2!", string(message.Payload()))
		message.Ack()
		wg.Done()
		processed.Inc()
	}))
	client.InboundMessageHandler(service.ControlMessageHandlerFunc(func(ctx context.Context, message service.ControlMessage) {
		require.Equal(t, uint8(1), message.Headers().OpCode())
		require.Equal(t, "Funky!", string(message.Payload()))
		message.Ack()
		wg.Done()
		processed.Inc()
	}))

	for i := 0; i < 1000; i++ {
		if i%2 == 0 {
			go func() {
				require.NoError(t, server.SendAndWaitForAck(1, mockMessage("Funky!")))
				wg.Done()
			}()
		} else {
			go func() {
				require.NoError(t, client.SendAndWaitForAck(2, mockMessage("Funky2!")))
				wg.Done()
			}()
		}
	}

	wg.Wait()

	logging.FromContext(ctx).Infof("Processed: %d", processed.Load())
}

func mustGenerateTLSServerConf(t *testing.T, certManager *CertificateManager) *tls.Config {
	dataPlaneKeyPair, err := certManager.EmitNewDataPlaneCertificate(context.TODO())
	require.NoError(t, err)

	dataPlaneCert, err := tls.X509KeyPair(dataPlaneKeyPair.CertBytes(), dataPlaneKeyPair.PrivateKeyBytes())
	require.NoError(t, err)

	certPool := x509.NewCertPool()
	certPool.AddCert(certManager.caCert)
	return &tls.Config{
		Certificates: []tls.Certificate{dataPlaneCert},
		ClientCAs:    certPool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ServerName:   certManager.CaCert().DNSNames[0],
	}
}

func testTLSConf(t *testing.T, ctx context.Context) (*tls.Config, *tls.Dialer) {
	cm, err := NewCertificateManager(ctx)
	require.NoError(t, err)

	serverTLSConf := mustGenerateTLSServerConf(t, cm)

	tlsDialer, err := cm.GenerateTLSDialer(&net.Dialer{
		KeepAlive: keepAlive,
		Deadline:  time.Time{},
	})
	require.NoError(t, err)
	return serverTLSConf, tlsDialer
}

func mustSetupWithTLS(t *testing.T) (serverCtx context.Context, server service.Service, clientCtx context.Context, client service.Service) {
	logger, _ := zap.NewDevelopment()
	ctx := logging.WithLogger(context.TODO(), logger.Sugar())
	serverTLSConf, clientTLSDialer := testTLSConf(t, ctx)

	clientCtx, clientCancelFn := context.WithCancel(ctx)
	serverCtx, serverCancelFn := context.WithCancel(ctx)

	server, closedServerSignal, err := StartControlServer(serverCtx, serverTLSConf)
	require.NoError(t, err)
	t.Cleanup(func() {
		serverCancelFn()
		<-closedServerSignal
	})

	client, err = StartControlClient(clientCtx, clientTLSDialer, "127.0.0.1")
	require.NoError(t, err)
	t.Cleanup(clientCancelFn)

	return serverCtx, server, clientCtx, client
}

func mustSetupInsecure(t *testing.T) (serverCtx context.Context, server service.Service, clientCtx context.Context, client service.Service) {
	logger, _ := zap.NewDevelopment()
	ctx := logging.WithLogger(context.TODO(), logger.Sugar())

	clientCtx, clientCancelFn := context.WithCancel(ctx)
	serverCtx, serverCancelFn := context.WithCancel(ctx)

	server, closedServerSignal, err := StartInsecureControlServer(serverCtx)
	require.NoError(t, err)
	t.Cleanup(func() {
		serverCancelFn()
		<-closedServerSignal
	})

	client, err = StartControlClient(clientCtx, &net.Dialer{
		KeepAlive: keepAlive,
		Deadline:  time.Time{},
	}, "127.0.0.1")
	require.NoError(t, err)
	t.Cleanup(clientCancelFn)

	return serverCtx, server, clientCtx, client
}

func runSendReceiveTest(t *testing.T, sender service.Service, receiver service.Service) {
	wg := sync.WaitGroup{}
	wg.Add(1)

	receiver.InboundMessageHandler(service.ControlMessageHandlerFunc(func(ctx context.Context, message service.ControlMessage) {
		require.Equal(t, uint8(1), message.Headers().OpCode())
		require.Equal(t, "Funky!", string(message.Payload()))
		message.Ack()
		wg.Done()
	}))

	require.NoError(t, sender.SendAndWaitForAck(1, mockMessage("Funky!")))

	wg.Wait()
}
