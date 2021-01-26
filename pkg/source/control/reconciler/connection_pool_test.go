package reconciler

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"knative.dev/pkg/logging"

	"knative.dev/eventing-kafka/pkg/source/control/network"
	"knative.dev/eventing-kafka/pkg/source/control/service"
)

type mockMessage string

func (m mockMessage) MarshalBinary() (data []byte, err error) {
	return []byte(m), nil
}

func (m *mockMessage) UnmarshalBinary(data []byte) error {
	*m = mockMessage(data)
	return nil
}

func parseMockMessage(bytes []byte) (interface{}, error) {
	var msg mockMessage
	err := (&msg).UnmarshalBinary(bytes)
	if err != nil {
		return nil, err
	}
	return &msg, nil
}

func mockValueMerger(old interface{}, new interface{}) interface{} {
	oldMsg := old.(*mockMessage)
	newMsg := new.(*mockMessage)
	merged := mockMessage(string(*oldMsg) + string(*newMsg))
	return &merged
}

var serverConnectionPoolSetupTestCases = map[string]func(t *testing.T, ctx context.Context, opts ...ControlPlaneConnectionPoolOption) (service.Service, *ControlPlaneConnectionPool){
	"InsecureConnectionPool": setupInsecureServerAndConnectionPool,
	"TLSConnectionPool": func(t *testing.T, ctx context.Context, opts ...ControlPlaneConnectionPoolOption) (service.Service, *ControlPlaneConnectionPool) {
		serverCtx, serverCancelFn := context.WithCancel(ctx)

		certManager, err := network.NewCertificateManager(ctx)
		require.NoError(t, err)

		server, closedServerSignal, err := network.StartControlServer(serverCtx, mustGenerateTLSServerConf(t, certManager))
		require.NoError(t, err)
		t.Cleanup(func() {
			serverCancelFn()
			<-closedServerSignal
		})

		connectionPool := NewControlPlaneConnectionPool(certManager, opts...)
		t.Cleanup(func() {
			connectionPool.Close(ctx)
		})

		return server, connectionPool
	},
}

func TestReconcileConnections(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	ctx := logging.WithLogger(context.TODO(), logger.Sugar())

	for name, setupFn := range serverConnectionPoolSetupTestCases {
		t.Run(name, func(t *testing.T) {
			server, connectionPool := setupFn(t, ctx)

			newServiceInvokedCounter := atomic.NewInt32(0)
			oldServiceInvokedCounter := atomic.NewInt32(0)

			conns, err := connectionPool.ReconcileConnections(context.TODO(), "hello", []string{"127.0.0.1"}, func(string, service.Service) {
				newServiceInvokedCounter.Inc()
			}, func(string) {
				oldServiceInvokedCounter.Inc()
			})
			require.NoError(t, err)
			require.Contains(t, conns, "127.0.0.1")
			require.Equal(t, int32(1), newServiceInvokedCounter.Load())
			require.Equal(t, int32(0), oldServiceInvokedCounter.Load())

			runSendReceiveTest(t, server, conns["127.0.0.1"])

			newServiceInvokedCounter.Store(0)
			oldServiceInvokedCounter.Store(0)

			conns, err = connectionPool.ReconcileConnections(context.TODO(), "hello", []string{}, func(string, service.Service) {
				newServiceInvokedCounter.Inc()
			}, func(string) {
				oldServiceInvokedCounter.Inc()
			})
			require.NoError(t, err)
			require.NotContains(t, conns, "127.0.0.1")
			require.Equal(t, int32(0), newServiceInvokedCounter.Load())
			require.Equal(t, int32(1), oldServiceInvokedCounter.Load())

		})
	}
}

func TestCachingWrapper(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	ctx := logging.WithLogger(context.TODO(), logger.Sugar())
	for name, setupFn := range serverConnectionPoolSetupTestCases {
		t.Run(name, func(t *testing.T) {
			dataPlane, connectionPool := setupFn(t, ctx, WithServiceWrapper(WithCachingService(ctx)))

			conns, err := connectionPool.ReconcileConnections(context.TODO(), "hello", []string{"127.0.0.1"}, nil, nil)
			require.NoError(t, err)
			require.Contains(t, conns, "127.0.0.1")

			controlPlane := conns["127.0.0.1"]

			messageReceivedCounter := atomic.NewInt32(0)

			dataPlane.InboundMessageHandler(service.ControlMessageHandlerFunc(func(ctx context.Context, message service.ControlMessage) {
				require.Equal(t, uint8(1), message.Headers().OpCode())
				require.Equal(t, "Funky!", string(message.Payload()))
				message.Ack()
				messageReceivedCounter.Inc()
			}))

			for i := 0; i < 10; i++ {
				require.NoError(t, controlPlane.SendAndWaitForAck(1, mockMessage("Funky!")))
			}

			require.Equal(t, int32(1), messageReceivedCounter.Load())
		})
	}
}

func mustGenerateTLSServerConf(t *testing.T, certManager *network.CertificateManager) *tls.Config {
	dataPlaneKeyPair, err := certManager.EmitNewDataPlaneCertificate(context.TODO())
	require.NoError(t, err)

	dataPlaneCert, err := tls.X509KeyPair(dataPlaneKeyPair.CertBytes(), dataPlaneKeyPair.PrivateKeyBytes())
	require.NoError(t, err)

	certPool := x509.NewCertPool()
	certPool.AddCert(certManager.CaCert())
	return &tls.Config{
		Certificates: []tls.Certificate{dataPlaneCert},
		ClientCAs:    certPool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ServerName:   certManager.CaCert().DNSNames[0],
	}
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

func setupInsecureServerAndConnectionPool(t *testing.T, ctx context.Context, opts ...ControlPlaneConnectionPoolOption) (service.Service, *ControlPlaneConnectionPool) {
	serverCtx, serverCancelFn := context.WithCancel(ctx)

	server, closedServerSignal, err := network.StartInsecureControlServer(serverCtx)
	require.NoError(t, err)
	t.Cleanup(func() {
		serverCancelFn()
		<-closedServerSignal
	})

	connectionPool := NewInsecureControlPlaneConnectionPool(opts...)
	t.Cleanup(func() {
		connectionPool.Close(ctx)
	})

	return server, connectionPool
}
