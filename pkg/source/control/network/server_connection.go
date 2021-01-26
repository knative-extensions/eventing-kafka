package network

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"net"
	"time"

	"knative.dev/pkg/logging"

	ctrl "knative.dev/eventing-kafka/pkg/source/control"
	"knative.dev/eventing-kafka/pkg/source/control/service"
)

const (
	baseCertsPath = "/etc/secret"
)

var listenConfig = net.ListenConfig{
	KeepAlive: 30 * time.Second,
}

func LoadServerTLSConfig() (*tls.Config, error) {
	dataPlaneCert, err := tls.LoadX509KeyPair(baseCertsPath+"/data_plane_cert.pem", baseCertsPath+"/data_plane_secret.pem")
	if err != nil {
		return nil, err
	}

	caCert, err := ioutil.ReadFile(baseCertsPath + "/ca_cert.pem")
	if err != nil {
		return nil, err
	}

	certPool := x509.NewCertPool()
	certPool.AppendCertsFromPEM(caCert)
	conf := &tls.Config{
		Certificates: []tls.Certificate{dataPlaneCert},
		ClientCAs:    certPool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ServerName:   fakeDnsName,
	}

	return conf, nil
}

func StartInsecureControlServer(ctx context.Context) (service.Service, <-chan struct{}, error) {
	return StartControlServer(ctx, nil)
}

func StartControlServer(ctx context.Context, tlsConf *tls.Config) (service.Service, <-chan struct{}, error) {
	ln, err := listenConfig.Listen(ctx, "tcp", ":9000")
	if err != nil {
		return nil, nil, err
	}
	if tlsConf != nil {
		ln = tls.NewListener(ln, tlsConf)
	}

	tcpConn := newServerTcpConnection(ctx, ln)
	ctrlService := service.NewService(ctx, tcpConn)

	logging.FromContext(ctx).Infof("Started listener: %s", ln.Addr().String())

	closedServerCh := make(chan struct{})

	tcpConn.startAcceptPolling(closedServerCh)

	return ctrlService, closedServerCh, nil
}

type serverTcpConnection struct {
	baseTcpConnection

	listener net.Listener
}

func newServerTcpConnection(ctx context.Context, listener net.Listener) *serverTcpConnection {
	c := &serverTcpConnection{
		baseTcpConnection: baseTcpConnection{
			ctx:                    ctx,
			logger:                 logging.FromContext(ctx),
			outboundMessageChannel: make(chan *ctrl.OutboundMessage, 10),
			inboundMessageChannel:  make(chan *ctrl.InboundMessage, 10),
			errors:                 make(chan error, 10),
		},
		listener: listener,
	}
	return c
}

func (t *serverTcpConnection) startAcceptPolling(closedServerChannel chan struct{}) {
	// We have 2 goroutines:
	// * One polls the listener to accept new conns
	// * One blocks on context done and closes the listener and the connection
	go func() {
		for {
			conn, err := t.listener.Accept()
			if err != nil {
				t.logger.Warnf("Error while accepting the connection, closing the accept loop: %s", err)
				return
			}
			t.logger.Debugf("Accepting new control connection from %s", conn.RemoteAddr())
			t.consumeConnection(conn)
			select {
			case <-t.ctx.Done():
				return
			default:
				continue
			}
		}
	}()
	go func() {
		<-t.ctx.Done()
		t.logger.Infof("Closing control server")
		err := t.listener.Close()
		t.logger.Infof("Listener closed")
		if err != nil {
			t.logger.Warnf("Error while closing the listener: %s", err)
		}
		err = t.close()
		if err != nil {
			t.logger.Warnf("Error while closing the tcp connection: %s", err)
		}
		close(closedServerChannel)
	}()
}
