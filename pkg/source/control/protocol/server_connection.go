package protocol

import (
	"context"
	"net"
	"time"

	"knative.dev/pkg/logging"
)

var listenConfig = net.ListenConfig{
	KeepAlive: 30 * time.Second,
}

func StartControlServer(ctx context.Context) (Service, error) {
	ln, err := listenConfig.Listen(ctx, "tcp", ":9000")
	if err != nil {
		return nil, err
	}

	tcpConn := newServerTcpConnection(ctx, ln)
	ctrlService := newService(ctx, tcpConn)

	logging.FromContext(ctx).Infof("Started listener: %s", ln.Addr().String())

	tcpConn.startAcceptPolling()

	return ctrlService, nil
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
			outboundMessageChannel: make(chan *OutboundMessage, 10),
			inboundMessageChannel:  make(chan *InboundMessage, 10),
			errors:                 make(chan error, 10),
		},
		listener: listener,
	}
	return c
}

func (t *serverTcpConnection) startAcceptPolling() {
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
			t.logger.Warnf("Error while closing the server: %s", err)
		}
		err = t.close()
	}()
}
