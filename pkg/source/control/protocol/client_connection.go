package protocol

import (
	"context"
	"fmt"
	"net"
	"time"

	"knative.dev/pkg/logging"
)

var dialOptions = net.Dialer{
	KeepAlive: keepAlive,
	Deadline:  time.Time{},
}

func StartControlClient(ctx context.Context, target string) (Service, error) {
	target = target + ":9000"
	logging.FromContext(ctx).Infof("Starting control client to %s", target)

	// Let's try the dial
	conn, err := tryDial(ctx, target, clientInitialDialRetry, clientDialRetryInterval)
	if err != nil {
		return nil, fmt.Errorf("cannot perform the initial dial to target %s: %w", target, err)
	}

	tcpConn := newClientTcpConnection(ctx)
	svc := newService(ctx, tcpConn)

	tcpConn.startPolling(conn)

	return svc, nil
}

func tryDial(ctx context.Context, target string, retries int, interval time.Duration) (net.Conn, error) {
	var conn net.Conn
	var err error
	for i := 0; i < retries; i++ {
		conn, err = dialOptions.DialContext(ctx, "tcp", target)
		if err == nil {
			return conn, nil
		}
		logging.FromContext(ctx).Warnf("Error while trying to connect: %v", err)
		select {
		case <-ctx.Done():
			return nil, err
		case <-time.After(interval):
		}
	}
	return nil, err
}

type clientTcpConnection struct {
	baseTcpConnection
}

func newClientTcpConnection(ctx context.Context) *clientTcpConnection {
	c := &clientTcpConnection{
		baseTcpConnection: baseTcpConnection{
			ctx:                    ctx,
			logger:                 logging.FromContext(ctx),
			outboundMessageChannel: make(chan *OutboundMessage, 10),
			inboundMessageChannel:  make(chan *InboundMessage, 10),
			errors:                 make(chan error, 10),
		},
	}
	return c
}

func (t *clientTcpConnection) startPolling(initialConn net.Conn) {
	// We have 2 goroutines:
	// * One consumes the connections and it eventually reconnects
	// * One blocks on context done and closes the connection
	go func(initialConn net.Conn) {
		// Consume the connection
		t.consumeConnection(initialConn)

		// Retry until connection closed
		for {
			select {
			case <-t.ctx.Done():
				return
			default:
				t.logger.Warnf("Connection lost, retrying to reconnect %s", initialConn.RemoteAddr().String())

				// Let's try the dial
				conn, err := tryDial(t.ctx, initialConn.RemoteAddr().String(), clientReconnectionRetry, clientDialRetryInterval)
				if err != nil {
					t.logger.Warnf("Cannot re-dial to target %s: %v", initialConn.RemoteAddr().String(), err)
					return
				}

				t.consumeConnection(conn)
			}
		}
	}(initialConn)
	go func() {
		<-t.ctx.Done()
		t.logger.Infof("Closing control client")
		err := t.close()
		t.logger.Infof("Connection closed")
		if err != nil {
			t.logger.Warnf("Error while closing the connection: %s", err)
		}
	}()
}
