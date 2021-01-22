package protocol

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"

	"go.uber.org/zap"
)

// Connection handles the low level stuff, reading and writing to the wire
type Connection interface {
	OutboundMessages() chan<- *OutboundMessage
	InboundMessages() <-chan *InboundMessage
	// On this channel we get only very bad, usually fatal, errors (like cannot re-establish the connection after several attempts)
	Errors() <-chan error
}

type baseTcpConnection struct {
	ctx    context.Context
	logger *zap.SugaredLogger

	conn net.Conn

	outboundMessageChannel chan *OutboundMessage
	inboundMessageChannel  chan *InboundMessage
	errors                 chan error
}

func (t *baseTcpConnection) OutboundMessages() chan<- *OutboundMessage {
	return t.outboundMessageChannel
}

func (t *baseTcpConnection) InboundMessages() <-chan *InboundMessage {
	return t.inboundMessageChannel
}

func (t *baseTcpConnection) Errors() <-chan error {
	return t.errors
}

func (t *baseTcpConnection) read() error {
	msg := &InboundMessage{}
	n, err := msg.ReadFrom(t.conn)
	if err != nil {
		return err
	}
	if n != int64(msg.Length())+24 {
		return fmt.Errorf("the number of read bytes doesn't match the expected length: %d != %d", n, int64(msg.Length())+24)
	}

	t.inboundMessageChannel <- msg
	return nil
}

func (t *baseTcpConnection) write(msg *OutboundMessage) error {
	n, err := msg.WriteTo(t.conn)
	if err != nil {
		return err
	}
	if n != int64(msg.Length())+24 {
		return fmt.Errorf("the number of read bytes doesn't match the expected length: %d != %d", n, int64(msg.Length())+24)
	}
	return nil
}

func (t *baseTcpConnection) consumeConnection(conn net.Conn) {
	t.logger.Infof("Setting new conn: %s", conn.RemoteAddr())
	t.conn = conn

	closedConnCtx, closedConnCancel := context.WithCancel(t.ctx)

	var wg sync.WaitGroup
	wg.Add(2)

	// We have 2 polling loops:
	// * One polls outbound messages and writes to the conn
	// * One reads from the conn and push to inbound messages
	go func() {
		defer wg.Done()
		for {
			select {
			case msg, ok := <-t.outboundMessageChannel:
				if !ok {
					t.logger.Debugf("Outbound channel closed, closing the polling")
					return
				}
				err := t.write(msg)
				if err != nil {
					if isDisconnection(err) {
						return
					} else if !isTransientError(err) {
						// We don't retry for non transient errors
						t.errors <- err
					}

					// Try to send to outboundMessageChannel if context not closed
					select {
					case <-t.ctx.Done():
						return
					default:
						t.outboundMessageChannel <- msg
					}
				}
			case <-closedConnCtx.Done():
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		defer closedConnCancel()
		for {
			// Blocking read
			err := t.read()
			if err != nil {
				if isDisconnection(err) {
					return
				} else if !isTransientError(err) {
					// Everything is bad except transient errors and io.EOF
					t.errors <- err
				}
			}

			select {
			case <-closedConnCtx.Done():
				return
			default:
				continue
			}
		}
	}()

	wg.Wait()

	t.logger.Debugf("Stopped consuming connection with local %s and remote %s", conn.LocalAddr().String(), conn.RemoteAddr().String())
	err := t.conn.Close()
	if err != nil && !isDisconnection(err) {
		t.logger.Warnf("Error while closing the previous connection: %s", err)
	}
}

func (t *baseTcpConnection) close() (err error) {
	if t.conn != nil {
		err = t.conn.Close()
	}
	close(t.inboundMessageChannel)
	close(t.outboundMessageChannel)
	close(t.errors)
	return err
}

func isTransientError(err error) bool {
	// Transient errors are fine
	if neterr, ok := err.(net.Error); ok {
		if neterr.Temporary() || neterr.Timeout() {
			return true
		}
	}
	return false
}

// Check connection closed https://stackoverflow.com/questions/12741386/how-to-know-tcp-connection-is-closed-in-net-package
func isDisconnection(err error) bool {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}

	if neterr, ok := err.(*net.OpError); ok && strings.Contains(neterr.Error(), "close") {
		return true
	}

	return false
}
