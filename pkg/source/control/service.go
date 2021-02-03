package control

import (
	"context"
	"encoding"
)

type OpCode uint8

const AckOpCode OpCode = 0

type ServiceMessage struct {
	inboundMessage *InboundMessage
	ackFunc        func()
}

func NewServiceMessage(inboundMessage *InboundMessage, ackFunc func()) ServiceMessage {
	return ServiceMessage{
		inboundMessage: inboundMessage,
		ackFunc:        ackFunc,
	}
}

func (c ServiceMessage) Headers() MessageHeader {
	return c.inboundMessage.MessageHeader
}

func (c ServiceMessage) Payload() []byte {
	return c.inboundMessage.Payload
}

func (c ServiceMessage) Ack() {
	c.ackFunc()
}

type MessageHandler interface {
	HandleServiceMessage(ctx context.Context, message ServiceMessage)
}

type MessageHandlerFunc func(ctx context.Context, message ServiceMessage)

func (c MessageHandlerFunc) HandleServiceMessage(ctx context.Context, message ServiceMessage) {
	c(ctx, message)
}

type ErrorHandler interface {
	HandleServiceError(ctx context.Context, err error)
}

type ErrorHandlerFunc func(ctx context.Context, err error)

func (c ErrorHandlerFunc) HandleServiceError(ctx context.Context, err error) {
	c(ctx, err)
}

// Service is the high level interface that handles send with retries and acks
type Service interface {
	SendAndWaitForAck(opcode OpCode, payload encoding.BinaryMarshaler) error
	// This is non blocking, because a polling loop is already running inside.
	MessageHandler(handler MessageHandler)
	// This is non blocking, because a polling loop is already running inside.
	ErrorHandler(handler ErrorHandler)
}

// ServiceWrapper wraps a service in another service to offer additional functionality
type ServiceWrapper func(Service) Service
