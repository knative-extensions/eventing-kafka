package service

import (
	"context"

	"knative.dev/pkg/logging"
)

type ControlMessageHandler interface {
	HandleControlMessage(ctx context.Context, message ControlMessage)
}

type ControlMessageHandlerFunc func(ctx context.Context, message ControlMessage)

func (c ControlMessageHandlerFunc) HandleControlMessage(ctx context.Context, message ControlMessage) {
	c(ctx, message)
}

var NoopControlMessageHandler ControlMessageHandlerFunc = func(ctx context.Context, message ControlMessage) {
	logging.FromContext(ctx).Warnf("Discarding control message '%s'", message.Headers().UUID())
	message.Ack()
}

type ErrorHandler interface {
	HandleServiceError(ctx context.Context, err error)
}

type ErrorHandlerFunc func(ctx context.Context, err error)

func (c ErrorHandlerFunc) HandleServiceError(ctx context.Context, err error) {
	c(ctx, err)
}

var LoggerErrorHandler ErrorHandlerFunc = func(ctx context.Context, err error) {
	logging.FromContext(ctx).Debugf("Error from the connection: %s", err)
}
