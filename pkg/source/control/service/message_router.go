package service

import (
	"context"

	"go.uber.org/zap"
	"knative.dev/pkg/logging"

	"knative.dev/eventing-kafka/pkg/source/control"
)

type MessageRouter map[control.OpCode]control.MessageHandler

func (c MessageRouter) HandleServiceMessage(ctx context.Context, message control.ServiceMessage) {
	logger := logging.FromContext(ctx)

	handler, ok := c[control.OpCode(message.Headers().OpCode())]
	if ok {
		handler.HandleServiceMessage(ctx, message)
		return
	}

	message.Ack()
	logger.Warnw(
		"Received an unknown message, I don't know what to do with it",
		zap.Uint8("opcode", message.Headers().OpCode()),
		zap.ByteString("payload", message.Payload()),
	)
}
