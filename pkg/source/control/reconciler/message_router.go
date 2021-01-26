package reconciler

import (
	"context"

	"go.uber.org/zap"
	"knative.dev/pkg/logging"

	ctrlservice "knative.dev/eventing-kafka/pkg/source/control/service"
)

type controlMessageRouter map[uint8]ctrlservice.ControlMessageHandler

func NewMessageRouter(routes map[uint8]ctrlservice.ControlMessageHandler) ctrlservice.ControlMessageHandler {
	return controlMessageRouter(routes)
}

func (c controlMessageRouter) HandleControlMessage(ctx context.Context, message ctrlservice.ControlMessage) {
	logger := logging.FromContext(ctx)

	handler, ok := c[message.Headers().OpCode()]
	if ok {
		handler.HandleControlMessage(ctx, message)
		return
	}

	message.Ack()
	logger.Warnw(
		"Received an unknown message, I don't know what to do with it",
		zap.Uint8("opcode", message.Headers().OpCode()),
		zap.ByteString("payload", message.Payload()),
	)
}
