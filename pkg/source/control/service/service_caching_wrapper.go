package service

import (
	"context"
	"encoding"
	"reflect"
	"sync"

	"knative.dev/pkg/logging"

	"knative.dev/eventing-kafka/pkg/source/control"
)

type cachingService struct {
	control.Service

	ctx context.Context

	sentMessageMutex sync.RWMutex
	sentMessages     map[control.OpCode]interface{}
}

var _ control.Service = (*cachingService)(nil)

// WithCachingService will cache last message sent for each opcode and, in case you try to send a message again with the same opcode and payload, the message
// won't be sent again
func WithCachingService(ctx context.Context) control.ServiceWrapper {
	return func(service control.Service) control.Service {
		return &cachingService{
			Service:      service,
			ctx:          ctx,
			sentMessages: make(map[control.OpCode]interface{}),
		}
	}
}

func (c *cachingService) SendAndWaitForAck(opcode control.OpCode, payload encoding.BinaryMarshaler) error {
	c.sentMessageMutex.RLock()
	lastPayload, ok := c.sentMessages[opcode]
	c.sentMessageMutex.RUnlock()
	if ok && reflect.DeepEqual(lastPayload, payload) {
		logging.FromContext(c.ctx).Debugf("Message with opcode %d already sent with payload: %v", opcode, lastPayload)
		return nil
	}
	err := c.Service.SendAndWaitForAck(opcode, payload)
	if err != nil {
		return err
	}
	c.sentMessageMutex.Lock()
	c.sentMessages[opcode] = payload
	c.sentMessageMutex.Unlock()
	return nil
}
