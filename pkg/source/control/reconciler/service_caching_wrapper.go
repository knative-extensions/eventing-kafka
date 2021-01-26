package reconciler

import (
	"context"
	"encoding"
	"reflect"
	"sync"

	"knative.dev/pkg/logging"

	ctrlservice "knative.dev/eventing-kafka/pkg/source/control/service"
)

type cachingService struct {
	ctrlservice.Service

	ctx context.Context

	sentMessageMutex sync.RWMutex
	sentMessages     map[uint8]interface{}
}

var _ ctrlservice.Service = (*cachingService)(nil)

// WithCachingService will cache last message sent for each opcode and, in case you try to send a message again with the same opcode and payload, the message
// won't be sent again
func WithCachingService(ctx context.Context) ctrlservice.ServiceWrapper {
	return func(service ctrlservice.Service) ctrlservice.Service {
		return &cachingService{
			Service:      service,
			ctx:          ctx,
			sentMessages: make(map[uint8]interface{}),
		}
	}
}

func (c *cachingService) SendAndWaitForAck(opcode uint8, payload encoding.BinaryMarshaler) error {
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
