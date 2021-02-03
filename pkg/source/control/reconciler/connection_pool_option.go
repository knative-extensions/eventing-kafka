package reconciler

import (
	"knative.dev/eventing-kafka/pkg/source/control"
)

type ControlPlaneConnectionPoolOption func(*ControlPlaneConnectionPool)

func WithServiceWrapper(wrapper control.ServiceWrapper) ControlPlaneConnectionPoolOption {
	return func(pool *ControlPlaneConnectionPool) {
		pool.serviceWrapperFactories = append(pool.serviceWrapperFactories, wrapper)
	}
}
