package reconciler

import ctrlservice "knative.dev/eventing-kafka/pkg/source/control/service"

type ControlPlaneConnectionPoolOption func(*ControlPlaneConnectionPool)

func WithServiceWrapper(wrapper ctrlservice.ServiceWrapper) ControlPlaneConnectionPoolOption {
	return func(pool *ControlPlaneConnectionPool) {
		pool.serviceWrapperFactories = append(pool.serviceWrapperFactories, wrapper)
	}
}
