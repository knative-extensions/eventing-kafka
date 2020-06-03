package health

import (
	"knative.dev/eventing-kafka/pkg/common/health"
	"sync"
)

// Start The HTTP Server Listening For Requests

type Server struct {
	health.Server

	// Additional Synchronization Mutexes
	dispatcherMutex sync.Mutex // Synchronizes access to the dispatcherReady flag

	// Additional Internal Flags
	dispatcherReady bool // A flag that the producer sets when it is ready
}

// Creates A New Server With Specified Configuration
func NewDispatcherHealthServer(httpPort string) *Server {
	dispatcherHealth := &Server{}
	dispatcherHealth.Server = *health.NewHealthServer(httpPort, dispatcherHealth)

	// Return The Server
	return dispatcherHealth
}

// Synchronized Function To Set Producer Ready Flag
func (chs *Server) SetDispatcherReady(isReady bool) {
	chs.dispatcherMutex.Lock()
	chs.dispatcherReady = isReady
	chs.dispatcherMutex.Unlock()
}

// Set All Liveness And Readiness Flags To False
func (chs *Server) Shutdown() {
	chs.Server.Shutdown()
	chs.SetDispatcherReady(false)
}

// Access Function For DispatcherReady Flag
func (chs *Server) DispatcherReady() bool {
	return chs.dispatcherReady
}

// Functions That Implement The HealthInterface

// Response Function For Readiness Requests (/healthy)
func (chs *Server) Ready() bool {
	return chs.dispatcherReady
}

// Response Function For Liveness Requests (/healthz)
func (chs *Server) Alive() bool {
	return chs.Server.Alive()
}
