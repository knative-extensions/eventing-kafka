package health

import (
	"sync"

	"knative.dev/eventing-kafka/pkg/channel/distributed/common/health"
)

// Start The HTTP Server Listening For Requests

type Server struct {
	health.Server

	// Additional Synchronization Mutexes
	producerMutex sync.Mutex // Synchronizes access to the producerReady flag
	channelMutex  sync.Mutex // Synchronizes access to the channelReady flag

	// Additional Internal Flags
	producerReady bool // A flag that the producer sets when it is ready
	channelReady  bool // A flag that the channel sets when it is ready
}

// Creates A New Server With Specified Configuration
func NewChannelHealthServer(httpPort string) *Server {
	channelHealth := &Server{}
	channelHealth.Server = *health.NewHealthServer(httpPort, channelHealth)

	// Return The Server
	return channelHealth
}

// Synchronized Function To Set Producer Ready Flag
func (chs *Server) SetProducerReady(isReady bool) {
	chs.producerMutex.Lock()
	chs.producerReady = isReady
	chs.producerMutex.Unlock()
}

// Synchronized Function To Set Channel Ready Flag
func (chs *Server) SetChannelReady(isReady bool) {
	chs.channelMutex.Lock()
	chs.channelReady = isReady
	chs.channelMutex.Unlock()
}

// Set All Liveness And Readiness Flags To False
func (chs *Server) Shutdown() {
	chs.Server.Shutdown()
	chs.SetProducerReady(false)
	chs.SetChannelReady(false)
}

// Access Function For ProducerReady Flag
func (chs *Server) ProducerReady() bool {
	return chs.producerReady
}

// Access Function For ChannelReady Flag
func (chs *Server) ChannelReady() bool {
	return chs.channelReady
}

// Functions That Implement The HealthInterface

// Response Function For Readiness Requests (/healthy)
func (chs *Server) Ready() bool {
	return chs.producerReady && chs.channelReady
}

// Response Function For Liveness Requests (/healthz)
func (chs *Server) Alive() bool {
	return chs.Server.Alive()
}
