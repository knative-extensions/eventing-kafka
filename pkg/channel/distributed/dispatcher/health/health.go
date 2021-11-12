/*
Copyright 2020 The Knative Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package health

import (
	"sync"

	"knative.dev/eventing-kafka/pkg/channel/distributed/common/health"
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
