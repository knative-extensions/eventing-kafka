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
	"context"
	"net"
	"net/http"
	"strconv"
	"sync"

	"go.uber.org/zap"
)

// Interface For Providing Overrides For Liveness And Readiness Information
type Status interface {
	Alive() bool
	Ready() bool
}

// Structure Containing Basic Liveness Information For Health Server
type Server struct {
	server   *http.Server // The Golang HTTP Server Instance
	status   Status
	HttpPort string // The HTTP Port The Dispatcher Server Listens On

	// Synchronization Mutexes
	liveMutex sync.Mutex // Synchronizes access to the liveness flag

	// Internal Flags
	alive bool // A flag that controls the response to liveness requests
}

// Creates A New Server With Specified Configuration
func NewHealthServer(httpPort string, healthStatus Status) *Server {
	health := &Server{
		HttpPort: httpPort,
		status:   healthStatus,
	}

	// Initialize The HTTP Server
	health.initializeServer(httpPort)

	// Return The Health Server
	return health
}

// Synchronized Function To Set Liveness Flag
func (hs *Server) SetAlive(isAlive bool) {
	hs.liveMutex.Lock()
	hs.alive = isAlive
	hs.liveMutex.Unlock()
}

// Set All Liveness And Readiness Flags To False
func (hs *Server) Shutdown() {
	hs.SetAlive(false)
}

// Initialize The HTTP Server
func (hs *Server) initializeServer(httpPort string) {

	serveMux := http.NewServeMux()
	serveMux.HandleFunc(LivenessPath, hs.HandleLiveness)
	serveMux.HandleFunc(ReadinessPath, hs.HandleReadiness)

	// Create The Server For Configured HTTP Port
	server := &http.Server{Addr: ":" + httpPort, Handler: serveMux}

	// Set The Initialized HTTP Server
	hs.server = server
}

// Start The HTTP Server (Blocking Call)
func (hs *Server) Start(logger *zap.Logger) error {
	listener, err := net.Listen("tcp", ":"+hs.HttpPort)
	if err != nil {
		logger.Error("Server HTTP Listen Returned Error", zap.Error(err))
		return err
	}

	// Set the HttpPort field to whatever port was actually used by the system
	// before launching the Server goroutine, so that the caller can access it.
	// (If "0" is passed in then the Listen call will assign one arbitrarily)
	hs.HttpPort = strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)

	go func() {
		logger.Info("Starting Server HTTP Server on port " + hs.HttpPort)

		err = hs.server.Serve(listener)
		if err != nil {
			logger.Info("Server HTTP Serve Returned Error", zap.Error(err)) // Info log since it could just be normal shutdown
		}
	}()

	return nil
}

// Stop The HTTP Server Listening For Requests
func (hs *Server) Stop(logger *zap.Logger) {
	logger.Info("Stopping Server HTTP Server")
	err := hs.server.Shutdown(context.TODO())
	if err != nil {
		logger.Error("Server Failed To Shutdown HTTP Server", zap.Error(err))
	}
}

// Access Function For "alive" Flag
func (hs *Server) Alive() bool {
	hs.liveMutex.Lock()
	defer hs.liveMutex.Unlock()
	return hs.alive
}

// HTTP Request Handler For Liveness Requests (/healthz)
func (hs *Server) HandleLiveness(responseWriter http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		responseWriter.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if hs.status.Alive() {
		responseWriter.WriteHeader(http.StatusOK)
	} else {
		responseWriter.WriteHeader(http.StatusInternalServerError)
	}
}

// HTTP Request Handler For Readiness Requests (/healthy)
func (hs *Server) HandleReadiness(responseWriter http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		responseWriter.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if hs.status.Ready() {
		responseWriter.WriteHeader(http.StatusOK)
	} else {
		responseWriter.WriteHeader(http.StatusInternalServerError)
	}
}
