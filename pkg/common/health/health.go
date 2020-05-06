package health

import (
	"context"
	"go.uber.org/zap"
	"net/http"
	"sync"
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
	httpPort string // The HTTP Port The Dispatcher Server Listens On

	// Synchronization Mutexes
	liveMutex sync.Mutex // Synchronizes access to the liveness flag

	// Internal Flags
	alive bool // A flag that controls the response to liveness requests
}

// Creates A New Server With Specified Configuration
func NewHealthServer(httpPort string, healthStatus Status) *Server {
	health := &Server{
		httpPort: httpPort,
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
	serveMux.HandleFunc(LivenessPath, hs.handleLiveness)
	serveMux.HandleFunc(ReadinessPath, hs.handleReadiness)

	// Create The Server For Configured HTTP Port
	server := &http.Server{Addr: ":" + httpPort, Handler: serveMux}

	// Set The Initialized HTTP Server
	hs.server = server
}

// Start The HTTP Server (Blocking Call)
func (hs *Server) Start(logger *zap.Logger) {
	logger.Info("Starting Server HTTP Server on port " + hs.httpPort)
	go func() {
		err := hs.server.ListenAndServe()
		if err != nil {
			logger.Info("Server HTTP ListenAndServe Returned Error", zap.Error(err)) // Info log since it could just be normal shutdown
		}
	}()
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
func (hs *Server) handleLiveness(responseWriter http.ResponseWriter, request *http.Request) {
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
func (hs *Server) handleReadiness(responseWriter http.ResponseWriter, request *http.Request) {
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
