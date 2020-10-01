package health

// Constants
const (
	// Default Health Configuration
	LivenessPath  = "/healthz" // The Endpoint Of The Liveness Check
	ReadinessPath = "/healthy" // The Endpoint Of The Readiness Check
)
