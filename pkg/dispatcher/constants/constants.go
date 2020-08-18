package constants

// Global Constants
const (
	// Default Values If Optional config-eventing-kafka ConfigMap Defaults Not Specified
	DefaultEventRetryInitialIntervalMillis = 500    // 0.5 seconds
	DefaultEventRetryTimeMillisMax         = 300000 // 5 minutes
	DefaultExponentialBackoff              = true   // Enabled

	KnativeEventingNamespace = "knative-eventing"
)
