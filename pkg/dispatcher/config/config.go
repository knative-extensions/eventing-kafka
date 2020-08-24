package config

import (
	"knative.dev/eventing-kafka/pkg/common/config"
	"knative.dev/eventing-kafka/pkg/dispatcher/constants"
)

// ConfigurationError is the type of error returned from VerifyOverrides
// when a setting is missing or invalid
type DispatcherConfigurationError string

func (err DispatcherConfigurationError) Error() string {
	return "dispatcher: invalid configuration (" + string(err) + ")"
}

// VerifyConfiguration returns an error if mandatory fields in the EventingKafkaConfig have not been set either
// via the external configmap or the internal variables.
func VerifyConfiguration(configuration *config.EventingKafkaConfig) error {
	// Verify Mandatory configuration Settings

	// Set Default Values For Some Fields If Not Provided
	if configuration.Dispatcher.RetryExponentialBackoff == nil {
		backoffDefault := constants.DefaultExponentialBackoff
		configuration.Dispatcher.RetryExponentialBackoff = &backoffDefault
	}

	if configuration.Dispatcher.RetryInitialIntervalMillis < 1 {
		configuration.Dispatcher.RetryInitialIntervalMillis = constants.DefaultEventRetryInitialIntervalMillis
	}

	if configuration.Dispatcher.RetryTimeMillis < 1 {
		configuration.Dispatcher.RetryTimeMillis = constants.DefaultEventRetryTimeMillisMax
	}

	// These settings should never be invalid because of the default values above, but verifying them
	// is cheap insurance against inadvertent changes
	switch {
	case configuration.Dispatcher.RetryExponentialBackoff == nil:
		return DispatcherConfigurationError("Dispatcher.RetryExponentialBackoff must not be nil")
	case configuration.Dispatcher.RetryTimeMillis < 1:
		return DispatcherConfigurationError("Dispatcher.RetryTimeMillis must be > 0")
	case configuration.Dispatcher.RetryInitialIntervalMillis < 1:
		return DispatcherConfigurationError("Dispatcher.RetryInitialIntervalMillis must be > 0")
	}
	return nil // no problems found
}
