package env

import (
	"go.uber.org/zap"
	"knative.dev/eventing-kafka/pkg/common/config"
	"knative.dev/eventing-kafka/pkg/common/env"
	"knative.dev/eventing-kafka/pkg/dispatcher/constants"
)

// Environment Structure
type Environment struct {

	// Metrics Configuration
	MetricsPort   int    // Required
	MetricsDomain string // Required

	// Health Configuration
	HealthPort int // Required

	// Kafka Configuration
	KafkaBrokers string // Required
	KafkaTopic   string // Required
	ChannelKey   string // Required
	ServiceName  string // Required

	// Kafka Authorization
	KafkaUsername string // Optional
	KafkaPassword string // Optional
}

// Get The Environment
func GetEnvironment(logger *zap.Logger) (*Environment, error) {

	// Error Reference
	var err error

	// The ControllerConfig Reference
	environment := &Environment{}

	// Get The Required Metrics Port Config Value & Convert To Int
	environment.MetricsPort, err = env.GetRequiredConfigInt(logger, env.MetricsPortEnvVarKey, "MetricsPort")
	if err != nil {
		return nil, err
	}

	// Get The Required Metrics Domain Config Value
	environment.MetricsDomain, err = env.GetRequiredConfigValue(logger, env.MetricsDomainEnvVarKey)
	if err != nil {
		return nil, err
	}

	// Get The Required HealthPort Port Config Value & Convert To Int
	environment.HealthPort, err = env.GetRequiredConfigInt(logger, env.HealthPortEnvVarKey, "HealthPort")
	if err != nil {
		return nil, err
	}

	// Get The Required K8S KafkaBrokers Config Value
	environment.KafkaBrokers, err = env.GetRequiredConfigValue(logger, env.KafkaBrokerEnvVarKey)
	if err != nil {
		return nil, err
	}

	// Get The Required K8S KafkaTopic Config Value
	environment.KafkaTopic, err = env.GetRequiredConfigValue(logger, env.KafkaTopicEnvVarKey)
	if err != nil {
		return nil, err
	}

	// Get The Required K8S ChannelKey Config Value
	environment.ChannelKey, err = env.GetRequiredConfigValue(logger, env.ChannelKeyEnvVarKey)
	if err != nil {
		return nil, err
	}

	// Get The Required K8S ServiceName Config Value
	environment.ServiceName, err = env.GetRequiredConfigValue(logger, env.ServiceNameEnvVarKey)
	if err != nil {
		return nil, err
	}

	// Get The Optional KafkaUsername Config Value
	environment.KafkaUsername = env.GetOptionalConfigValue(logger, env.KafkaUsernameEnvVarKey, "")

	// Get The Optional KafkaPassword Config Value
	environment.KafkaPassword = env.GetOptionalConfigValue(logger, env.KafkaPasswordEnvVarKey, "")

	// Clone The Environment & Mask The Password For Safe Logging
	safeEnvironment := *environment
	if len(safeEnvironment.KafkaPassword) > 0 {
		safeEnvironment.KafkaPassword = "*************"
	}

	// Log The Dispatcher Configuration Loaded From Environment Variables
	logger.Info("Environment Variables", zap.Any("Environment", safeEnvironment))

	// Return The Populated Dispatcher Configuration Environment Structure
	return environment, nil
}

// ConfigurationError is the type of error returned from VerifyOverrides
// when a setting is missing or invalid
type DispatcherConfigurationError string

func (err DispatcherConfigurationError) Error() string {
	return "dispatcher: invalid configuration (" + string(err) + ")"
}

// VerifyOverrides overwrites an EventingKafkaConfig struct with the values from an Environment struct
// The fields here are not permitted to be overridden by the values from the config-eventing-kafka configmap
// VerifyOverrides returns an error if mandatory fields in the EventingKafkaConfig have not been set either
// via the external configmap or the internal variables.
func VerifyOverrides(configuration *config.EventingKafkaConfig, environment *Environment) error {
	configuration.Metrics.Port = environment.MetricsPort
	configuration.Metrics.Domain = environment.MetricsDomain
	configuration.Health.Port = environment.HealthPort
	configuration.Kafka.Brokers = environment.KafkaBrokers
	configuration.Kafka.Topic.Name = environment.KafkaTopic
	configuration.Kafka.ChannelKey = environment.ChannelKey
	configuration.Kafka.ServiceName = environment.ServiceName
	configuration.Kafka.Username = environment.KafkaUsername
	configuration.Kafka.Password = environment.KafkaPassword

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

	// Verify Mandatory configuration Settings
	switch {
	case configuration.Health.Port < 1:
		return DispatcherConfigurationError("Health.Port must be > 0")
	case configuration.Metrics.Port < 1:
		return DispatcherConfigurationError("Metrics.Port must be > 0")
	case configuration.Metrics.Domain == "":
		return DispatcherConfigurationError("Metrics.Domain must not be empty")

	// These settings should never be invalid because of the default values above, but verifying them
	// is cheap insurance against inadvertent changes
	case configuration.Dispatcher.RetryExponentialBackoff == nil:
		return DispatcherConfigurationError("Dispatcher.RetryExponentialBackoff must not be nil")
	case configuration.Dispatcher.RetryTimeMillis < 1:
		return DispatcherConfigurationError("Dispatcher.RetryTimeMillis must be > 0")
	case configuration.Dispatcher.RetryInitialIntervalMillis < 1:
		return DispatcherConfigurationError("Dispatcher.RetryInitialIntervalMillis must be > 0")
	}
	return nil // no problems found
}
