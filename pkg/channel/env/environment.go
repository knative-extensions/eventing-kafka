package env

import (
	"go.uber.org/zap"
	"knative.dev/eventing-kafka/pkg/common/config"
	"knative.dev/eventing-kafka/pkg/common/env"
)

// The Environment Struct
type Environment struct {

	// Metrics Configuration
	MetricsPort   int    // Required
	MetricsDomain string // Required

	// Health Configuration
	HealthPort int // Required

	// Kafka Configuration
	KafkaBrokers string // Required
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

	// Log The Channel Configuration Loaded From Environment Variables
	logger.Info("Environment Variables", zap.Any("Environment", safeEnvironment))

	// Return The Populated Channel Configuration Environment Structure
	return environment, nil
}

// ConfigurationError is the type of error returned from VerifyOverrides
// when a setting is missing or invalid
type ChannelConfigurationError string

func (err ChannelConfigurationError) Error() string {
	return "channel: invalid configuration (" + string(err) + ")"
}

// VerifyOverrides overwrites an EventingKafkaConfig struct with the values from an Environment struct
// The fields here are not permitted to be overridden by the values from the config-eventing-kafka configmap
// VerifyOverrides returns an error if mandatory fields in the EventingKafkaConfig have not been set either
// via the external configmap or the internal variables.
func VerifyOverrides(configuration *config.EventingKafkaConfig, environment *Environment) error {
	// Copy environment to configuration struct
	configuration.Metrics.Port = environment.MetricsPort
	configuration.Metrics.Domain = environment.MetricsDomain
	configuration.Health.Port = environment.HealthPort
	configuration.Kafka.Brokers = environment.KafkaBrokers
	configuration.Kafka.ServiceName = environment.ServiceName
	configuration.Kafka.Username = environment.KafkaUsername
	configuration.Kafka.Password = environment.KafkaPassword

	// Verify mandatory configuration settings
	switch {
	case configuration.Health.Port < 1:
		return ChannelConfigurationError("Health.Port must be > 0")
	case configuration.Metrics.Port < 1:
		return ChannelConfigurationError("Metrics.Port must be > 0")
	case configuration.Metrics.Domain == "":
		return ChannelConfigurationError("Metrics.Domain must not be empty")
	}
	return nil // no problems found
}
