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

// ApplyOverrides overwrites an EventingKafkaConfig struct with the values from an Environment struct
// This allows us to remove variables from the required environment list as desired instead of
// changing everything at once.  As they are removed from the GetEnvironment function (and the Environment
// struct itself), the values from the configmap will take over.
func ApplyOverrides(ekConfig *config.EventingKafkaConfig, environment *Environment) {
	ekConfig.Metrics.Port = environment.MetricsPort
	ekConfig.Metrics.Domain = environment.MetricsDomain
	ekConfig.Health.Port = environment.HealthPort
	ekConfig.Kafka.Brokers = environment.KafkaBrokers
	ekConfig.Kafka.ServiceName = environment.ServiceName
	ekConfig.Kafka.Username = environment.KafkaUsername
	ekConfig.Kafka.Password = environment.KafkaPassword
}
