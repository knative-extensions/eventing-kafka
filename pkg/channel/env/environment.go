package env

import (
	"go.uber.org/zap"
	"knative.dev/eventing-kafka/pkg/common/env"
)

// The Environment Struct
type Environment struct {

	// Metrics Configuration
	MetricsPort int                         // Required
	MetricsDomain string                    // Required

	// Health Configuration
	HealthPort int                          // Required

	// Kafka Configuration
	KafkaBrokers string                     // Required
	ServiceName string                      // Required

	// Kafka Authorization
	KafkaUsername string                    // Optional
	KafkaPassword string                    // Optional
	KafkaPasswordLog string                 // Derived
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

	// Get The Required KafkaUsername Config Value
	environment.KafkaUsername, err = env.GetRequiredConfigValue(logger, env.KafkaUsernameEnvVarKey)
	if err != nil {
		return nil, err
	}

	// Get The Required KafkaPassword Config Value
	environment.KafkaPassword, err = env.GetRequiredConfigValue(logger, env.KafkaPasswordEnvVarKey)
	if err != nil {
		return nil, err
	}

	// Mask The Password For Logging (If There Was One)
	environment.KafkaPasswordLog = ""
	if len(environment.KafkaPassword) > 0 {
		environment.KafkaPasswordLog = "*************"
	}

	// Log The Channel Configuration Loaded From Environment Variables
	logger.Info("Environment Variables", zap.Any("Environment", environment))

	// Return The Populated Channel Configuration Environment Structure
	return environment, nil
}
