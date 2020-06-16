package env

import (
	"errors"
	"go.uber.org/zap"
	"knative.dev/eventing-kafka/pkg/controller/env"
	"os"
)

// The Environment Struct
type Environment struct {
	MetricsPort   string
	HealthPort    string
	KafkaBrokers  string
	KafkaUsername string
	KafkaPassword string
	ServiceName   string
}

// Load & Return The Environment Variables
func GetEnvironment(logger *zap.Logger) (Environment, error) {

	// Create The Environment With Current Values
	environment := Environment{
		MetricsPort:   os.Getenv(env.MetricsPortEnvVarKey),
		HealthPort:    os.Getenv(env.HealthPortEnvVarKey),
		KafkaBrokers:  os.Getenv(env.KafkaBrokerEnvVarKey),
		KafkaUsername: os.Getenv(env.KafkaUsernameEnvVarKey),
		KafkaPassword: os.Getenv(env.KafkaPasswordEnvVarKey),
		ServiceName:   os.Getenv(env.ServiceNameEnvVarKey),
	}

	// Safely Log The ControllerConfig Loaded From Environment Variables
	safeEnvironment := environment
	safeEnvironment.KafkaPassword = ""
	if len(environment.KafkaPassword) > 0 {
		safeEnvironment.KafkaPassword = "********"
	}
	logger.Info("Environment Variables", zap.Any("Environment", safeEnvironment))

	// Validate The Environment Variables
	err := validateEnvironment(logger, environment)

	// Return Results
	return environment, err
}

// Validate The Specified Environment Variables
func validateEnvironment(logger *zap.Logger, environment Environment) error {

	valid := validateRequiredEnvironmentVariable(logger, env.MetricsPortEnvVarKey, environment.MetricsPort) &&
		validateRequiredEnvironmentVariable(logger, env.HealthPortEnvVarKey, environment.HealthPort) &&
		validateRequiredEnvironmentVariable(logger, env.KafkaBrokerEnvVarKey, environment.KafkaBrokers) &&
		validateRequiredEnvironmentVariable(logger, env.ServiceNameEnvVarKey, environment.ServiceName)

	if valid {
		return nil
	} else {
		return errors.New("invalid / incomplete environment variables")
	}
}

// Log The Missing Required Environment Variable With Specified Key
func validateRequiredEnvironmentVariable(logger *zap.Logger, key string, value string) bool {
	if len(value) <= 0 {
		logger.Error("Missing Required Environment Variable", zap.String("Key", key))
		return false
	} else {
		return true
	}
}
