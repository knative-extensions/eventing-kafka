package env

import (
	"errors"
	"go.uber.org/zap"
	commonenv "knative.dev/eventing-kafka/pkg/common/env"
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
		MetricsPort:   os.Getenv(commonenv.MetricsPortEnvVarKey),
		HealthPort:    os.Getenv(commonenv.HealthPortEnvVarKey),
		KafkaBrokers:  os.Getenv(commonenv.KafkaBrokerEnvVarKey),
		KafkaUsername: os.Getenv(commonenv.KafkaUsernameEnvVarKey),
		KafkaPassword: os.Getenv(commonenv.KafkaPasswordEnvVarKey),
		ServiceName:   os.Getenv(commonenv.ServiceNameEnvVarKey),
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

	valid := validateRequiredEnvironmentVariable(logger, commonenv.MetricsPortEnvVarKey, environment.MetricsPort) &&
		validateRequiredEnvironmentVariable(logger, commonenv.HealthPortEnvVarKey, environment.HealthPort) &&
		validateRequiredEnvironmentVariable(logger, commonenv.KafkaBrokerEnvVarKey, environment.KafkaBrokers) &&
		validateRequiredEnvironmentVariable(logger, commonenv.ServiceNameEnvVarKey, environment.ServiceName)

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
