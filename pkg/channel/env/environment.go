package env

import (
	"errors"
	"go.uber.org/zap"
	"os"
)

// Environment Constants
const (

	// Environment Variable Keys
	MetricsPortEnvVarKey   = "METRICS_PORT"
	HealthPortEnvVarKey    = "HEALTH_PORT"
	KafkaBrokersEnvVarKey  = "KAFKA_BROKERS"
	KafkaUsernameEnvVarKey = "KAFKA_USERNAME"
	KafkaPasswordEnvVarKey = "KAFKA_PASSWORD"
)

// The Environment Struct
type Environment struct {
	MetricsPort   string
	HealthPort    string
	KafkaBrokers  string
	KafkaUsername string
	KafkaPassword string
}

// Load & Return The Environment Variables
func GetEnvironment(logger *zap.Logger) (Environment, error) {

	// Create The Environment With Current Values
	environment := Environment{
		MetricsPort:   os.Getenv(MetricsPortEnvVarKey),
		HealthPort:    os.Getenv(HealthPortEnvVarKey),
		KafkaBrokers:  os.Getenv(KafkaBrokersEnvVarKey),
		KafkaUsername: os.Getenv(KafkaUsernameEnvVarKey),
		KafkaPassword: os.Getenv(KafkaPasswordEnvVarKey),
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

	valid := validateRequiredEnvironmentVariable(logger, MetricsPortEnvVarKey, environment.MetricsPort) &&
		validateRequiredEnvironmentVariable(logger, HealthPortEnvVarKey, environment.HealthPort) &&
		validateRequiredEnvironmentVariable(logger, KafkaBrokersEnvVarKey, environment.KafkaBrokers)

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
