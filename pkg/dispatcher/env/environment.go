package env

import (
	"go.uber.org/zap"
	"knative.dev/eventing-kafka/pkg/common/env"
)

// Environment Structure
type Environment struct {

	// Metrics Configuration
	MetricsPort   int    // Required
	MetricsDomain string // Required

	// Health Configuration
	HealthPort int // Required

	// Dispatcher Retry Settings
	ExponentialBackoff   bool  // Required
	ExpBackoffPresent    bool  // Derived
	MaxRetryTime         int64 // Required
	InitialRetryInterval int64 // Required

	// Kafka Configuration
	KafkaBrokers                    string // Required
	KafkaTopic                      string // Required
	ChannelKey                      string // Required
	ServiceName                     string // Required
	KafkaOffsetCommitMessageCount   int64  // Required
	KafkaOffsetCommitDurationMillis int64  // Required

	// Kafka Authorization
	KafkaUsername    string // Optional
	KafkaPassword    string // Optional
	KafkaPasswordLog string // Derived
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

	// Get The Required MaxRetryTime Config Value
	environment.ExponentialBackoff, environment.ExpBackoffPresent, err = env.GetRequiredConfigBool(logger, env.ExponentialBackoffEnvVarKey, "ExponentialBackoff")
	if err != nil {
		return nil, err
	}

	// Get The Required MaxRetryTime Config Value
	environment.MaxRetryTime, err = env.GetRequiredConfigInt64(logger, env.MaxRetryTimeEnvVarKey, "MaxRetryTime")
	if err != nil {
		return nil, err
	}

	// Get The Required InitialRetryInterval Config Value
	environment.InitialRetryInterval, err = env.GetRequiredConfigInt64(logger, env.InitialRetryIntervalEnvVarKey, "InitialRetryInterval")
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

	// Get The Required K8S KafkaOffsetCommitMessageCount Config Value
	environment.KafkaOffsetCommitMessageCount, err = env.GetRequiredConfigInt64(logger, env.KafkaOffsetCommitMessageCountEnvVarKey, "KafkaOffsetCommitMessageCount")
	if err != nil {
		return nil, err
	}

	// Get The Required K8S KafkaOffsetCommitDurationMillis Config Value
	environment.KafkaOffsetCommitDurationMillis, err = env.GetRequiredConfigInt64(logger, env.KafkaOffsetCommitDurationMillisEnvVarKey, "KafkaOffsetCommitDurationMillis")
	if err != nil {
		return nil, err
	}

	// Get The Optional KafkaUsername Config Value
	environment.KafkaUsername = env.GetOptionalConfigValue(logger, env.KafkaUsernameEnvVarKey, "")
	if err != nil {
		return nil, err
	}

	// Get The Optional KafkaPassword Config Value
	environment.KafkaPassword = env.GetOptionalConfigValue(logger, env.KafkaPasswordEnvVarKey, "")
	if err != nil {
		return nil, err
	}

	// Mask The Password For Logging (If There Was One)
	environment.KafkaPasswordLog = ""
	if len(environment.KafkaPassword) > 0 {
		environment.KafkaPasswordLog = "*************"
	}

	// Log The Dispatcher Configuration Loaded From Environment Variables
	logger.Info("Environment Variables", zap.Any("Environment", environment))

	// Return The Populated Dispatcher Configuration Environment Structure
	return environment, nil
}
