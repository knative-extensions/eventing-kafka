package env

import (
	"fmt"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/api/resource"
	"knative.dev/eventing-kafka/pkg/common/env"
	"strings"
)

// Package Constants
const (

	// Default Values To Use If Not Available In Knative Channels Argument
	DefaultNumPartitionsEnvVarKey     = "DEFAULT_NUM_PARTITIONS"
	DefaultReplicationFactorEnvVarKey = "DEFAULT_REPLICATION_FACTOR"
	DefaultRetentionMillisEnvVarKey   = "DEFAULT_RETENTION_MILLIS"

	// Dispatcher Event Retry Values
	DispatcherRetryInitialIntervalMillisEnvVarKey = "DISPATCHER_RETRY_INITIAL_INTERVAL_MILLIS"
	DispatcherRetryTimeMillisMaxEnvVarKey         = "DISPATCHER_RETRY_TIME_MILLIS"
	DispatcherRetryExponentialBackoffEnvVarKey    = "DISPATCHER_RETRY_EXPONENTIAL_BACKOFF"

	// Default Values If Optional Environment Variable Defaults Not Specified
	DefaultRetentionMillis                 = "604800000" // 1 Week
	DefaultEventRetryInitialIntervalMillis = "500"       // 0.5 seconds
	DefaultEventRetryTimeMillisMax         = "300000"    // 5 minutes
	DefaultExponentialBackoff              = "true"      // Enabled

	// Kafka Provider Types
	KafkaProviderValueLocal     = "local"
	KafkaProviderValueConfluent = "confluent"
	KafkaProviderValueAzure     = "azure"

	// Dispatcher Resources
	DispatcherImageEnvVarKey         = "DISPATCHER_IMAGE"
	DispatcherReplicasEnvVarKey      = "DISPATCHER_REPLICAS"
	DispatcherCpuRequestEnvVarKey    = "DISPATCHER_CPU_REQUEST"
	DispatcherCpuLimitEnvVarKey      = "DISPATCHER_CPU_LIMIT"
	DispatcherMemoryRequestEnvVarKey = "DISPATCHER_MEMORY_REQUEST"
	DispatcherMemoryLimitEnvVarKey   = "DISPATCHER_MEMORY_LIMIT"

	// Channel Resources
	ChannelImageEnvVarKey         = "CHANNEL_IMAGE"
	ChannelReplicasEnvVarKey      = "CHANNEL_REPLICAS"
	ChannelMemoryRequestEnvVarKey = "CHANNEL_MEMORY_REQUEST"
	ChannelMemoryLimitEnvVarKey   = "CHANNEL_MEMORY_LIMIT"
	ChannelCpuRequestEnvVarKey    = "CHANNEL_CPU_REQUEST"
	ChannelCpuLimitEnvVarKey      = "CHANNEL_CPU_LIMIT"
)

// Environment Structure
type Environment struct {

	// Eventing-kafka Configuration
	ServiceAccount string // Required
	MetricsPort    int    // Required
	MetricsDomain  string // Required

	// Kafka Configuration / Authorization
	KafkaProvider                   string // Required
	KafkaOffsetCommitMessageCount   int64  // Optional
	KafkaOffsetCommitDurationMillis int64  // Optional

	// Default Values To Use If Not Available In Knative Channels Argument
	DefaultNumPartitions     int32 // Required
	DefaultReplicationFactor int16 // Required
	DefaultRetentionMillis   int64 // Optional

	// Resource configuration
	DispatcherImage         string            // Required
	DispatcherReplicas      int               // Required
	DispatcherMemoryRequest resource.Quantity // Required
	DispatcherMemoryLimit   resource.Quantity // Required
	DispatcherCpuRequest    resource.Quantity // Required
	DispatcherCpuLimit      resource.Quantity // Required

	// Dispatcher Retry Settings
	DispatcherRetryInitialIntervalMillis int64 // Optional
	DispatcherRetryTimeMillisMax         int64 // Optional
	DispatcherRetryExponentialBackoff    bool  // Optional

	// Resource Limits for each Channel Deployment
	ChannelImage         string            // Required
	ChannelReplicas      int               // Required
	ChannelMemoryRequest resource.Quantity // Required
	ChannelMemoryLimit   resource.Quantity // Required
	ChannelCpuRequest    resource.Quantity // Required
	ChannelCpuLimit      resource.Quantity // Required
}

// Get The Environment
func GetEnvironment(logger *zap.Logger) (*Environment, error) {

	// Error Reference
	var err error

	// The ControllerConfig Reference
	environment := &Environment{}

	// Get The Required K8S ServiceAccount Config Value
	environment.ServiceAccount, err = env.GetRequiredConfigValue(logger, env.ServiceAccountEnvVarKey)
	if err != nil {
		return nil, err
	}

	// Get The Required Metrics Domain Config Value
	environment.MetricsDomain, err = env.GetRequiredConfigValue(logger, env.MetricsDomainEnvVarKey)
	if err != nil {
		return nil, err
	}

	// Get The Required Metrics Port Config Value & Convert To Int
	environment.MetricsPort, err = env.GetRequiredConfigInt(logger, env.MetricsPortEnvVarKey, "MetricsPort")
	if err != nil {
		return nil, err
	}

	// Get The Required Kafka Provider Config Value
	kafkaProviderString, err := env.GetRequiredConfigValue(logger, env.KafkaProviderEnvVarKey)
	if err != nil {
		return nil, err
	} else {
		switch strings.ToLower(kafkaProviderString) {
		case KafkaProviderValueLocal:
			environment.KafkaProvider = KafkaProviderValueLocal
		case KafkaProviderValueConfluent:
			environment.KafkaProvider = KafkaProviderValueConfluent
		case KafkaProviderValueAzure:
			environment.KafkaProvider = KafkaProviderValueAzure
		default:
			logger.Error("Invalid / Unknown KafkaProvider", zap.String("Value", kafkaProviderString), zap.Error(err))
			return nil, fmt.Errorf("invalid (unknown) value '%s' for environment variable '%s'", kafkaProviderString, env.KafkaProviderEnvVarKey)
		}
	}

	// Get The Required DefaultNumPartitions Config Value & Convert To Int
	environment.DefaultNumPartitions, err = env.GetRequiredConfigInt32(logger, DefaultNumPartitionsEnvVarKey, "DefaultNumPartitions")
	if err != nil {
		return nil, err
	}

	// Get The Required DefaultReplicationFactor Config Value & Convert To Int
	environment.DefaultReplicationFactor, err = env.GetRequiredConfigInt16(logger, DefaultReplicationFactorEnvVarKey, "DefaultReplicationFactor")
	if err != nil {
		return nil, err
	}

	// Get The Optional DefaultRetentionMillis Config Value & Convert To Int
	environment.DefaultRetentionMillis, err = env.GetOptionalConfigInt64(logger, DefaultRetentionMillisEnvVarKey, DefaultRetentionMillis, "DefaultRetentionMillis")
	if err != nil {
		return nil, err
	}

	//
	// Dispatcher Configuration
	//

	// Get The Required DispatcherImage Config Value
	environment.DispatcherImage, err = env.GetRequiredConfigValue(logger, DispatcherImageEnvVarKey)
	if err != nil {
		return nil, err
	}

	// Get The Required DispatcherReplicas Config Value & Convert To Int
	environment.DispatcherReplicas, err = env.GetRequiredConfigInt(logger, DispatcherReplicasEnvVarKey, "DispatcherReplicas")
	if err != nil {
		return nil, err
	}

	// Get The Required DispatcherMemoryRequest Config Value
	quantity, err := env.GetRequiredQuantityConfigValue(logger, DispatcherMemoryRequestEnvVarKey)
	if err != nil {
		return nil, err
	} else {
		environment.DispatcherMemoryRequest = *quantity
	}

	// Get The Required DispatcherMemoryLimit Config Value
	quantity, err = env.GetRequiredQuantityConfigValue(logger, DispatcherMemoryLimitEnvVarKey)
	if err != nil {
		return nil, err
	} else {
		environment.DispatcherMemoryLimit = *quantity
	}

	// Get The Required DispatcherCpuRequest Config Value
	quantity, err = env.GetRequiredQuantityConfigValue(logger, DispatcherCpuRequestEnvVarKey)
	if err != nil {
		return nil, err
	} else {
		environment.DispatcherCpuRequest = *quantity
	}

	// Get The Required DispatcherCpuLimit Config Value
	quantity, err = env.GetRequiredQuantityConfigValue(logger, DispatcherCpuLimitEnvVarKey)
	if err != nil {
		return nil, err
	} else {
		environment.DispatcherCpuLimit = *quantity
	}

	// Get The Optional DispatcherRetryInitialIntervalMillis Config Value & Convert To Int
	environment.DispatcherRetryInitialIntervalMillis, err = env.GetOptionalConfigInt64(logger, DispatcherRetryInitialIntervalMillisEnvVarKey, DefaultEventRetryInitialIntervalMillis, "DispatcherRetryInitialIntervalMillis")
	if err != nil {
		return nil, err
	}

	// Get The Optional DispatcherRetryTimeMillisMax Config Value & Convert To Int
	environment.DispatcherRetryTimeMillisMax, err = env.GetOptionalConfigInt64(logger, DispatcherRetryTimeMillisMaxEnvVarKey, DefaultEventRetryTimeMillisMax, "DispatcherRetryTimeMillisMax")
	if err != nil {
		return nil, err
	}

	// Get The Optional DispatcherRetryExponentialBackoff Config Value & Convert To Bool
	environment.DispatcherRetryExponentialBackoff, err = env.GetOptionalConfigBool(logger, DispatcherRetryExponentialBackoffEnvVarKey, DefaultExponentialBackoff, "DispatcherRetryExponentialBackoff")
	if err != nil {
		return nil, err
	}

	//
	// Channel Configuration
	//

	// Get The Required ChannelImage Config Value
	environment.ChannelImage, err = env.GetRequiredConfigValue(logger, ChannelImageEnvVarKey)
	if err != nil {
		return nil, err
	}

	// Get The Required ChannelReplicas Config Value & Convert To Int
	environment.ChannelReplicas, err = env.GetRequiredConfigInt(logger, ChannelReplicasEnvVarKey, "ChannelReplicas")
	if err != nil {
		return nil, err
	}

	quantity, err = env.GetRequiredQuantityConfigValue(logger, ChannelMemoryRequestEnvVarKey)
	if err != nil {
		return nil, err
	} else {
		environment.ChannelMemoryRequest = *quantity
	}

	quantity, err = env.GetRequiredQuantityConfigValue(logger, ChannelMemoryLimitEnvVarKey)
	if err != nil {
		return nil, err
	} else {
		environment.ChannelMemoryLimit = *quantity
	}

	quantity, err = env.GetRequiredQuantityConfigValue(logger, ChannelCpuRequestEnvVarKey)
	if err != nil {
		return nil, err
	} else {
		environment.ChannelCpuRequest = *quantity
	}

	quantity, err = env.GetRequiredQuantityConfigValue(logger, ChannelCpuLimitEnvVarKey)
	if err != nil {
		return nil, err
	} else {
		environment.ChannelCpuLimit = *quantity
	}

	// Log The ControllerConfig Loaded From Environment Variables
	logger.Info("Environment Variables", zap.Any("Environment", environment))

	// Return The Populated ControllerConfig
	return environment, nil
}
