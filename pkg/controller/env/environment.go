package env

import (
	"fmt"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/api/resource"
	"os"
	"strconv"
	"strings"
)

// Package Constants
const (
	// Knative-Kafka Configuration
	ServiceAccountEnvVarKey = "SERVICE_ACCOUNT"
	MetricsPortEnvVarKey    = "METRICS_PORT"
	HealthPortEnvVarKey     = "HEALTH_PORT"

	// Kafka Authorization
	KafkaBrokerEnvVarKey   = "KAFKA_BROKERS"
	KafkaUsernameEnvVarKey = "KAFKA_USERNAME"
	KafkaPasswordEnvVarKey = "KAFKA_PASSWORD"

	// Kafka Configuration
	KafkaProviderEnvVarKey                   = "KAFKA_PROVIDER"
	KafkaOffsetCommitMessageCountEnvVarKey   = "KAFKA_OFFSET_COMMIT_MESSAGE_COUNT"
	KafkaOffsetCommitDurationMillisEnvVarKey = "KAFKA_OFFSET_COMMIT_DURATION_MILLIS"
	KafkaTopicEnvVarKey                      = "KAFKA_TOPIC"

	// Dispatcher Configuration
	ChannelKeyEnvVarKey           = "CHANNEL_KEY"
	ExponentialBackoffEnvVarKey   = "EXPONENTIAL_BACKOFF"
	InitialRetryIntervalEnvVarKey = "INITIAL_RETRY_INTERVAL"
	MaxRetryTimeEnvVarKey         = "MAX_RETRY_TIME"

	// Default Values To Use If Not Available In Env Variables
	DefaultKafkaOffsetCommitMessageCount   = "100"
	DefaultKafkaOffsetCommitDurationMillis = "5000"

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

	// Knative-Kafka Configuration
	ServiceAccount string // Required
	MetricsPort    int    // Required

	// Kafka Configuration / Authorization
	KafkaProvider                   string // Required
	KafkaOffsetCommitMessageCount   int64  // Optional
	KafkaOffsetCommitDurationMillis int64  // Optional

	// Default Values To Use If Not Available In Knative Channels Argument
	DefaultNumPartitions     int   // Required
	DefaultReplicationFactor int   // Required
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
	environment.ServiceAccount, err = getRequiredConfigValue(logger, ServiceAccountEnvVarKey)
	if err != nil {
		return nil, err
	}

	// Get The Required Metrics Port Config Value & Convert To Int
	metricsPortString, err := getRequiredConfigValue(logger, MetricsPortEnvVarKey)
	if err != nil {
		return nil, err
	} else {
		environment.MetricsPort, err = strconv.Atoi(metricsPortString)
		if err != nil {
			logger.Error("Invalid MetricsPort (Non Integer)", zap.String("Value", metricsPortString), zap.Error(err))
			return nil, fmt.Errorf("invalid (non-integer) value '%s' for environment variable '%s'", metricsPortString, MetricsPortEnvVarKey)
		}
	}

	// Get The Required Kafka Provider Config Value
	kafkaProviderString, err := getRequiredConfigValue(logger, KafkaProviderEnvVarKey)
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
			return nil, fmt.Errorf("invalid (unknown) value '%s' for environment variable '%s'", kafkaProviderString, KafkaProviderEnvVarKey)
		}
	}

	// Get The Optional KafkaOffsetCommitMessageCount Config Value
	kafkaOffsetCommitMessageCountString := getOptionalConfigValue(logger, KafkaOffsetCommitMessageCountEnvVarKey, DefaultKafkaOffsetCommitMessageCount)
	environment.KafkaOffsetCommitMessageCount, err = strconv.ParseInt(kafkaOffsetCommitMessageCountString, 10, 64)
	if err != nil {
		logger.Error("Invalid KafkaOffsetCommitMessageCount (Non Integer)", zap.String("Value", kafkaOffsetCommitMessageCountString), zap.Error(err))
		return nil, fmt.Errorf("invalid (non-integer) value '%s' for environment variable '%s'", kafkaOffsetCommitMessageCountString, DefaultKafkaOffsetCommitMessageCount)
	}

	// Get The Optional KafkaOffsetCommitDurationMillis Config Value
	kafkaOffsetCommitDurationMillisString := getOptionalConfigValue(logger, KafkaOffsetCommitDurationMillisEnvVarKey, DefaultKafkaOffsetCommitDurationMillis)
	environment.KafkaOffsetCommitDurationMillis, err = strconv.ParseInt(kafkaOffsetCommitDurationMillisString, 10, 64)
	if err != nil {
		logger.Error("Invalid KafkaOffsetCommitDurationMillis (Non Integer)", zap.String("Value", kafkaOffsetCommitDurationMillisString), zap.Error(err))
		return nil, fmt.Errorf("invalid (non-integer) value '%s' for environment variable '%s'", kafkaOffsetCommitDurationMillisString, DefaultKafkaOffsetCommitDurationMillis)
	}

	// Get The Required DefaultNumPartitions Config Value & Convert To Int
	defaultNumPartitionsString, err := getRequiredConfigValue(logger, DefaultNumPartitionsEnvVarKey)
	if err != nil {
		return nil, err
	} else {
		environment.DefaultNumPartitions, err = strconv.Atoi(defaultNumPartitionsString)
		if err != nil {
			logger.Error("Invalid DefaultNumPartitions (Non Integer)", zap.String("Value", defaultNumPartitionsString), zap.Error(err))
			return nil, fmt.Errorf("invalid (non-integer) value '%s' for environment variable '%s'", defaultNumPartitionsString, DefaultNumPartitionsEnvVarKey)
		}
	}

	// Get The Required DefaultReplicationFactor Config Value & Convert To Int
	defaultReplicationFactorString, err := getRequiredConfigValue(logger, DefaultReplicationFactorEnvVarKey)
	if err != nil {
		return nil, err
	} else {
		environment.DefaultReplicationFactor, err = strconv.Atoi(defaultReplicationFactorString)
		if err != nil {
			logger.Error("Invalid DefaultReplicationFactor (Non Integer)", zap.String("Value", defaultReplicationFactorString), zap.Error(err))
			return nil, fmt.Errorf("invalid (non-integer) value '%s' for environment variable '%s'", defaultReplicationFactorString, DefaultReplicationFactorEnvVarKey)
		}
	}

	// Get The Optional DefaultRetentionMillis Config Value & Convert To Int
	defaultRetentionMillisString := getOptionalConfigValue(logger, DefaultRetentionMillisEnvVarKey, DefaultRetentionMillis)
	environment.DefaultRetentionMillis, err = strconv.ParseInt(defaultRetentionMillisString, 10, 64)
	if err != nil {
		logger.Error("Invalid DefaultRetentionMillis (Non Integer)", zap.String("Value", defaultRetentionMillisString), zap.Error(err))
		return nil, fmt.Errorf("invalid (non-integer) value '%s' for environment variable '%s'", defaultRetentionMillisString, DefaultRetentionMillisEnvVarKey)
	}

	//
	// Dispatcher Configuration
	//

	// Get The Required DispatcherImage Config Value
	environment.DispatcherImage, err = getRequiredConfigValue(logger, DispatcherImageEnvVarKey)
	if err != nil {
		return nil, err
	}

	// Get The Required DispatcherReplicas Config Value & Convert To Int
	dispatcherReplicasString, err := getRequiredConfigValue(logger, DispatcherReplicasEnvVarKey)
	if err != nil {
		return nil, err
	} else {
		environment.DispatcherReplicas, err = strconv.Atoi(dispatcherReplicasString)
		if err != nil {
			logger.Error("Invalid DispatcherRepli	cas (Non Integer)", zap.String("Value", dispatcherReplicasString), zap.Error(err))
			return nil, fmt.Errorf("invalid (non-integer) value '%s' for environment variable '%s'", dispatcherReplicasString, DispatcherReplicasEnvVarKey)
		}
	}

	quantity, err := getRequiredQuantityConfigValue(logger, DispatcherMemoryRequestEnvVarKey)
	if err != nil {
		return nil, err
	} else {
		environment.DispatcherMemoryRequest = *quantity
	}

	quantity, err = getRequiredQuantityConfigValue(logger, DispatcherMemoryLimitEnvVarKey)
	if err != nil {
		return nil, err
	} else {
		environment.DispatcherMemoryLimit = *quantity
	}

	quantity, err = getRequiredQuantityConfigValue(logger, DispatcherCpuRequestEnvVarKey)
	if err != nil {
		return nil, err
	} else {
		environment.DispatcherCpuRequest = *quantity
	}

	quantity, err = getRequiredQuantityConfigValue(logger, DispatcherCpuLimitEnvVarKey)
	if err != nil {
		return nil, err
	} else {
		environment.DispatcherCpuLimit = *quantity
	}

	// Get The Optional DispatcherRetryInitialIntervalMillis Config Value & Convert To Int
	dispatcherRetryInitialIntervalMillisString := getOptionalConfigValue(logger, DispatcherRetryInitialIntervalMillisEnvVarKey, DefaultEventRetryInitialIntervalMillis)
	environment.DispatcherRetryInitialIntervalMillis, err = strconv.ParseInt(dispatcherRetryInitialIntervalMillisString, 10, 64)
	if err != nil {
		logger.Error("Invalid DispatcherRetryInitialIntervalMillis (Non Integer)", zap.String("Value", dispatcherRetryInitialIntervalMillisString), zap.Error(err))
		return nil, fmt.Errorf("invalid (non-integer) value '%s' for environment variable '%s'", dispatcherRetryInitialIntervalMillisString, DispatcherRetryInitialIntervalMillisEnvVarKey)
	}

	// Get The Optional DispatcherRetryTimeMillisMax Config Value & Convert To Int
	dispatcherRetryTimeMillisMaxString := getOptionalConfigValue(logger, DispatcherRetryTimeMillisMaxEnvVarKey, DefaultEventRetryTimeMillisMax)
	environment.DispatcherRetryTimeMillisMax, err = strconv.ParseInt(dispatcherRetryTimeMillisMaxString, 10, 64)
	if err != nil {
		logger.Error("Invalid DispatcherRetryTimeMillisMax (Non Integer)", zap.String("Value", dispatcherRetryTimeMillisMaxString), zap.Error(err))
		return nil, fmt.Errorf("invalid (non-integer) value '%s' for environment variable '%s'", dispatcherRetryTimeMillisMaxString, DispatcherRetryTimeMillisMaxEnvVarKey)
	}

	// Get The Optional DispatcherRetryExponentialBackoff Config Value & Convert To Bool
	dispatcherRetryExponentialBackoffString := getOptionalConfigValue(logger, DispatcherRetryExponentialBackoffEnvVarKey, DefaultExponentialBackoff)
	environment.DispatcherRetryExponentialBackoff, err = strconv.ParseBool(dispatcherRetryExponentialBackoffString)
	if err != nil {
		logger.Error("Invalid DispatcherRetryExponentialBackoff (Non Boolean)", zap.String("Value", dispatcherRetryExponentialBackoffString), zap.Error(err))
		return nil, fmt.Errorf("invalid (non-boolean) value '%s' for environment variable '%s'", dispatcherRetryExponentialBackoffString, DispatcherRetryExponentialBackoffEnvVarKey)
	}

	//
	// Channel Configuration
	//

	// Get The Required ChannelImage Config Value
	environment.ChannelImage, err = getRequiredConfigValue(logger, ChannelImageEnvVarKey)
	if err != nil {
		return nil, err
	}

	// Get The Required ChannelReplicas Config Value & Convert To Int
	channelReplicasString, err := getRequiredConfigValue(logger, ChannelReplicasEnvVarKey)
	if err != nil {
		return nil, err
	} else {
		environment.ChannelReplicas, err = strconv.Atoi(channelReplicasString)
		if err != nil {
			logger.Error("Invalid ChannelReplicas (Non Integer)", zap.String("Value", channelReplicasString), zap.Error(err))
			return nil, fmt.Errorf("invalid (non-integer) value '%s' for environment variable '%s'", channelReplicasString, ChannelReplicasEnvVarKey)
		}
	}

	quantity, err = getRequiredQuantityConfigValue(logger, ChannelMemoryRequestEnvVarKey)
	if err != nil {
		return nil, err
	} else {
		environment.ChannelMemoryRequest = *quantity
	}

	quantity, err = getRequiredQuantityConfigValue(logger, ChannelMemoryLimitEnvVarKey)
	if err != nil {
		return nil, err
	} else {
		environment.ChannelMemoryLimit = *quantity
	}

	quantity, err = getRequiredQuantityConfigValue(logger, ChannelCpuRequestEnvVarKey)
	if err != nil {
		return nil, err
	} else {
		environment.ChannelCpuRequest = *quantity
	}

	quantity, err = getRequiredQuantityConfigValue(logger, ChannelCpuLimitEnvVarKey)
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

// Get The Specified Required Config Value From OS & Log Errors If Not Present
func getRequiredConfigValue(logger *zap.Logger, key string) (string, error) {
	value := os.Getenv(key)
	if len(value) > 0 {
		return value, nil
	} else {
		logger.Error("Missing Required Environment Variable", zap.String("key", key))
		return "", fmt.Errorf("missing required environment variable '%s'", key)
	}
}

// Get The Specified Optional Config Value From OS
func getOptionalConfigValue(logger *zap.Logger, key string, defaultValue string) string {
	value := os.Getenv(key)
	if len(value) <= 0 {
		logger.Info("Optional Environment Variable Not Specified - Using Default", zap.String("key", key), zap.String("value", defaultValue))
		value = defaultValue
	}
	return value
}

// Parse Quantity Value
func getRequiredQuantityConfigValue(logger *zap.Logger, envVarKey string) (*resource.Quantity, error) {

	// Get The Required Config Value As String
	value, err := getRequiredConfigValue(logger, envVarKey)
	if err != nil {
		return nil, err
	}

	// Attempt To Parse The Value As A Quantity
	quantity, err := resource.ParseQuantity(value)
	if err != nil {
		message := fmt.Sprintf("invalid (non-quantity) value '%s' for environment variable '%s'", value, envVarKey)
		logger.Error(message, zap.Error(err))
		return nil, fmt.Errorf(message)
	}

	// Return The Parsed Quantity
	return &quantity, nil
}
