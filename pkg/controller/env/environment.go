package env

import (
	"strings"

	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/api/resource"
	"knative.dev/eventing-kafka/pkg/common/config"
	"knative.dev/eventing-kafka/pkg/common/env"
)

// Package Constants
const (

	// Kafka Provider Types
	KafkaProviderValueLocal     = "local"
	KafkaProviderValueConfluent = "confluent"
	KafkaProviderValueAzure     = "azure"

	// Dispatcher Resources
	DispatcherImageEnvVarKey = "DISPATCHER_IMAGE"

	// Channel Resources
	ChannelImageEnvVarKey = "CHANNEL_IMAGE"
)

// Environment Structure
type Environment struct {

	// Eventing-kafka Configuration
	ServiceAccount string // Required
	MetricsPort    int    // Required
	MetricsDomain  string // Required

	// Resource configuration
	DispatcherImage string // Required

	// Resource Limits for each Channel Deployment
	ChannelImage string // Required
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

	//
	// Dispatcher Configuration
	//

	// Get The Required DispatcherImage Config Value
	environment.DispatcherImage, err = env.GetRequiredConfigValue(logger, DispatcherImageEnvVarKey)
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

	// Log The ControllerConfig Loaded From Environment Variables
	logger.Info("Environment Variables", zap.Any("Environment", environment))

	// Return The Populated ControllerConfig
	return environment, nil
}

// ConfigurationError is the type of error returned from VerifyOverrides
// when a setting is missing or invalid
type ControllerConfigurationError string

func (err ControllerConfigurationError) Error() string {
	return "controller: invalid configuration (" + string(err) + ")"
}

// VerifyConfiguration returns an error if mandatory fields in the EventingKafkaConfig have not been set either
// via the external configmap or the internal variables.
func VerifyConfiguration(configuration *config.EventingKafkaConfig) error {
	switch strings.ToLower(configuration.Kafka.Provider) {
	case KafkaProviderValueLocal:
		configuration.Kafka.Provider = KafkaProviderValueLocal
	case KafkaProviderValueConfluent:
		configuration.Kafka.Provider = KafkaProviderValueConfluent
	case KafkaProviderValueAzure:
		configuration.Kafka.Provider = KafkaProviderValueAzure
	default:
		return ControllerConfigurationError("Invalid / Unknown KafkaProvider: " + configuration.Kafka.Provider)
	}

	// Verify mandatory configuration settings
	switch {
	case configuration.Kafka.Topic.DefaultNumPartitions < 1:
		return ControllerConfigurationError("Kafka.Topic.DefaultNumPartitions must be > 0")
	case configuration.Kafka.Topic.DefaultReplicationFactor < 1:
		return ControllerConfigurationError("Kafka.Topic.DefaultReplicationFactor must be > 0")
	case configuration.Kafka.Topic.DefaultRetentionMillis < 1:
		return ControllerConfigurationError("Kafka.Topic.DefaultRetentionMillis must be > 0")
	case configuration.Dispatcher.CpuLimit == resource.Quantity{}:
		return ControllerConfigurationError("Dispatcher.CpuLimit must be nonzero")
	case configuration.Dispatcher.CpuRequest == resource.Quantity{}:
		return ControllerConfigurationError("Dispatcher.CpuRequest must be nonzero")
	case configuration.Dispatcher.MemoryLimit == resource.Quantity{}:
		return ControllerConfigurationError("Dispatcher.MemoryLimit must be nonzero")
	case configuration.Dispatcher.MemoryRequest == resource.Quantity{}:
		return ControllerConfigurationError("Dispatcher.MemoryRequest must be nonzero")
	case configuration.Dispatcher.Replicas < 1:
		return ControllerConfigurationError("Dispatcher.Replicas must be > 0")
	case configuration.Channel.CpuLimit == resource.Quantity{}:
		return ControllerConfigurationError("Channel.CpuLimit must be nonzero")
	case configuration.Channel.CpuRequest == resource.Quantity{}:
		return ControllerConfigurationError("Channel.CpuRequest must be nonzero")
	case configuration.Channel.MemoryLimit == resource.Quantity{}:
		return ControllerConfigurationError("Channel.MemoryLimit must be nonzero")
	case configuration.Channel.MemoryRequest == resource.Quantity{}:
		return ControllerConfigurationError("Channel.MemoryRequest must be nonzero")
	case configuration.Channel.Replicas < 1:
		return ControllerConfigurationError("Channel.Replicas must be > 0")
	}
	return nil // no problems found
}
