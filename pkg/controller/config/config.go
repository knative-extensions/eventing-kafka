package config

import (
	"strings"

	"k8s.io/apimachinery/pkg/api/resource"
	"knative.dev/eventing-kafka/pkg/common/config"
	"knative.dev/eventing-kafka/pkg/controller/constants"
)

// ConfigurationError is the type of error returned from VerifyOverrides
// when a setting is missing or invalid
type ControllerConfigurationError string

func (err ControllerConfigurationError) Error() string {
	return "controller: invalid configuration (" + string(err) + ")"
}

// VerifyConfiguration returns an error if mandatory fields in the EventingKafkaConfig have not been set either
// via the external configmap or the internal variables.
func VerifyConfiguration(configuration *config.EventingKafkaConfig) error {

	// Verify & Lowercase The Kafka AdminType
	lowercaseKafkaAdminType := strings.ToLower(configuration.Kafka.AdminType)
	switch lowercaseKafkaAdminType {
	case constants.KafkaAdminTypeValueKafka, constants.KafkaAdminTypeValueAzure, constants.KafkaAdminTypeValueCustom:
		configuration.Kafka.AdminType = lowercaseKafkaAdminType
	default:
		return ControllerConfigurationError("Invalid / Unknown Kafka Admin Type: " + configuration.Kafka.AdminType)
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
