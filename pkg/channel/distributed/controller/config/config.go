/*
Copyright 2020 The Knative Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package config

import (
	"strings"

	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/constants"
	commonconfig "knative.dev/eventing-kafka/pkg/common/config"
)

// ConfigurationError is the type of error returned from VerifyConfiguration
// when a setting is missing or invalid
type ControllerConfigurationError string

func (err ControllerConfigurationError) Error() string {
	return "controller: invalid configuration (" + string(err) + ")"
}

// VerifyConfiguration returns an error if mandatory fields in the EventingKafkaConfig have not been set either
// via the external configmap or the internal variables.
func VerifyConfiguration(configuration *commonconfig.EventingKafkaConfig) error {

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
	case configuration.Dispatcher.Replicas < 1:
		return ControllerConfigurationError("Dispatcher.Replicas must be > 0")
	case configuration.Receiver.Replicas < 1:
		return ControllerConfigurationError("Receiver.Replicas must be > 0")
	}
	return nil // no problems found
}
