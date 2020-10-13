package env

import (
	"go.uber.org/zap"
	"knative.dev/eventing-kafka/pkg/channel/distributed/common/env"
)

// Package Constants
const (

	// Dispatcher Configuration
	DispatcherImageEnvVarKey = "DISPATCHER_IMAGE"

	// Receiver Configuration
	ReceiverImageEnvVarKey = "RECEIVER_IMAGE"
)

// Environment Structure
type Environment struct {

	// Eventing-kafka Configuration
	ServiceAccount string // Required
	MetricsPort    int    // Required
	MetricsDomain  string // Required

	// Dispatcher Configuration
	DispatcherImage string // Required

	// Receiver Configuration
	ReceiverImage string // Required
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
	// Receiver Configuration
	//

	// Get The Required ReceiverImage Config Value
	environment.ReceiverImage, err = env.GetRequiredConfigValue(logger, ReceiverImageEnvVarKey)
	if err != nil {
		return nil, err
	}

	// Log The ControllerConfig Loaded From Environment Variables
	logger.Info("Environment Variables", zap.Any("Environment", environment))

	// Return The Populated ControllerConfig
	return environment, nil
}
