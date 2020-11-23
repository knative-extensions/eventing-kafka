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

package env

import (
	"context"
	"errors"
	"knative.dev/pkg/system"
	"strconv"
	"time"

	"go.uber.org/zap"
	"knative.dev/eventing-kafka/pkg/channel/distributed/common/env"
	"knative.dev/pkg/controller"
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
	SystemNamespace string        // Required
	ServiceAccount  string        // Required
	MetricsPort     int           // Required
	MetricsDomain   string        // Required
	ResyncPeriod    time.Duration // Optional

	// Dispatcher Configuration
	DispatcherImage string // Required

	// Receiver Configuration
	ReceiverImage string // Required
}

// Key is used as the key for associating information with a context.Context.
type Key struct{}

// Get The Environment
func GetEnvironment(logger *zap.Logger) (*Environment, error) {

	// Error Reference
	var err error

	// The ControllerConfig Reference
	environment := &Environment{}

	// Get The Required System Namespace Config Value
	environment.SystemNamespace, err = env.GetRequiredConfigValue(logger, system.NamespaceEnvKey)
	if err != nil {
		return nil, err
	}

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

	// Get The Optional Resync Period config Value & Convert To Duration
	resyncMinutes, err := env.GetOptionalConfigInt(
		logger,
		env.ResyncPeriodMinutesEnvVarKey,
		strconv.Itoa(int(controller.DefaultResyncPeriod/time.Minute)),
		"ResyncPeriodMinutes")
	if err != nil {
		return nil, err
	}
	environment.ResyncPeriod = time.Duration(resyncMinutes) * time.Minute

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

// Obtain the Environment struct contained in a Context, if present
func FromContext(ctx context.Context) (*Environment, error) {
	environment, ok := ctx.Value(Key{}).(*Environment)
	if !ok {
		return nil, errors.New("could not extract Environment from context")
	}
	return environment, nil
}
