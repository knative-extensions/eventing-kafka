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
	"fmt"
	"os"
	"strconv"

	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/api/resource"
)

// Get The Specified Required Config Value From OS & Log Errors If Not Present
func GetRequiredConfigValue(logger *zap.Logger, key string) (string, error) {
	value := os.Getenv(key)
	if len(value) > 0 {
		return value, nil
	} else {
		logger.Error("Missing Required Environment Variable", zap.String("key", key))
		return "", fmt.Errorf("missing required environment variable '%s'", key)
	}
}

// Get The Specified Required Config Value From OS & Log Errors If Not Present Or Not An Int
func GetRequiredConfigInt(logger *zap.Logger, envKey string, name string) (int, error) {
	envString, err := GetRequiredConfigValue(logger, envKey)
	if err != nil {
		return 0, err
	}
	envInt, err := strconv.Atoi(envString)
	if err != nil {
		logger.Error("Invalid "+name+" (Non Int)", zap.String("Value", envString), zap.Error(err))
		return 0, fmt.Errorf("invalid (non int) value '%s' for environment variable '%s'", envString, envKey)
	}
	return envInt, nil
}

// Get The Specified Required Config Value From OS & Log Errors If Not Present Or Not An Int16
func GetRequiredConfigInt16(logger *zap.Logger, envKey string, name string) (int16, error) {
	envString, err := GetRequiredConfigValue(logger, envKey)
	if err != nil {
		return 0, err
	}
	envInt, err := strconv.ParseInt(envString, 10, 16)
	if err != nil {
		logger.Error("Invalid "+name+" (Non Int16)", zap.String("Value", envString), zap.Error(err))
		return 0, fmt.Errorf("invalid (non int16) value '%s' for environment variable '%s'", envString, envKey)
	}
	return int16(envInt), nil
}

// Get The Specified Required Config Value From OS & Log Errors If Not Present Or Not An Int32
func GetRequiredConfigInt32(logger *zap.Logger, envKey string, name string) (int32, error) {
	envString, err := GetRequiredConfigValue(logger, envKey)
	if err != nil {
		return 0, err
	}
	envInt, err := strconv.ParseInt(envString, 10, 32)
	if err != nil {
		logger.Error("Invalid "+name+" (Non Int32)", zap.String("Value", envString), zap.Error(err))
		return 0, fmt.Errorf("invalid (non int32) value '%s' for environment variable '%s'", envString, envKey)
	}
	return int32(envInt), nil
}

// Get The Specified Required Config Value From OS & Log Errors If Not Present Or Not An Int64
func GetRequiredConfigInt64(logger *zap.Logger, envKey string, name string) (int64, error) {
	envString, err := GetRequiredConfigValue(logger, envKey)
	if err != nil {
		return 0, err
	}
	envInt, err := strconv.ParseInt(envString, 10, 64)
	if err != nil {
		logger.Error("Invalid "+name+" (Non Int64)", zap.String("Value", envString), zap.Error(err))
		return 0, fmt.Errorf("invalid (non int64) value '%s' for environment variable '%s'", envString, envKey)
	}
	return envInt, nil
}

// Get The Specified Required Config Value From OS & Log Errors If Not Present Or Not Boolean
func GetRequiredConfigBool(logger *zap.Logger, envKey string, name string) (bool, bool, error) {
	envString, err := GetRequiredConfigValue(logger, envKey)
	if err != nil {
		return false, false, err
	}
	envBool, err := strconv.ParseBool(envString)
	if err != nil {
		logger.Error("Invalid "+name+" (Non Integer)", zap.String("Value", envString), zap.Error(err))
		return false, false, fmt.Errorf("invalid (non boolean) value '%s' for environment variable '%s'", envString, envKey)
	}
	return envBool, true, nil
}

// Get The Specified Optional Config Value From OS
func GetOptionalConfigValue(logger *zap.Logger, key string, defaultValue string) string {
	value := os.Getenv(key)
	if len(value) <= 0 {
		logger.Info("Optional Environment Variable Not Specified - Using Default", zap.String("key", key), zap.String("value", defaultValue))
		value = defaultValue
	}
	return value
}

// Get The Specified Optional Config Value From OS & Log Errors If Not Present Or Not Boolean
func GetOptionalConfigBool(logger *zap.Logger, envKey string, defaultValue string, name string) (bool, error) {
	envString := GetOptionalConfigValue(logger, envKey, defaultValue)
	envBool, err := strconv.ParseBool(envString)
	if err != nil {
		logger.Error("Invalid "+name+" (Non Boolean)", zap.String("Value", envString), zap.Error(err))
		return false, fmt.Errorf("invalid (non boolean) value '%s' for environment variable '%s'", envString, envKey)
	}
	return envBool, nil
}

// Get The Specified Optional Config Value From OS & Log Errors If Not Present Or Not An Int64
func GetOptionalConfigInt64(logger *zap.Logger, envKey string, defaultValue string, name string) (int64, error) {
	envString := GetOptionalConfigValue(logger, envKey, defaultValue)
	envInt, err := strconv.ParseInt(envString, 10, 64)
	if err != nil {
		logger.Error("Invalid "+name+" (Non Int64)", zap.String("Value", envString), zap.Error(err))
		return 0, fmt.Errorf("invalid (non int64) value '%s' for environment variable '%s'", envString, envKey)
	}
	return envInt, nil
}

// Get The Specified Optional Config Value From OS & Log Errors If Not Present Or Not An Int
func GetOptionalConfigInt(logger *zap.Logger, envKey string, defaultValue string, name string) (int, error) {
	envString := GetOptionalConfigValue(logger, envKey, defaultValue)
	envInt, err := strconv.Atoi(envString)
	if err != nil {
		logger.Error("Invalid "+name+" (Non Int)", zap.String("Value", envString), zap.Error(err))
		return 0, fmt.Errorf("invalid (non int) value '%s' for environment variable '%s'", envString, envKey)
	}
	return envInt, nil
}

// Parse Quantity Value
func GetRequiredQuantityConfigValue(logger *zap.Logger, envVarKey string) (*resource.Quantity, error) {

	// Get The Required Config Value As String
	value, err := GetRequiredConfigValue(logger, envVarKey)
	if err != nil {
		return nil, err
	}

	// Attempt To Parse The Value As A Quantity
	quantity, err := resource.ParseQuantity(value)
	if err != nil {
		message := fmt.Sprintf("invalid (non quantity) value '%s' for environment variable '%s'", value, envVarKey)
		logger.Error(message, zap.Error(err))
		return nil, fmt.Errorf(message)
	}

	// Return The Parsed Quantity
	return &quantity, nil
}
