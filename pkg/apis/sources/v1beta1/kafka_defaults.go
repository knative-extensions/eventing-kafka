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

package v1beta1

import (
	"context"
	"fmt"
	"strconv"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/pointer"
)

const (
	uuidPrefix                  = "knative-kafka-source-"
	DefaultMinScaleKey          = "minScale"
	DefaultMaxScaleKey          = "maxScale"
	DefaultPollingIntervalKey   = "pollingInterval"
	DefaultCooldownPeriodKey    = "cooldownPeriod"
	DefaultKafkaLagThresholdKey = "kafkaLagThreshold"

	DefaultMinScale          = -1
	DefaultMaxScale          = -1
	DefaultPollingInterval   = -1
	DefaultCooldownPeriod    = -1
	DefaultKafkaLagThreshold = -1

	KafkaKedaDefaultsConfigName = "config-kafka-defaults"
)

// SetDefaults ensures KafkaSource reflects the default values.
func (k *KafkaSource) SetDefaults(ctx context.Context) {
	if k.Spec.ConsumerGroup == "" {
		k.Spec.ConsumerGroup = uuidPrefix + uuid.New().String()
	}

	if k.Spec.Consumers == nil {
		k.Spec.Consumers = pointer.Int32Ptr(1)
	}

	if k.Spec.InitialOffset == "" {
		k.Spec.InitialOffset = OffsetLatest
	}
}

// NewPingDefaultsConfigFromMap creates a Defaults from the supplied Map
func NewKafkaKedaDefaultsConfigFromMap(data map[string]string) (*KafkaKedaDefaults, error) {
	nc := &KafkaKedaDefaults{
		MinScale:          DefaultMinScale,
		MaxScale:          DefaultMaxScale,
		PollingInterval:   DefaultPollingInterval,
		CooldownPeriod:    DefaultCooldownPeriod,
		KafkaLagThreshold: DefaultKafkaLagThreshold,
	}

	// Parse out the MaxSizeKey
	value, present := data[DefaultMinScaleKey]
	if !present || value == "" {
		return nc, nil
	}
	int64Value, err := strconv.ParseInt(value, 0, 64)
	if err != nil {
		return nil, fmt.Errorf("Failed to parse the entry: %s", err)
	}
	nc.MinScale = int64Value

	value, present = data[DefaultMaxScaleKey]
	if !present || value == "" {
		return nc, nil
	}
	int64Value, err = strconv.ParseInt(value, 0, 64)
	if err != nil {
		return nil, fmt.Errorf("Failed to parse the entry: %s", err)
	}
	nc.MaxScale = int64Value

	value, present = data[DefaultKafkaLagThresholdKey]
	if !present || value == "" {
		return nc, nil
	}
	int64Value, err = strconv.ParseInt(value, 0, 64)
	if err != nil {
		return nil, fmt.Errorf("Failed to parse the entry: %s", err)
	}
	nc.KafkaLagThreshold = int64Value

	value, present = data[DefaultPollingIntervalKey]
	if !present || value == "" {
		return nc, nil
	}
	int64Value, err = strconv.ParseInt(value, 0, 64)
	if err != nil {
		return nil, fmt.Errorf("Failed to parse the entry: %s", err)
	}
	nc.PollingInterval = int64Value

	value, present = data[DefaultCooldownPeriodKey]
	if !present || value == "" {
		return nc, nil
	}
	int64Value, err = strconv.ParseInt(value, 0, 64)
	if err != nil {
		return nil, fmt.Errorf("Failed to parse the entry: %s", err)
	}
	nc.CooldownPeriod = int64Value

	return nc, nil
}

// NewPingDefaultsConfigFromConfigMap creates a PingDefaults from the supplied configMap
func NewKafkaKedaDefaultsConfigFromConfigMap(config *corev1.ConfigMap) (*KafkaKedaDefaults, error) {
	return NewKafkaKedaDefaultsConfigFromMap(config.Data)
}

type KafkaKedaDefaults struct {
	MinScale          int64 `json:"minScale"`
	MaxScale          int64 `json:"maxScale"`
	PollingInterval   int64 `json:"pollingInterval"`
	CooldownPeriod    int64 `json:"cooldownPeriod"`
	KafkaLagThreshold int64 `json:"kafkaLagThreshold"`
}

func (d *KafkaKedaDefaults) DeepCopy() *KafkaKedaDefaults {
	if d == nil {
		return nil
	}
	out := new(KafkaKedaDefaults)
	*out = *d
	return out
}
