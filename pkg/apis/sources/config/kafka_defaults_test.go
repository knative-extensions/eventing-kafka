/*
Copyright 2021 The Knative Authors

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
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	. "knative.dev/pkg/configmap/testing"
	"knative.dev/pkg/system"
	_ "knative.dev/pkg/system/testing"
)

func TestNewKafkaDefaultsConfigFromConfigMap(t *testing.T) {
	_, example := ConfigMapsFromTestFile(t, KafkaDefaultsConfigName)
	if _, err := NewKafkaDefaultsConfigFromConfigMap(example); err != nil {
		t.Error("NewKafkaDefaultsConfigFromMap(example) =", err)
	}
}

func TestKafkaDefaultsConfiguration(t *testing.T) {
	testCases := []struct {
		name                  string
		wantErr               bool
		wantDefault           KafkaSourceDefaults
		wantClass             string
		wantMinScale          int64
		wantMaxScale          int64
		wantPollingInterval   int64
		wantCooldownPeriod    int64
		wantKafkaLagThreshold int64
		config                *corev1.ConfigMap
	}{{
		name:    "default config",
		wantErr: false,
		wantDefault: KafkaSourceDefaults{
			AutoscalingClass: "",
		},
		config: &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: system.Namespace(),
				Name:      KafkaDefaultsConfigName,
			},
			Data: map[string]string{},
		},
	}, {
		name:    "example text",
		wantErr: false,
		config: &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: system.Namespace(),
				Name:      KafkaDefaultsConfigName,
			},
			Data: map[string]string{
				"_example": `
    ################################
    #                              #
    #    EXAMPLE CONFIGURATION     #
    #                              #
    ################################

    # This block is not actually functional configuration,
    # but serves to illustrate the available configuration
    # options and document them in a way that is accessible
    # to users that kubectl edit this config map.
    #
    # These sample configuration options may be copied out of
    # this example block and unindented to be in the data block
    # to actually change the configuration.

    # autoscalingClass is the autoscaler class name to use.
    # valid value: keda.autoscaling.knative.dev
    autoscalingClass: "keda.autoscaling.knative.dev"

    # minScale is the minimum number of replicas to scale down to.
    # minScale: "1"

    # maxScale is the maximum number of replicas to scale up to.
    # maxScale: "1"

    # pollingInterval is the interval in seconds KEDA uses to poll metrics.
    # pollingInterval: "30"

    # cooldownPeriod is the period of time in seconds KEDA waits until it scales down.
    # cooldownPeriod: "300"

    # kafkaLagThreshold is the lag (ie. number of messages in a partition) threshold for KEDA to scale up sources.
    # kafkaLagThreshold: "10"
`,
			},
		},
	}, {
		name:    "valid autoscaler class",
		wantErr: false,
		wantDefault: KafkaSourceDefaults{
			AutoscalingClass:  "keda.autoscaling.knative.dev",
			MinScale:          DefaultMinScaleValue,
			MaxScale:          DefaultMaxScaleValue,
			PollingInterval:   DefaultPollingIntervalValue,
			CooldownPeriod:    DefaultCooldownPeriodValue,
			KafkaLagThreshold: DefaultKafkaLagThresholdValue,
		},
		config: &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: system.Namespace(),
				Name:      KafkaDefaultsConfigName,
			},
			Data: map[string]string{
				"autoscalingClass": "keda.autoscaling.knative.dev",
			},
		},
	}, {
		name:    "invalid autoscaler class",
		wantErr: true,
		config: &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: system.Namespace(),
				Name:      KafkaDefaultsConfigName,
			},
			Data: map[string]string{
				"autoscalingClass": "invalid",
			},
		},
	}, {
		name:    "dangling key/value pair",
		wantErr: true,
		config: &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: system.Namespace(),
				Name:      KafkaDefaultsConfigName,
			},
			Data: map[string]string{
				"autoscalingClass": "#nothing to see here",
			},
		},
	}, {
		name:    "change minScale default",
		wantErr: false,
		wantDefault: KafkaSourceDefaults{
			AutoscalingClass:  "keda.autoscaling.knative.dev",
			MinScale:          40,
			MaxScale:          DefaultMaxScaleValue,
			PollingInterval:   DefaultPollingIntervalValue,
			CooldownPeriod:    DefaultCooldownPeriodValue,
			KafkaLagThreshold: DefaultKafkaLagThresholdValue,
		},
		config: &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: system.Namespace(),
				Name:      KafkaDefaultsConfigName,
			},
			Data: map[string]string{
				"autoscalingClass": "keda.autoscaling.knative.dev",
				"minScale":         "40",
			},
		},
	}, {
		name:    "change maxScale default",
		wantErr: false,
		wantDefault: KafkaSourceDefaults{
			AutoscalingClass:  "keda.autoscaling.knative.dev",
			MinScale:          DefaultMinScaleValue,
			MaxScale:          60,
			PollingInterval:   DefaultPollingIntervalValue,
			CooldownPeriod:    DefaultCooldownPeriodValue,
			KafkaLagThreshold: DefaultKafkaLagThresholdValue,
		},
		config: &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: system.Namespace(),
				Name:      KafkaDefaultsConfigName,
			},
			Data: map[string]string{
				"autoscalingClass": "keda.autoscaling.knative.dev",
				"maxScale":         "60",
			},
		},
	}, {
		name:    "change pollingInterval default",
		wantErr: false,
		wantDefault: KafkaSourceDefaults{
			AutoscalingClass:  "keda.autoscaling.knative.dev",
			MinScale:          DefaultMinScaleValue,
			MaxScale:          DefaultMaxScaleValue,
			PollingInterval:   500,
			CooldownPeriod:    DefaultCooldownPeriodValue,
			KafkaLagThreshold: DefaultKafkaLagThresholdValue,
		},
		config: &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: system.Namespace(),
				Name:      KafkaDefaultsConfigName,
			},
			Data: map[string]string{
				"autoscalingClass": "keda.autoscaling.knative.dev",
				"pollingInterval":  "500",
			},
		},
	}, {
		name:    "change cooldownPeriod default",
		wantErr: false,
		wantDefault: KafkaSourceDefaults{
			AutoscalingClass:  "keda.autoscaling.knative.dev",
			MinScale:          DefaultMinScaleValue,
			MaxScale:          DefaultMaxScaleValue,
			PollingInterval:   DefaultPollingIntervalValue,
			CooldownPeriod:    900,
			KafkaLagThreshold: DefaultKafkaLagThresholdValue,
		},
		config: &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: system.Namespace(),
				Name:      KafkaDefaultsConfigName,
			},
			Data: map[string]string{
				"autoscalingClass": "keda.autoscaling.knative.dev",
				"cooldownPeriod":   "900",
			},
		},
	}, {
		name:    "change kafkaLagThreshold default",
		wantErr: false,
		wantDefault: KafkaSourceDefaults{
			AutoscalingClass:  "keda.autoscaling.knative.dev",
			MinScale:          DefaultMinScaleValue,
			MaxScale:          DefaultMaxScaleValue,
			PollingInterval:   DefaultPollingIntervalValue,
			CooldownPeriod:    DefaultCooldownPeriodValue,
			KafkaLagThreshold: 800,
		},
		config: &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: system.Namespace(),
				Name:      KafkaDefaultsConfigName,
			},
			Data: map[string]string{
				"autoscalingClass":  "keda.autoscaling.knative.dev",
				"kafkaLagThreshold": "800",
			},
		},
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actualDefault, err := NewKafkaDefaultsConfigFromConfigMap(tc.config)

			if (err != nil) != tc.wantErr {
				t.Fatalf("Test: %q: NewKafkaDefaultsConfigFromMap() error = %v, wantErr %v", tc.name, err, tc.wantErr)
			}
			if !tc.wantErr {
				if diff := cmp.Diff(tc.wantDefault, *actualDefault); diff != "" {
					t.Error("unexpected value (-want, +got)", diff)
				}
			}
		})
	}
}
