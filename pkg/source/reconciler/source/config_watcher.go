/*
Copyright 2019 The Knative Authors

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

package source

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"knative.dev/eventing-kafka/pkg/common/constants"
	"knative.dev/eventing/pkg/reconciler/source"
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/logging"
)

const (
	EnvKafkaCfg           = "K_KAFKA_CONFIG"
	kafkaConfigMapNameEnv = "CONFIG_KAFKA_NAME"
)

type KafkaConfig struct {
	SaramaYamlString string
}

type KafkaSourceConfigAccessor interface {
	source.ConfigAccessor
	KafkaConfig() *KafkaConfig
}

// ToEnvVars serializes the contents of the ConfigWatcher to individual
// environment variables.
func (cw *KafkaSourceConfigWatcher) ToEnvVars() []corev1.EnvVar {
	envs := cw.ConfigWatcher.ToEnvVars()

	envs = maybeAppendEnvVar(envs, cw.kafkaConfigEnvVar(), cw.KafkaConfig() != nil)

	return envs
}

// maybeAppendEnvVar appends an EnvVar only if the condition boolean is true.
func maybeAppendEnvVar(envs []corev1.EnvVar, env corev1.EnvVar, cond bool) []corev1.EnvVar {
	if !cond {
		return envs
	}

	return append(envs, env)
}

// KafkaConfig returns the logging configuration from the ConfigWatcher.
func (cw *KafkaSourceConfigWatcher) KafkaConfig() *KafkaConfig {
	if cw == nil {
		return nil
	}
	return cw.kafkaCfg
}

var _ KafkaSourceConfigAccessor = (*KafkaSourceConfigWatcher)(nil)

type KafkaSourceConfigWatcher struct {
	*source.ConfigWatcher
	logger *zap.SugaredLogger

	kafkaCfg *KafkaConfig
}

// WatchConfigurations returns a ConfigWatcher initialized with the given
// options. If no option is passed, the ConfigWatcher observes ConfigMaps for
// logging, metrics, tracing and Kafka.
func WatchConfigurations(loggingCtx context.Context, component string,
	cmw configmap.Watcher) *KafkaSourceConfigWatcher {

	configWatcher := source.WatchConfigurations(loggingCtx, component, cmw)

	cw := &KafkaSourceConfigWatcher{
		ConfigWatcher: configWatcher,
		logger:        logging.FromContext(loggingCtx),
		kafkaCfg:      nil,
	}

	WatchConfigMapWithKafka(cw, cmw)

	return cw
}

// WatchConfigMapWithKafka observes a Kafka ConfigMap.
func WatchConfigMapWithKafka(cw *KafkaSourceConfigWatcher, cmw configmap.Watcher) {
	cw.kafkaCfg = &KafkaConfig{}
	watchConfigMap(cmw, KafkaConfigMapName(), cw.updateFromKafkaConfigMap)
}

func watchConfigMap(cmw configmap.Watcher, cmName string, obs configmap.Observer) {
	if dcmw, ok := cmw.(configmap.DefaultingWatcher); ok {
		dcmw.WatchWithDefault(corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: cmName},
			Data:       map[string]string{},
		}, obs)

	} else {
		cmw.Watch(cmName, obs)
	}
}

func (cw *KafkaSourceConfigWatcher) updateFromKafkaConfigMap(cfg *corev1.ConfigMap) {
	kafkaCfg, err := NewKafkaConfigFromConfigMap(cfg)
	if err != nil {
		cw.logger.Warnw("ignoring configuration in Kafka configmap ", zap.String("cfg.Name", cfg.Name), zap.Error(err))
		return
	}

	cw.kafkaCfg = kafkaCfg

	cw.logger.Debugw("Updated Kafka config from ConfigMap", zap.Any("ConfigMap", cfg))
	cw.logger.Debugf("Updated Kafka config from ConfigMap 2 %+v", cw.kafkaCfg)
}

func NewKafkaConfigFromConfigMap(cfg *corev1.ConfigMap) (*KafkaConfig, error) {
	if cfg == nil {
		return nil, fmt.Errorf("kafka configmap does not exist")
	}
	if _, ok := cfg.Data[constants.SaramaSettingsConfigKey]; !ok {
		return nil, fmt.Errorf("'%s' key does not exist in Kafka configmap", constants.SaramaSettingsConfigKey)
	}
	delete(cfg.Data, "_example")
	return &KafkaConfig{
		SaramaYamlString: cfg.Data[constants.SaramaSettingsConfigKey],
	}, nil
}

// kafkaConfigEnvVar returns an EnvVar containing the serialized Kafka
// configuration from the ConfigWatcher.
func (cw *KafkaSourceConfigWatcher) kafkaConfigEnvVar() corev1.EnvVar {
	cfg, err := kafkaConfigToJSON(cw.KafkaConfig())
	if err != nil {
		cw.logger.Warnw("Error while serializing Kafka config", zap.Error(err))
	}

	return corev1.EnvVar{
		Name:  EnvKafkaCfg,
		Value: cfg,
	}
}

func kafkaConfigToJSON(cfg *KafkaConfig) (string, error) { //nolint // for backcompat.
	if cfg == nil {
		return "", nil
	}

	jsonOpts, err := json.Marshal(cfg)
	return string(jsonOpts), err
}

// ConfigMapName gets the name of the Kafka ConfigMap
func KafkaConfigMapName() string {
	if cm := os.Getenv(kafkaConfigMapNameEnv); cm != "" {
		return cm
	}
	return constants.SettingsConfigMapName
}
