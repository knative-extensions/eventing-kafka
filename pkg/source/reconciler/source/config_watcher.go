package source

import (
	"context"
	"encoding/json"
	"os"

	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

// TODO: docs
// WatchConfigurations returns a ConfigWatcher initialized with the given
// options. If no option is passed, the ConfigWatcher observes ConfigMaps for
// logging, metrics and tracing.
func WatchConfigurations(loggingCtx context.Context, component string,
	cmw configmap.Watcher) *KafkaSourceConfigWatcher {

	configWatcher := source.WatchConfigurations(loggingCtx, component, cmw)

	cw := &KafkaSourceConfigWatcher{
		ConfigWatcher: configWatcher,
		logger:        logging.FromContext(loggingCtx),
		kafkaCfg:      nil,
	}

	WithKafka(cw, cmw)

	return cw
}

// WithKafka observes a Kafka ConfigMap.
func WithKafka(cw *KafkaSourceConfigWatcher, cmw configmap.Watcher) {
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
	if cfg == nil {
		return
	}

	delete(cfg.Data, "_example")

	kafkaCfg, err := NewKafkaConfigFromConfigMap(cfg)
	if err != nil {
		cw.logger.Warnw("failed to create kafka config from ConfigMap", zap.String("cfg.Name", cfg.Name))
		return
	}

	cw.kafkaCfg = kafkaCfg

	cw.logger.Debugw("Updated Kafka config from ConfigMap", zap.Any("ConfigMap", cfg))
	cw.logger.Debugf("Updated Kafka config from ConfigMap 2 %+v", cw.kafkaCfg)
}

func NewKafkaConfigFromConfigMap(cfg *corev1.ConfigMap) (*KafkaConfig, error) {
	// TODO: think when sarama key doesn't exist or it is empty
	// TODO: error handling
	return &KafkaConfig{
		SaramaYamlString: cfg.Data["sarama"],
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
	return "config-kafka"
}
