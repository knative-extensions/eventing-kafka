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
	"knative.dev/pkg/metrics"
	tracingconfig "knative.dev/pkg/tracing/config"
)

const (
	EnvLoggingCfg = "K_LOGGING_CONFIG"
	EnvMetricsCfg = "K_METRICS_CONFIG"
	EnvTracingCfg = "K_TRACING_CONFIG"
	EnvKafkaCfg   = "K_KAFKA_CONFIG"
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
	envs := make([]corev1.EnvVar, 0, 4)

	envs = maybeAppendEnvVar(envs, cw.loggingConfigEnvVar(), cw.LoggingConfig() != nil)
	envs = maybeAppendEnvVar(envs, cw.metricsConfigEnvVar(), cw.MetricsConfig() != nil)
	envs = maybeAppendEnvVar(envs, cw.tracingConfigEnvVar(), cw.TracingConfig() != nil)
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

// LoggingConfig returns the logging configuration from the ConfigWatcher.
func (cw *KafkaSourceConfigWatcher) LoggingConfig() *logging.Config {
	if cw == nil {
		return nil
	}
	return cw.loggingCfg
}

// MetricsConfig returns the metrics configuration from the ConfigWatcher.
func (cw *KafkaSourceConfigWatcher) MetricsConfig() *metrics.ExporterOptions {
	if cw == nil {
		return nil
	}
	return cw.metricsCfg
}

// TracingConfig returns the tracing configuration from the ConfigWatcher.
func (cw *KafkaSourceConfigWatcher) TracingConfig() *tracingconfig.Config {
	if cw == nil {
		return nil
	}
	return cw.tracingCfg
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
	// TODO: copy-pasted

	logger *zap.SugaredLogger

	component string

	// configurations remain nil if disabled
	loggingCfg *logging.Config
	metricsCfg *metrics.ExporterOptions
	tracingCfg *tracingconfig.Config

	kafkaCfg *KafkaConfig
}

// configWatcherOption is a function option for ConfigWatchers.
type configWatcherOption func(*KafkaSourceConfigWatcher, configmap.Watcher)

// WatchConfigurations returns a ConfigWatcher initialized with the given
// options. If no option is passed, the ConfigWatcher observes ConfigMaps for
// logging, metrics and tracing.
func WatchConfigurations(loggingCtx context.Context, component string,
	cmw configmap.Watcher, opts ...configWatcherOption) *KafkaSourceConfigWatcher {

	cw := &KafkaSourceConfigWatcher{
		logger:    logging.FromContext(loggingCtx),
		component: component,
	}

	if len(opts) == 0 {
		WithLogging(cw, cmw)
		WithMetrics(cw, cmw)
		WithTracing(cw, cmw)
		WithKafka(cw, cmw)

	} else {
		for _, opt := range opts {
			opt(cw, cmw)
		}
	}

	return cw
}

// WithLogging observes a logging ConfigMap.
func WithLogging(cw *KafkaSourceConfigWatcher, cmw configmap.Watcher) {
	cw.loggingCfg = &logging.Config{}
	watchConfigMap(cmw, logging.ConfigMapName(), cw.updateFromLoggingConfigMap)
}

// WithMetrics observes a metrics ConfigMap.
func WithMetrics(cw *KafkaSourceConfigWatcher, cmw configmap.Watcher) {
	cw.metricsCfg = &metrics.ExporterOptions{}
	watchConfigMap(cmw, metrics.ConfigMapName(), cw.updateFromMetricsConfigMap)
}

// WithTracing observes a tracing ConfigMap.
func WithTracing(cw *KafkaSourceConfigWatcher, cmw configmap.Watcher) {
	cw.tracingCfg = &tracingconfig.Config{}
	watchConfigMap(cmw, tracingconfig.ConfigName, cw.updateFromTracingConfigMap)
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

func (cw *KafkaSourceConfigWatcher) updateFromLoggingConfigMap(cfg *corev1.ConfigMap) {
	if cfg == nil {
		return
	}

	delete(cfg.Data, "_example")

	loggingCfg, err := logging.NewConfigFromConfigMap(cfg)
	if err != nil {
		cw.logger.Warnw("failed to create logging config from ConfigMap", zap.String("cfg.Name", cfg.Name))
		return
	}

	cw.loggingCfg = loggingCfg

	cw.logger.Debugw("Updated logging config from ConfigMap", zap.Any("ConfigMap", cfg))
}

func (cw *KafkaSourceConfigWatcher) updateFromMetricsConfigMap(cfg *corev1.ConfigMap) {
	if cfg == nil {
		return
	}

	delete(cfg.Data, "_example")

	cw.metricsCfg = &metrics.ExporterOptions{
		Domain:    metrics.Domain(),
		Component: cw.component,
		ConfigMap: cfg.Data,
	}

	cw.logger.Debugw("Updated metrics config from ConfigMap", zap.Any("ConfigMap", cfg))
}

func (cw *KafkaSourceConfigWatcher) updateFromTracingConfigMap(cfg *corev1.ConfigMap) {
	if cfg == nil {
		return
	}

	delete(cfg.Data, "_example")

	tracingCfg, err := tracingconfig.NewTracingConfigFromMap(cfg.Data)
	if err != nil {
		cw.logger.Warnw("failed to create tracing config from ConfigMap", zap.String("cfg.Name", cfg.Name))
		return
	}

	cw.tracingCfg = tracingCfg

	cw.logger.Debugw("Updated tracing config from ConfigMap", zap.Any("ConfigMap", cfg))
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

// loggingConfigEnvVar returns an EnvVar containing the serialized logging
// configuration from the ConfigWatcher.
func (cw *KafkaSourceConfigWatcher) loggingConfigEnvVar() corev1.EnvVar {
	cfg, err := logging.ConfigToJSON(cw.LoggingConfig())
	if err != nil {
		cw.logger.Warnw("Error while serializing logging config", zap.Error(err))
	}

	return corev1.EnvVar{
		Name:  EnvLoggingCfg,
		Value: cfg,
	}
}

// metricsConfigEnvVar returns an EnvVar containing the serialized metrics
// configuration from the ConfigWatcher.
func (cw *KafkaSourceConfigWatcher) metricsConfigEnvVar() corev1.EnvVar {
	cfg, err := metrics.OptionsToJSON(cw.MetricsConfig())
	if err != nil {
		cw.logger.Warnw("Error while serializing metrics config", zap.Error(err))
	}

	return corev1.EnvVar{
		Name:  EnvMetricsCfg,
		Value: cfg,
	}
}

// tracingConfigEnvVar returns an EnvVar containing the serialized tracing
// configuration from the ConfigWatcher.
func (cw *KafkaSourceConfigWatcher) tracingConfigEnvVar() corev1.EnvVar {
	cfg, err := tracingconfig.TracingConfigToJSON(cw.TracingConfig())
	if err != nil {
		cw.logger.Warnw("Error while serializing tracing config", zap.Error(err))
	}

	return corev1.EnvVar{
		Name:  EnvTracingCfg,
		Value: cfg,
	}
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

const (
	kafkaConfigMapNameEnv = "CONFIG_KAFKA_NAME"
)

// ConfigMapName gets the name of the Kafka ConfigMap
func KafkaConfigMapName() string {
	if cm := os.Getenv(kafkaConfigMapNameEnv); cm != "" {
		return cm
	}
	return "config-kafka"
}
