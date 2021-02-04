package source

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"knative.dev/eventing/pkg/reconciler/source"
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/metrics"
	tracingconfig "knative.dev/pkg/tracing/config"

	loggingtesting "knative.dev/pkg/logging/testing"

	// metrics package is suggesting to the following
	// import to fix the panics in unit tests
	_ "knative.dev/pkg/metrics/testing"
)

const testComponent = "test_component"

func TestNewConfigWatcher_defaults(t *testing.T) {
	testCases := []struct {
		name                  string
		cmw                   configmap.Watcher
		expectLoggingContains string
		expectMetricsContains string
		expectTracingContains string
		expectKafkaContains   string
	}{
		{
			name:                  "With pre-filled sample data",
			cmw:                   configMapWatcherWithSampleData(),
			expectLoggingContains: `"zap-logger-config":"{\"level\": \"fatal\"}"`,
			expectMetricsContains: `"ConfigMap":{"metrics.backend":"test"}`,
			expectTracingContains: `"zipkin-endpoint":"zipkin.test"`,
			expectKafkaContains:   `"SaramaYamlString":"{Version: 2.0.0}"`,
		},
		{
			name: "With empty data",
			cmw:  configMapWatcherWithEmptyData(),
			// logging defaults to Knative's defaults
			expectLoggingContains: ``,
			// metrics defaults to empty ConfigMap
			expectMetricsContains: `"ConfigMap":{}`,
			// tracing defaults to None backend
			expectTracingContains: `"backend":"none"`,
			// kafka defaults to empty
			expectKafkaContains: ``,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := loggingtesting.TestContextWithLogger(t)
			cw := WatchConfigurations(ctx, testComponent, tc.cmw)

			assert.NotNil(t, cw.LoggingConfig(), "logging config should be enabled")
			assert.NotNil(t, cw.MetricsConfig(), "metrics config should be enabled")
			assert.NotNil(t, cw.TracingConfig(), "tracing config should be enabled")
			assert.NotNil(t, cw.KafkaConfig(), "kafka config should be enabled")

			envs := cw.ToEnvVars()

			const expectEnvs = 4
			require.Lenf(t, envs, expectEnvs, "there should be %d env var(s)", expectEnvs)

			assert.Equal(t, source.EnvLoggingCfg, envs[0].Name, "first env var is logging config")
			assert.Contains(t, envs[0].Value, tc.expectLoggingContains)

			assert.Equal(t, source.EnvMetricsCfg, envs[1].Name, "second env var is metrics config")
			assert.Contains(t, envs[1].Value, tc.expectMetricsContains)

			assert.Equal(t, source.EnvTracingCfg, envs[2].Name, "third env var is tracing config")
			assert.Contains(t, envs[2].Value, tc.expectTracingContains)

			assert.Equal(t, EnvKafkaCfg, envs[3].Name, "third env var is kafka config")
			assert.Contains(t, envs[3].Value, tc.expectKafkaContains)
		})
	}
}

// configMapWatcherWithSampleData constructs a Watcher for static sample data.
func configMapWatcherWithSampleData() configmap.Watcher {
	return configmap.NewStaticWatcher(
		newTestConfigMap(logging.ConfigMapName(), loggingConfigMapData()),
		newTestConfigMap(metrics.ConfigMapName(), metricsConfigMapData()),
		newTestConfigMap(tracingconfig.ConfigName, tracingConfigMapData()),
		newTestConfigMap(KafkaConfigMapName(), kafkaConfigMapData()),
	)
}

// configMapWatcherWithEmptyData constructs a Watcher for empty data.
func configMapWatcherWithEmptyData() configmap.Watcher {
	return configmap.NewStaticWatcher(
		newTestConfigMap(logging.ConfigMapName(), nil),
		newTestConfigMap(metrics.ConfigMapName(), nil),
		newTestConfigMap(tracingconfig.ConfigName, nil),
		newTestConfigMap(KafkaConfigMapName(), nil),
	)
}

func newTestConfigMap(name string, data map[string]string) *corev1.ConfigMap {
	if data == nil {
		data = make(map[string]string, 1)
	}

	// _example key is always appended to mimic Knative's release manifests
	data["_example"] = "test-config"

	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Data: data,
	}
}

// ConfigMap data generators
func loggingConfigMapData() map[string]string {
	return map[string]string{
		"zap-logger-config": `{"level": "fatal"}`,
	}
}
func metricsConfigMapData() map[string]string {
	return map[string]string{
		"metrics.backend": "test",
	}
}
func tracingConfigMapData() map[string]string {
	return map[string]string{
		"backend":         "zipkin",
		"zipkin-endpoint": "zipkin.test",
	}
}

func kafkaConfigMapData() map[string]string {
	return map[string]string{
		"sarama": `{Version: 2.0.0}`,
	}
}

func TestKafkaConfigToJSON(t *testing.T) {

	testCases := []struct {
		name     string
		cfg      KafkaConfig
		success  bool
		expected string
	}{{
		name: "simple marshal",
		cfg: KafkaConfig{
			SaramaYamlString: `Version: 2.0.0`,
		},
		success:  true,
		expected: `{"SaramaYamlString":"Version: 2.0.0"}`,
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			output, err := kafkaConfigToJSON(&tc.cfg)
			if tc.success == true && err != nil {
				t.Errorf("KafkaConfig should've been converted to JSON successfully. Err: %e", err)
			}
			if tc.success == false && err != nil {
				t.Errorf("KafkaConfig should not have been converted to JSON successfully.")
			}
			if output != tc.expected {
				t.Errorf("KafkaConfig conversion is wrong, want: %s vs got %s", tc.expected, output)
			}
		})
	}

}
