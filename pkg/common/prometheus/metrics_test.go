package prometheus

import (
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	logtesting "knative.dev/pkg/logging/testing"
	"testing"
)

// Test The MetricsServer's ObserveMetrics() Functionality
func TestObserveMetrics(t *testing.T) {

	// Test Data
	topic := "test-topic"
	count := int64(99)
	metrics := createTestMetrics(topic, count)

	// Create A Test Logger
	logger := logtesting.TestLogger(t).Desugar()

	// Create A MetricsServer To Test
	metricsServer := NewMetricsServer(logger, "8888", "/path")

	// Perform The Test
	metricsServer.ObserveMetrics(metrics)

	// Verify The Gauge Was Updated As Expected
	assertGaugeEqual(t, metricsServer.producedMsgCountGauges, topic, float64(count))
}

// Utility Function For Creating Sample Test Metrics  (Representative Data From Sarama Metrics Trace - With Custom Test Data)
func createTestMetrics(topic string, count int64) map[string]map[string]interface{} {
	metrics := make(map[string]map[string]interface{})
	metrics["batch-size"] = map[string]interface{}{"75%": 422, "95%": 422, "99%": 422, "99.9%": 422, "count": 5, "max": 422, "mean": 422, "median": 422, "min": 422, "stddev": 0}
	metrics["batch-size-for-topic-"+topic] = map[string]interface{}{"75%": 422, "95%": 422, "99%": 422, "99.9%": 422, "count": 5, "max": 422, "mean": 422, "median": 422, "min": 422, "stddev": 0}
	metrics["compression-ratio"] = map[string]interface{}{"75%": 100, "95%": 100, "99%": 100, "99.9%": 100, "count": 5, "max": 100, "mean": 100, "median": 100, "min": 100, "stddev": 0}
	metrics["compression-ratio-for-topic-stage_sample-kafka-channel-1"] = map[string]interface{}{"75%": 100, "95%": 100, "99%": 100, "99.9%": 100, "count": 5, "max": 100, "mean": 100, "median": 100, "min": 100, "stddev": 0}
	metrics["incoming-byte-rate"] = map[string]interface{}{"15m.rate": 338.48922714325533, "1m.rate": 148.6636907525621, "5m.rate": 300.3520357972228, "count": 2157, "mean.rate": 36.136821927158024}
	metrics["incoming-byte-rate-for-broker-0"] = map[string]interface{}{"15m.rate": 57.04287862876796, "1m.rate": 49.81665008593679, "5m.rate": 55.94572447585394, "count": 360, "mean.rate": 26.962040661298193}
	metrics["outgoing-byte-rate"] = map[string]interface{}{"15m.rate": 8.542065819239612, "1m.rate": 36.494569810073976, "5m.rate": 13.085405055952117, "count": 2501, "mean.rate": 41.89999208758941}
	metrics["outgoing-byte-rate-for-broker-0"] = map[string]interface{}{"15m.rate": 391.3775283696023, "1m.rate": 341.7975714229552, "5m.rate": 383.8498318204423, "count": 2470, "mean.rate": 184.9934008427559}
	metrics["record-send-rate"] = map[string]interface{}{"15m.rate": 0.7922622031773328, "1m.rate": 0.6918979178602331, "5m.rate": 0.777023951053527, "count": 5, "mean.rate": 0.3744896470537649}
	metrics[RecordSendRateForTopicPrefix+topic] = map[string]interface{}{"15m.rate": 0.7922622031773328, "1m.rate": 0.6918979178602331, "5m.rate": 0.777023951053527, "count": count, "mean.rate": 0.3744894223293246}
	metrics["records-per-request"] = map[string]interface{}{"75%": 1, "95%": 1, "99%": 1, "99.9%": 1, "count": 5, "max": 1, "mean": 1, "median": 1, "min": 1, "stddev": 0}
	metrics["records-per-request-for-topic-stage_sample-kafka-channel-1"] = map[string]interface{}{"75%": 1, "95%": 1, "99%": 1, "99.9%": 1, "count": 5, "max": 1, "mean": 1, "median": 1, "min": 1, "stddev": 0}
	metrics["request-latency-in-ms"] = map[string]interface{}{"75%": 24, "95%": 78, "99%": 78, "99.9%": 78, "count": 6, "max": 78, "mean": 16.666666666666668, "median": 5, "min": 3, "stddev": 27.45096638655105}
	metrics["request-latency-in-ms-for-broker-0"] = map[string]interface{}{"75%": 42, "95%": 78, "99%": 78, "99.9%": 78, "count": 5, "max": 78, "mean": 19.4, "median": 5, "min": 3, "stddev": 29.31620712165883}
	metrics["request-rate"] = map[string]interface{}{"15m.rate": 0.19362878205360173, "1m.rate": 0.1488272222720787, "5m.rate": 0.1825385339752764, "count": 6, "mean.rate": 0.1005196891551448}
	metrics["request-rate-for-broker-0"] = map[string]interface{}{"15m.rate": 0.7922622031773328, "1m.rate": 0.6918979178602331, "5m.rate": 0.777023951053527, "count": 5, "mean.rate": 0.37447298101266696}
	metrics["request-size"] = map[string]interface{}{"75%": 494, "95%": 494, "99%": 494, "99.9%": 494, "count": 6, "max": 494, "mean": 416.8333333333333, "median": 494, "min": 31, "stddev": 172.54991226373375}
	metrics["request-size-for-broker-0"] = map[string]interface{}{"75%": 494, "95%": 494, "99%": 494, "99.9%": 494, "count": 5, "max": 494, "mean": 494, "median": 494, "min": 494, "stddev": 0}
	metrics["response-rate"] = map[string]interface{}{"15m.rate": 0.19362878205360173, "1m.rate": 0.1488272222720787, "5m.rate": 0.1825385339752764, "count": 6, "mean.rate": 0.10051977925613151}
	metrics["response-rate-for-broker-0"] = map[string]interface{}{"15m.rate": 0.7922622031773328, "1m.rate": 0.6918979178602331, "5m.rate": 0.777023951053527, "count": 5, "mean.rate": 0.3744806601376294}
	metrics["response-size"] = map[string]interface{}{"75%": 503.25, "95%": 1797, "99%": 1797, "99.9%": 1797, "count": 6, "max": 1797, "mean": 359.5, "median": 72, "min": 72, "stddev": 642.8695435311895}
	metrics["response-size-for-broker-0"] = map[string]interface{}{"75%": 72, "95%": 72, "99%": 72, "99.9%": 72, "count": 5, "max": 72, "mean": 72, "median": 72, "min": 72, "stddev": 0}
	return metrics
}

// Utility Function For Comparing Expected Prometheus Gauge Values
func assertGaugeEqual(t *testing.T, gaugeVec *prometheus.GaugeVec, topic string, expectedValue float64) {
	gauge, err := gaugeVec.GetMetricWithLabelValues(topic)
	assert.Nil(t, err)
	assert.NotNil(t, gauge)
	metric := &dto.Metric{}
	err = gauge.Write(metric)
	assert.Nil(t, err)
	assert.Equal(t, expectedValue, *metric.Gauge.Value)
}
