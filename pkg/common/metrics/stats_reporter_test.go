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

package metrics

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opencensus.io/metric/metricdata"

	"github.com/stretchr/testify/assert"
	"knative.dev/eventing-kafka/pkg/channel/distributed/common/env"
	logtesting "knative.dev/pkg/logging/testing"
)

const (
	// Sarama Metrics
	RecordSendRateForTopicPrefix = "record-send-rate-for-topic-"
)

// Test The MetricsServer's Report() Functionality
func TestMetricsServer_Report(t *testing.T) {
	// Test Data
	metricsDomain := "eventing-kafka"
	topicName := "test-topic-name"
	msgCount := int64(13579)

	// Initialize The Environment For The Test
	assert.Nil(t, os.Setenv(env.MetricsDomainEnvVarKey, metricsDomain))

	// Create A Test Logger
	logger := logtesting.TestLogger(t).Desugar()

	// Create A New StatsReporter To Test
	statsReporter := NewStatsReporter(logger)
	defer statsReporter.Shutdown()

	// Create The Stats / Metrics To Report
	metrics := createTestMetrics(topicName, msgCount)

	// Perform The Test
	statsReporter.Report(metrics)

	// Verify that the count was stored for export properly
	savedMetric := statsReporter.(*Reporter).metrics[RecordSendRateForTopicPrefix+topicName+".count"]
	require.NotNil(t, savedMetric)
	require.Equal(t, 1, len(savedMetric.TimeSeries))
	require.Equal(t, 1, len(savedMetric.TimeSeries[0].Points))
	messageValue, ok := savedMetric.TimeSeries[0].Points[0].Value.(int64)
	require.True(t, ok)
	assert.Equal(t, msgCount, messageValue)
}

func TestGetMetricSubInfo(t *testing.T) {
	const subMetric = "test-sub-metric"

	tests := []struct {
		name string
		want string
	}{
		{name: "incoming-byte-rate-for-broker-0", want: "Bytes/second read of broker 0: " + subMetric},
		{name: "outgoing-byte-rate-for-broker-0", want: "Bytes/second written of broker 0: " + subMetric},
		{name: "request-rate-for-broker-0", want: "Requests/second sent to broker 0: " + subMetric},
		{name: "request-size-for-broker-0", want: "Distribution of the request size in bytes for broker 0: " + subMetric},
		{name: "request-latency-in-ms-for-broker-0", want: "Distribution of the request latency in ms for broker 0: " + subMetric},
		{name: "response-rate-for-broker-0", want: "Responses/second received from broker 0: " + subMetric},
		{name: "response-size-for-broker-0", want: "Distribution of the response size in bytes for broker 0: " + subMetric},
		{name: "requests-in-flight-for-broker-0", want: "The current number of in-flight requests awaiting a response for broker 0: " + subMetric},
		{name: "batch-size-for-topic-test-topic", want: "Distribution of the number of bytes sent per partition per request for topic \"test-topic\": " + subMetric},
		{name: "record-send-rate-for-topic-test-topic", want: "Records/second sent to topic \"test-topic\": " + subMetric},
		{name: "records-per-request-for-topic-test-topic", want: "Distribution of the number of records sent per request for topic \"test-topic\": " + subMetric},
		{name: "compression-ratio-for-topic-test-topic", want: "Distribution of the compression ratio times 100 of record batches for topic \"test-topic\": " + subMetric},
		{name: "incoming-byte-rate", want: "Bytes/second read of all brokers: " + subMetric},
		{name: "outgoing-byte-rate", want: "Bytes/second written of all brokers: " + subMetric},
		{name: "request-rate", want: "Requests/second sent to all brokers: " + subMetric},
		{name: "request-size", want: "Distribution of the request size in bytes for all brokers: " + subMetric},
		{name: "request-latency-in-ms", want: "Distribution of the request latency in ms for all brokers: " + subMetric},
		{name: "response-rate", want: "Responses/second received from all brokers: " + subMetric},
		{name: "response-size", want: "Distribution of the response size in bytes for all brokers: " + subMetric},
		{name: "requests-in-flight", want: "The current number of in-flight requests awaiting a response for all brokers: " + subMetric},
		{name: "batch-size", want: "Distribution of the number of bytes sent per partition per request for all topics: " + subMetric},
		{name: "record-send-rate", want: "Records/second sent to all topics: " + subMetric},
		{name: "records-per-request", want: "Distribution of the number of records sent per request for all topics: " + subMetric},
		{name: "compression-ratio", want: "Distribution of the compression ratio times 100 of record batches for all topics: " + subMetric},
		{name: "consumer-batch-size", want: "Distribution of the number of messages in a batch: " + subMetric},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test uncached regex replacement
			if got := getMetricSubInfo(tt.name, subMetric).Description; got != tt.want {
				t.Errorf("getMetricSubInfo() = %v, want %v", got, tt.want)
			}
			// Test cached map replacement (should be identical)
			if got := getMetricSubInfo(tt.name, subMetric).Description; got != tt.want {
				t.Errorf("getMetricSubInfo() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetMetricInfo(t *testing.T) {
	tests := []struct {
		name string
		want string
	}{
		{name: "incoming-byte-rate-for-broker-0", want: "Bytes/second read of broker 0"},
		{name: "outgoing-byte-rate-for-broker-0", want: "Bytes/second written of broker 0"},
		{name: "request-rate-for-broker-0", want: "Requests/second sent to broker 0"},
		{name: "request-size-for-broker-0", want: "Distribution of the request size in bytes for broker 0"},
		{name: "request-latency-in-ms-for-broker-0", want: "Distribution of the request latency in ms for broker 0"},
		{name: "response-rate-for-broker-0", want: "Responses/second received from broker 0"},
		{name: "response-size-for-broker-0", want: "Distribution of the response size in bytes for broker 0"},
		{name: "requests-in-flight-for-broker-0", want: "The current number of in-flight requests awaiting a response for broker 0"},
		{name: "batch-size-for-topic-test-topic", want: "Distribution of the number of bytes sent per partition per request for topic \"test-topic\""},
		{name: "record-send-rate-for-topic-test-topic", want: "Records/second sent to topic \"test-topic\""},
		{name: "records-per-request-for-topic-test-topic", want: "Distribution of the number of records sent per request for topic \"test-topic\""},
		{name: "compression-ratio-for-topic-test-topic", want: "Distribution of the compression ratio times 100 of record batches for topic \"test-topic\""},
		{name: "incoming-byte-rate", want: "Bytes/second read of all brokers"},
		{name: "outgoing-byte-rate", want: "Bytes/second written of all brokers"},
		{name: "request-rate", want: "Requests/second sent to all brokers"},
		{name: "request-size", want: "Distribution of the request size in bytes for all brokers"},
		{name: "request-latency-in-ms", want: "Distribution of the request latency in ms for all brokers"},
		{name: "response-rate", want: "Responses/second received from all brokers"},
		{name: "response-size", want: "Distribution of the response size in bytes for all brokers"},
		{name: "requests-in-flight", want: "The current number of in-flight requests awaiting a response for all brokers"},
		{name: "batch-size", want: "Distribution of the number of bytes sent per partition per request for all topics"},
		{name: "record-send-rate", want: "Records/second sent to all topics"},
		{name: "records-per-request", want: "Distribution of the number of records sent per request for all topics"},
		{name: "compression-ratio", want: "Distribution of the compression ratio times 100 of record batches for all topics"},
		{name: "consumer-batch-size", want: "Distribution of the number of messages in a batch"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test uncached regex replacement
			if got := getMetricInfo(tt.name).Description; got != tt.want {
				t.Errorf("getMetricInfo() = %v, want %v", got, tt.want)
			}
			// Test cached map replacement (should be identical)
			if got := getMetricInfo(tt.name).Description; got != tt.want {
				t.Errorf("getMetricInfo() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetSubDescription(t *testing.T) {
	tests := []struct {
		name string
		want string
	}{
		{name: "1m.rate", want: "1-Minute Rate"},
		{name: "5m.rate", want: "5-Minute Rate"},
		{name: "15m.rate", want: "15-Minute Rate"},
		{name: "count", want: "Count"},
		{name: "max", want: "Maximum"},
		{name: "mean", want: "Mean"},
		{name: "mean.rate", want: "Mean Rate"},
		{name: "median", want: "Median"},
		{name: "min", want: "Minimum"},
		{name: "stddev", want: "Standard Deviation"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getSubDescription(tt.name); got != tt.want {
				t.Errorf("getSubDescription() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestReporterNewPoint(t *testing.T) {
	tests := []struct {
		name        string
		value       interface{}
		ExpectZero  bool
		ExpectFloat bool
	}{
		{name: "Int64 Measure", value: int64(-12345678901)},
		{name: "Int32 Measure", value: int32(-123456)},
		{name: "Int16 Measure", value: int16(-1234), ExpectZero: true},
		{name: "Int8 Measure", value: int8(-123), ExpectZero: true},
		{name: "Int Measure", value: -12345},
		{name: "UInt64 Measure", value: uint64(12345678901), ExpectZero: true},
		{name: "UInt32 Measure", value: uint32(123456), ExpectZero: true},
		{name: "UInt16 Measure", value: uint16(1234), ExpectZero: true},
		{name: "UInt8 Measure", value: uint8(123), ExpectZero: true},
		{name: "UInt Measure", value: uint(12345), ExpectZero: true},
		{name: "Float64 Measure", value: 12.345, ExpectFloat: true},
		{name: "Float32 Measure", value: float32(12.345), ExpectFloat: true},
		{name: "Invalid Measure", value: "not-a-number", ExpectZero: true},
	}

	timeNow := time.Now()
	statsReporter := createTestReporter(t)
	defer statsReporter.Shutdown()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			// Perform the test
			point := statsReporter.newPoint(timeNow, tt.value)

			// Verify the results
			if tt.ExpectZero {
				assert.Zero(t, point.Value.(int64))
			} else if tt.ExpectFloat {
				assert.InDelta(t, tt.value, point.Value.(float64), 0.1) // InDelta handles "off by 0.00000001" float issues
			} else {
				assert.InDelta(t, tt.value, point.Value.(int64), 0.1) // InDelta handles "slightly different int-types"
			}
		})
	}
}

func TestIsPercentileMetric(t *testing.T) {
	testMetrics := createTestMetrics("test-topic", 100)

	tests := []struct {
		name   string
		expect bool
	}{
		{name: "batch-size", expect: true},
		{name: "batch-size-for-topic-test-topic", expect: true},
		{name: "compression-ratio", expect: true},
		{name: "compression-ratio-for-topic-stage_sample-kafka-channel-1", expect: true},
		{name: "incoming-byte-rate", expect: false},
		{name: "incoming-byte-rate-for-broker-0", expect: false},
		{name: "outgoing-byte-rate", expect: false},
		{name: "outgoing-byte-rate-for-broker-0", expect: false},
		{name: "record-send-rate", expect: false},
		{name: RecordSendRateForTopicPrefix + "test-topic", expect: false},
		{name: "records-per-request", expect: true},
		{name: "records-per-request-for-topic-stage_sample-kafka-channel-1", expect: true},
		{name: "request-latency-in-ms", expect: true},
		{name: "request-latency-in-ms-for-broker-0", expect: true},
		{name: "request-rate", expect: false},
		{name: "request-rate-for-broker-0", expect: false},
		{name: "request-size", expect: true},
		{name: "request-size-for-broker-0", expect: true},
		{name: "response-rate", expect: false},
		{name: "response-rate-for-broker-0", expect: false},
		{name: "response-size", expect: true},
		{name: "response-size-for-broker-0", expect: true},
		{name: "int32-test-metric", expect: false},
		{name: "float32-test-metric", expect: false},
		{name: "nan-test-metric", expect: false},
		{name: "bad-header", expect: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expect, isPercentileMetric(testMetrics[tt.name]))
		})
	}
}

func TestGetMetricUnit(t *testing.T) {
	tests := []struct {
		name     string
		expectMS bool
	}{
		{name: "batch-size"},
		{name: "batch-size-for-topic-test-topic"},
		{name: "compression-ratio"},
		{name: "compression-ratio-for-topic-stage_sample-kafka-channel-1"},
		{name: "incoming-byte-rate"},
		{name: "incoming-byte-rate-for-broker-0"},
		{name: "outgoing-byte-rate"},
		{name: "outgoing-byte-rate-for-broker-0"},
		{name: "record-send-rate"},
		{name: RecordSendRateForTopicPrefix + "test-topic"},
		{name: "records-per-request"},
		{name: "records-per-request-for-topic-stage_sample-kafka-channel-1"},
		{name: "request-latency-in-ms", expectMS: true},
		{name: "request-latency-in-ms-for-broker-0", expectMS: true},
		{name: "request-rate"},
		{name: "request-rate-for-broker-0"},
		{name: "request-size"},
		{name: "request-size-for-broker-0"},
		{name: "response-rate"},
		{name: "response-rate-for-broker-0"},
		{name: "response-size"},
		{name: "response-size-for-broker-0"},
		{name: "int32-test-metric"},
		{name: "float32-test-metric"},
		{name: "nan-test-metric"},
		{name: "bad-header"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expectMS, getMetricUnit(tt.name) == metricdata.UnitMilliseconds)
		})
	}
}

func TestReporterRecordMetric(t *testing.T) {
	reporter := createTestReporter(t)
	defer reporter.Shutdown()
	metrics := createTestMetrics("test-topic", 100)
	tests := []struct {
		name       string
		noCount    bool
		percentile bool
	}{
		{name: "batch-size", percentile: true},
		{name: "batch-size-for-topic-test-topic", percentile: true},
		{name: "compression-ratio", percentile: true},
		{name: "compression-ratio-for-topic-stage_sample-kafka-channel-1", percentile: true},
		{name: "incoming-byte-rate"},
		{name: "incoming-byte-rate-for-broker-0"},
		{name: "outgoing-byte-rate"},
		{name: "outgoing-byte-rate-for-broker-0"},
		{name: "record-send-rate"},
		{name: RecordSendRateForTopicPrefix + "test-topic"},
		{name: "records-per-request", percentile: true},
		{name: "records-per-request-for-topic-stage_sample-kafka-channel-1", percentile: true},
		{name: "request-latency-in-ms", percentile: true},
		{name: "request-latency-in-ms-for-broker-0", percentile: true},
		{name: "request-rate"},
		{name: "request-rate-for-broker-0"},
		{name: "request-size", percentile: true},
		{name: "request-size-for-broker-0", percentile: true},
		{name: "response-rate"},
		{name: "response-rate-for-broker-0"},
		{name: "response-size", percentile: true},
		{name: "response-size-for-broker-0", percentile: true},
		{name: "int32-test-metric", noCount: true},
		{name: "float32-test-metric", noCount: true},
		{name: "nan-test-metric", noCount: true},
		{name: "bad-header", noCount: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Clear the metrics before each test
			reporter.metrics = make(map[string]*metricdata.Metric)
			item := metrics[tt.name]
			reporter.recordMetric(tt.name, item)
			if tt.percentile {
				savedMetric, ok := reporter.metrics[tt.name]
				require.True(t, ok)
				// Verify that all submetrics are part of one timeseries array (except for "count")
				for _, series := range savedMetric.TimeSeries {
					require.Equal(t, 1, len(series.Points))
					require.Equal(t, 1, len(series.LabelValues))
					itemKey := series.LabelValues[0].Value
					if itemKey == "50%" {
						itemKey = "median" // special-case conversion
					}
					// Make sure this point's value came from the original item
					assert.InDelta(t, item[itemKey], series.Points[0].Value, 0.1)
				}
			} else {
				// Verify that all of the sub-items exist as their own metric
				for key, subItemValue := range item {
					subItemInfo := getMetricSubInfo(tt.name, key)
					savedMetric, ok := reporter.metrics[subItemInfo.Name]
					require.True(t, ok)
					require.Equal(t, 1, len(savedMetric.TimeSeries))
					require.Equal(t, 1, len(savedMetric.TimeSeries[0].Points))
					// Verify that the metric's value came from the original item
					if _, ok = subItemValue.(string); ok {
						// If it isn't a number it should be logged as zero
						assert.InDelta(t, 0, savedMetric.TimeSeries[0].Points[0].Value, 0.1)
					} else {
						assert.InDelta(t, subItemValue, savedMetric.TimeSeries[0].Points[0].Value, 0.1)
					}
				}
			}
		})
	}
}

func TestReporterRead(t *testing.T) {
	reporter := createTestReporter(t)
	defer reporter.Shutdown()
	metrics := createTestMetrics("test-topic", 100)
	expectedMetrics := 0
	for key, value := range metrics {
		if isPercentileMetric(value) {
			expectedMetrics += 2 // The count and the timeseries array are two separate metrics
		} else {
			expectedMetrics += len(value) // Each individual subitem is its own metric
		}
		reporter.recordMetric(key, value)
	}
	metricsArray := reporter.Read()
	// Verify that we are outputting the number of metrics we expect, given the test metrics list
	assert.Equal(t, expectedMetrics, len(metricsArray))
}

// Utility Function For Creating Test Reporter Struct
func createTestReporter(t *testing.T) *Reporter {
	return &Reporter{
		logger:  logtesting.TestLogger(t).Desugar(),
		metrics: make(map[string]*metricdata.Metric),
	}
}

// Utility Function For Creating Sample Test Metrics  (Representative Data From Sarama Metrics Trace - With Custom Test Data)
func createTestMetrics(topic string, count int64) ReportingList {
	testMetrics := make(ReportingList)
	testMetrics["batch-size"] = ReportingItem{"75%": 422, "95%": 422, "99%": 422, "99.9%": 422, "count": 5, "max": 422, "mean": 422, "median": 422, "min": 422, "stddev": 0}
	testMetrics["batch-size-for-topic-"+topic] = ReportingItem{"75%": 422, "95%": 422, "99%": 422, "99.9%": 422, "count": 5, "max": 422, "mean": 422, "median": 422, "min": 422, "stddev": 0}
	testMetrics["compression-ratio"] = ReportingItem{"75%": 100, "95%": 100, "99%": 100, "99.9%": 100, "count": 5, "max": 100, "mean": 100, "median": 100, "min": 100, "stddev": 0}
	testMetrics["compression-ratio-for-topic-stage_sample-kafka-channel-1"] = ReportingItem{"75%": 100, "95%": 100, "99%": 100, "99.9%": 100, "count": 5, "max": 100, "mean": 100, "median": 100, "min": 100, "stddev": 0}
	testMetrics["incoming-byte-rate"] = ReportingItem{"15m.rate": 338.48922714325533, "1m.rate": 148.6636907525621, "5m.rate": 300.3520357972228, "count": 2157, "mean.rate": 36.136821927158024}
	testMetrics["incoming-byte-rate-for-broker-0"] = ReportingItem{"15m.rate": 57.04287862876796, "1m.rate": 49.81665008593679, "5m.rate": 55.94572447585394, "count": 360, "mean.rate": 26.962040661298193}
	testMetrics["outgoing-byte-rate"] = ReportingItem{"15m.rate": 8.542065819239612, "1m.rate": 36.494569810073976, "5m.rate": 13.085405055952117, "count": 2501, "mean.rate": 41.89999208758941}
	testMetrics["outgoing-byte-rate-for-broker-0"] = ReportingItem{"15m.rate": 391.3775283696023, "1m.rate": 341.7975714229552, "5m.rate": 383.8498318204423, "count": 2470, "mean.rate": 184.9934008427559}
	testMetrics["record-send-rate"] = ReportingItem{"15m.rate": 0.7922622031773328, "1m.rate": 0.6918979178602331, "5m.rate": 0.777023951053527, "count": 5, "mean.rate": 0.3744896470537649}
	testMetrics[RecordSendRateForTopicPrefix+topic] = ReportingItem{"15m.rate": 0.7922622031773328, "1m.rate": 0.6918979178602331, "5m.rate": 0.777023951053527, "count": count, "mean.rate": 0.3744894223293246}
	testMetrics["records-per-request"] = ReportingItem{"75%": 1, "95%": 1, "99%": 1, "99.9%": 1, "count": 5, "max": 1, "mean": 1, "median": 1, "min": 1, "stddev": 0}
	testMetrics["records-per-request-for-topic-stage_sample-kafka-channel-1"] = ReportingItem{"75%": 1, "95%": 1, "99%": 1, "99.9%": 1, "count": 5, "max": 1, "mean": 1, "median": 1, "min": 1, "stddev": 0}
	testMetrics["request-latency-in-ms"] = ReportingItem{"75%": 24, "95%": 78, "99%": 78, "99.9%": 78, "count": 6, "max": 78, "mean": 16.666666666666668, "median": 5, "min": 3, "stddev": 27.45096638655105}
	testMetrics["request-latency-in-ms-for-broker-0"] = ReportingItem{"75%": 42, "95%": 78, "99%": 78, "99.9%": 78, "count": 5, "max": 78, "mean": 19.4, "median": 5, "min": 3, "stddev": 29.31620712165883}
	testMetrics["request-rate"] = ReportingItem{"15m.rate": 0.19362878205360173, "1m.rate": 0.1488272222720787, "5m.rate": 0.1825385339752764, "count": 6, "mean.rate": 0.1005196891551448}
	testMetrics["request-rate-for-broker-0"] = ReportingItem{"15m.rate": 0.7922622031773328, "1m.rate": 0.6918979178602331, "5m.rate": 0.777023951053527, "count": 5, "mean.rate": 0.37447298101266696}
	testMetrics["request-size"] = ReportingItem{"75%": 494, "95%": 494, "99%": 494, "99.9%": 494, "count": 6, "max": 494, "mean": 416.8333333333333, "median": 494, "min": 31, "stddev": 172.54991226373375}
	testMetrics["request-size-for-broker-0"] = ReportingItem{"75%": 494, "95%": 494, "99%": 494, "99.9%": 494, "count": 5, "max": 494, "mean": 494, "median": 494, "min": 494, "stddev": 0}
	testMetrics["response-rate"] = ReportingItem{"15m.rate": 0.19362878205360173, "1m.rate": 0.1488272222720787, "5m.rate": 0.1825385339752764, "count": 6, "mean.rate": 0.10051977925613151}
	testMetrics["response-rate-for-broker-0"] = ReportingItem{"15m.rate": 0.7922622031773328, "1m.rate": 0.6918979178602331, "5m.rate": 0.777023951053527, "count": 5, "mean.rate": 0.3744806601376294}
	testMetrics["response-size"] = ReportingItem{"75%": 503.25, "95%": 1797, "99%": 1797, "99.9%": 1797, "count": 6, "max": 1797, "mean": 359.5, "median": 72, "min": 72, "stddev": 642.8695435311895}
	testMetrics["response-size-for-broker-0"] = ReportingItem{"75%": 72, "95%": 72, "99%": 72, "99.9%": 72, "count": 5, "max": 72, "mean": 72, "median": 72, "min": 72, "stddev": 0}
	testMetrics["int32-test-metric"] = ReportingItem{"test-header": int32(1)}
	testMetrics["float32-test-metric"] = ReportingItem{"test-header": float32(1.2345)}
	testMetrics["nan-test-metric"] = ReportingItem{"test-header": "not-a-number"}
	testMetrics["bad-header"] = ReportingItem{"�": 0}
	return testMetrics
}
