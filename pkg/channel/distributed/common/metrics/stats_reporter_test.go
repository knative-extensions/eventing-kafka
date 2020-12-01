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

package metrics

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	ocstats "go.opencensus.io/stats"
	"knative.dev/eventing-kafka/pkg/channel/distributed/common/env"
	logtesting "knative.dev/pkg/logging/testing"
)

// Test The MetricsServer's Report() Functionality
func TestMetricsServer_Report(t *testing.T) {
	// Test Data
	metricsDomain := "eventing-kafka"
	topicName := "test-topic-name"
	msgCount := 13579

	// Initialize The Environment For The Test
	assert.Nil(t, os.Setenv(env.MetricsDomainEnvVarKey, metricsDomain))

	// Create A Test Logger
	logger := logtesting.TestLogger(t).Desugar()

	// Create A New StatsReporter To Test
	statsReporter := NewStatsReporter(logger)

	// Create The Stats / Metrics To Report
	stats := createTestMetrics(topicName, int64(msgCount))

	measuredMsgCount := 0.0

	RecordWrapperRef := RecordWrapper
	defer func() { RecordWrapper = RecordWrapperRef }()

	// Set up a mock RecordWrapper() that will capture the produced message count from our stats reporter
	RecordWrapper = func(ctx context.Context, ms ocstats.Measurement, ros ...ocstats.Options) {
		measuredMsgCount += ms.Value()
	}

	// Perform The Test
	statsReporter.Report(stats)

	assert.Equal(t, float64(msgCount), measuredMsgCount)
}

// Utility Function For Creating Sample Test Metrics  (Representative Data From Sarama Metrics Trace - With Custom Test Data)
func createTestMetrics(topic string, count int64) map[string]map[string]interface{} {
	testMetrics := make(map[string]map[string]interface{})
	testMetrics["batch-size"] = map[string]interface{}{"75%": 422, "95%": 422, "99%": 422, "99.9%": 422, "count": 5, "max": 422, "mean": 422, "median": 422, "min": 422, "stddev": 0}
	testMetrics["batch-size-for-topic-"+topic] = map[string]interface{}{"75%": 422, "95%": 422, "99%": 422, "99.9%": 422, "count": 5, "max": 422, "mean": 422, "median": 422, "min": 422, "stddev": 0}
	testMetrics["compression-ratio"] = map[string]interface{}{"75%": 100, "95%": 100, "99%": 100, "99.9%": 100, "count": 5, "max": 100, "mean": 100, "median": 100, "min": 100, "stddev": 0}
	testMetrics["compression-ratio-for-topic-stage_sample-kafka-channel-1"] = map[string]interface{}{"75%": 100, "95%": 100, "99%": 100, "99.9%": 100, "count": 5, "max": 100, "mean": 100, "median": 100, "min": 100, "stddev": 0}
	testMetrics["incoming-byte-rate"] = map[string]interface{}{"15m.rate": 338.48922714325533, "1m.rate": 148.6636907525621, "5m.rate": 300.3520357972228, "count": 2157, "mean.rate": 36.136821927158024}
	testMetrics["incoming-byte-rate-for-broker-0"] = map[string]interface{}{"15m.rate": 57.04287862876796, "1m.rate": 49.81665008593679, "5m.rate": 55.94572447585394, "count": 360, "mean.rate": 26.962040661298193}
	testMetrics["outgoing-byte-rate"] = map[string]interface{}{"15m.rate": 8.542065819239612, "1m.rate": 36.494569810073976, "5m.rate": 13.085405055952117, "count": 2501, "mean.rate": 41.89999208758941}
	testMetrics["outgoing-byte-rate-for-broker-0"] = map[string]interface{}{"15m.rate": 391.3775283696023, "1m.rate": 341.7975714229552, "5m.rate": 383.8498318204423, "count": 2470, "mean.rate": 184.9934008427559}
	testMetrics["record-send-rate"] = map[string]interface{}{"15m.rate": 0.7922622031773328, "1m.rate": 0.6918979178602331, "5m.rate": 0.777023951053527, "count": 5, "mean.rate": 0.3744896470537649}
	testMetrics[RecordSendRateForTopicPrefix+topic] = map[string]interface{}{"15m.rate": 0.7922622031773328, "1m.rate": 0.6918979178602331, "5m.rate": 0.777023951053527, "count": count, "mean.rate": 0.3744894223293246}
	testMetrics["records-per-request"] = map[string]interface{}{"75%": 1, "95%": 1, "99%": 1, "99.9%": 1, "count": 5, "max": 1, "mean": 1, "median": 1, "min": 1, "stddev": 0}
	testMetrics["records-per-request-for-topic-stage_sample-kafka-channel-1"] = map[string]interface{}{"75%": 1, "95%": 1, "99%": 1, "99.9%": 1, "count": 5, "max": 1, "mean": 1, "median": 1, "min": 1, "stddev": 0}
	testMetrics["request-latency-in-ms"] = map[string]interface{}{"75%": 24, "95%": 78, "99%": 78, "99.9%": 78, "count": 6, "max": 78, "mean": 16.666666666666668, "median": 5, "min": 3, "stddev": 27.45096638655105}
	testMetrics["request-latency-in-ms-for-broker-0"] = map[string]interface{}{"75%": 42, "95%": 78, "99%": 78, "99.9%": 78, "count": 5, "max": 78, "mean": 19.4, "median": 5, "min": 3, "stddev": 29.31620712165883}
	testMetrics["request-rate"] = map[string]interface{}{"15m.rate": 0.19362878205360173, "1m.rate": 0.1488272222720787, "5m.rate": 0.1825385339752764, "count": 6, "mean.rate": 0.1005196891551448}
	testMetrics["request-rate-for-broker-0"] = map[string]interface{}{"15m.rate": 0.7922622031773328, "1m.rate": 0.6918979178602331, "5m.rate": 0.777023951053527, "count": 5, "mean.rate": 0.37447298101266696}
	testMetrics["request-size"] = map[string]interface{}{"75%": 494, "95%": 494, "99%": 494, "99.9%": 494, "count": 6, "max": 494, "mean": 416.8333333333333, "median": 494, "min": 31, "stddev": 172.54991226373375}
	testMetrics["request-size-for-broker-0"] = map[string]interface{}{"75%": 494, "95%": 494, "99%": 494, "99.9%": 494, "count": 5, "max": 494, "mean": 494, "median": 494, "min": 494, "stddev": 0}
	testMetrics["response-rate"] = map[string]interface{}{"15m.rate": 0.19362878205360173, "1m.rate": 0.1488272222720787, "5m.rate": 0.1825385339752764, "count": 6, "mean.rate": 0.10051977925613151}
	testMetrics["response-rate-for-broker-0"] = map[string]interface{}{"15m.rate": 0.7922622031773328, "1m.rate": 0.6918979178602331, "5m.rate": 0.777023951053527, "count": 5, "mean.rate": 0.3744806601376294}
	testMetrics["response-size"] = map[string]interface{}{"75%": 503.25, "95%": 1797, "99%": 1797, "99.9%": 1797, "count": 6, "max": 1797, "mean": 359.5, "median": 72, "min": 72, "stddev": 642.8695435311895}
	testMetrics["response-size-for-broker-0"] = map[string]interface{}{"75%": 72, "95%": 72, "99%": 72, "99.9%": 72, "count": 5, "max": 72, "mean": 72, "median": 72, "min": 72, "stddev": 0}
	return testMetrics
}

// Verifies that the metrics response string slice contains the desired values
// This is test code so a quick regex check will suffice rather than parsing the Prometheus output in a more structured manner
func verifyMetric(body []string, name string, topicName string, expectedValue string) bool {
	for _, line := range body {
		if isMatch(line, fmt.Sprintf(`^%s`, name)) &&
			isMatch(line, fmt.Sprintf(`topic="%s"`, topicName)) &&
			isMatch(line, fmt.Sprintf(` %s$`, expectedValue)) {
			return true
		}
	}
	return false
}

// Simple regex match that treats errors as false, for testing only
func isMatch(source string, regex string) bool {
	match, err := regexp.MatchString(regex, source)
	if err != nil {
		return false
	}
	return match
}
