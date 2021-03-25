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
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"go.opencensus.io/metric/metricdata"
	"go.opencensus.io/metric/metricproducer"
	"go.opencensus.io/resource"
	"go.uber.org/zap"
)

// StatsReporter defines the interface for sending ingress metrics.
type StatsReporter interface {
	Report(ReportingList)
	Shutdown()
}

// Verify Reporter Implements StatsReporter Interface
var _ StatsReporter = &Reporter{}

// Define a list of regular expressions that can be applied to a raw incoming Sarama metric in order to produce
// text suitable for the "# HELP" section of the metrics endpoint output
// Note that all of the expressions are executed in-order, so a string such as "request-rate-for-broker-0" will
// first replace "request-rate" with "Requests/second sent to all brokers" and then also replace
// "all brokers-for-broker-0" with " for broker 0" to produce the final description.
var regexDescriptions = []struct {
	Search  *regexp.Regexp
	Replace string
}{
	// Sarama Histograms
	{regexp.MustCompile(`^request-size`), `Distribution of the request size in bytes for all brokers`},
	{regexp.MustCompile(`^request-latency-in-ms`), `Distribution of the request latency in ms for all brokers`},
	{regexp.MustCompile(`^response-size`), `Distribution of the response size in bytes for all brokers`},
	{regexp.MustCompile(`^batch-size`), `Distribution of the number of bytes sent per partition per request for all topics`},
	{regexp.MustCompile(`^records-per-request`), `Distribution of the number of records sent per request for all topics`},
	{regexp.MustCompile(`^compression-ratio`), `Distribution of the compression ratio times 100 of record batches for all topics`},
	{regexp.MustCompile(`^consumer-batch-size`), `Distribution of the number of messages in a batch`},

	// Sarama Meters
	{regexp.MustCompile(`^incoming-byte-rate`), `Bytes/second read of all brokers`},
	{regexp.MustCompile(`^request-rate`), `Requests/second sent to all brokers`},
	{regexp.MustCompile(`^response-rate`), `Responses/second received from all brokers`},
	{regexp.MustCompile(`^record-send-rate`), `Records/second sent to all topics`},
	{regexp.MustCompile(`^outgoing-byte-rate`), `Bytes/second written of all brokers`},

	// Sarama Counters
	{regexp.MustCompile(`^requests-in-flight`), `The current number of in-flight requests awaiting a response for all brokers`},

	// Touch-ups for specific topics/brokers
	{regexp.MustCompile(`all topics-for-topic-(.*)`), `topic "${1}"`},
	{regexp.MustCompile(`all brokers-for-broker-`), `broker `},
}

// The saramaMetricInfo struct holds information related to a particular Sarama metric, used when creating TimeSeries
type saramaMetricInfo struct {
	Name        string
	Description string
	Unit        metricdata.Unit
}

// Since regular expressions are somewhat costly and the metrics are repetitive, this cache will hold a simple
// string-to-saramaMetricInfo direct replacement
var replacementCache = map[string]saramaMetricInfo{}

// Some type aliases for the otherwise unwieldy metric collection map-of-maps-to-interfaces
type ReportingItem = map[string]interface{}
type ReportingList = map[string]ReportingItem

// Define StatsReporter Structure, which implements the OpenCensus Producer interface
type Reporter struct {
	logger  *zap.Logger
	metrics map[string]*metricdata.Metric
	once    sync.Once // Used to add a particular metric producer to the OpenCensus global manager only one time
}

// StatsReporter Constructor
func NewStatsReporter(log *zap.Logger) StatsReporter {
	return &Reporter{
		logger:  log,
		metrics: make(map[string]*metricdata.Metric),
	}
}

//
// Report The Sarama Metrics (go-metrics) Via Knative / OpenCensus Metrics
//
func (r *Reporter) Report(list ReportingList) {

	// Add this Reporter as an OpenCensus Producer, if it has not been done already
	r.once.Do(func() {
		metricproducer.GlobalManager().AddProducer(r)
	})

	// Validate The Metrics
	// Loop Over The Observed Metrics
	for metricKey, metricValue := range list {
		r.recordMetric(metricKey, metricValue)
	}
}

// Remove this producer from the global manager's list so that it will no longer call Read()
func (r *Reporter) Shutdown() {
	metricproducer.GlobalManager().DeleteProducer(r)
}

// Read implements the OpenCensus Producer interface
func (r *Reporter) Read() []*metricdata.Metric {
	metricsArray := make([]*metricdata.Metric, len(r.metrics))
	index := 0
	for name := range r.metrics {
		metricsArray[index] = r.metrics[name]
		index++
	}
	return metricsArray
}

// Creates the metrics for this particular set of reporting items.  For example, the Sarama metric "batch-size":
//   {"75%": X, "95%": X, "99%": X, "99.9%": X, "count": X, "max": X, "mean": X, "median": X, "min": X, "stddev": X}
// requires a collection of TimeSeries values so that they appear in the exporter properly as "one name with different
// tags for the percentile values".
func (r *Reporter) recordMetric(metricKey string, item ReportingItem) {
	timeNow := time.Now()

	if isPercentileMetric(item) {
		// Record this metric as a single collection of TimeSeries values.  Example /metrics output:
		//
		//   # HELP eventing_kafka_request_latency_in_ms Distribution of the request latency in ms for all brokers
		//   # TYPE eventing_kafka_request_latency_in_ms gauge
		//   eventing_kafka_request_latency_in_ms{percentile="50%"} 251
		//   eventing_kafka_request_latency_in_ms{percentile="75%"} 252
		//   eventing_kafka_request_latency_in_ms{percentile="95%"} 254
		//   eventing_kafka_request_latency_in_ms{percentile="99%"} 256
		//   eventing_kafka_request_latency_in_ms{percentile="99.9%"} 3009
		//   eventing_kafka_request_latency_in_ms{percentile="max"} 3009
		//   eventing_kafka_request_latency_in_ms{percentile="mean"} 241.8544891640867
		//   eventing_kafka_request_latency_in_ms{percentile="min"} 0
		//   eventing_kafka_request_latency_in_ms{percentile="stddev"} 119.7515250132759
		//   # HELP eventing_kafka_request_latency_in_ms_count Distribution of the request latency in ms for all brokers (count)
		//   # TYPE eventing_kafka_request_latency_in_ms_count gauge
		//   eventing_kafka_request_latency_in_ms_count 646
		//
		r.recordPercentileMetric(timeNow, metricKey, item)
	} else {
		// Otherwise export all of the individual values as their own metrics.  Example /metrics output:
		//
		//   # HELP eventing_kafka_request_rate_15m_rate Requests/second sent to all brokers: 15-Minute Rate
		//   # TYPE eventing_kafka_request_rate_15m_rate gauge
		//   eventing_kafka_request_rate_15m_rate 0.8275150616643104
		//   # HELP eventing_kafka_request_rate_1m_rate Requests/second sent to all brokers: 1-Minute Rate
		//   # TYPE eventing_kafka_request_rate_1m_rate gauge
		//   eventing_kafka_request_rate_1m_rate 3.96042677919562
		//   # HELP eventing_kafka_request_rate_5m_rate Requests/second sent to all brokers: 5-Minute Rate
		//   # TYPE eventing_kafka_request_rate_5m_rate gauge
		//   eventing_kafka_request_rate_5m_rate 1.8089955139418779
		//
		for subKey, value := range item {
			info := getMetricSubInfo(metricKey, subKey)
			r.metrics[info.Name] = &metricdata.Metric{
				Descriptor: metricdata.Descriptor{
					Name:        info.Name,
					Description: info.Description,
					Unit:        info.Unit,
					Type:        metricdata.TypeGaugeFloat64,
				},
				TimeSeries: []*metricdata.TimeSeries{{
					Points:    []metricdata.Point{r.newPoint(timeNow, value)},
					StartTime: timeNow,
				}},
				Resource: &resource.Resource{Type: info.Name},
			}
		}
	}
}

// recordPercentileMetric takes a ReportingItem and breaks it apart into individual TimeSeries
// elements that can be recorded as a single metric name with different tags, which makes graphing
// the metric simpler.
//
// Note:  There is a metric type of metricdata.TypeSummary that would be somewhat simpler to use than
//        creating all of the TimeSeries entries manually, but it is not (as of this writing) implemented
//        in the OpenCensus Go exporter and instead returns a nil output with no error (see
//        contrib.go.opencensus.io/exporter/prometheus/prometheus.go::toPromMetric).  It is implemented in
//        the parallel Java version of the code (see exporter/stats/prometheus/PrometheusExportUtils.java
//        in the opencensus-instrumentation project) and so may be ported at some point.
//
func (r *Reporter) recordPercentileMetric(metricTime time.Time, metricKey string, item ReportingItem) {

	info := getMetricInfo(metricKey)

	// Create a TimeSeries for each percentile item in the ReportingItem provided
	timeSeries := make([]*metricdata.TimeSeries, 0, 10)
	for key, value := range item {
		label := key
		if key == "median" {
			label = "50%" // For visual consistency, since the other values are percentage strings
		}
		// Count isn't the same unit as anything else, so don't put it in this timeseries
		if key != "count" {
			timeSeries = append(timeSeries, &metricdata.TimeSeries{
				LabelValues: []metricdata.LabelValue{{Value: label, Present: true}},
				Points:      []metricdata.Point{r.newPoint(metricTime, value)},
			})
		}
	}

	// Add the array of TimeSeries values to the metric map that is part of this Reporter, so that it will
	// be exported when the Read() function is called (via the GetAll() function of the metricproducer's Manager)
	r.metrics[metricKey] = &metricdata.Metric{
		Descriptor: metricdata.Descriptor{
			Name:        info.Name,
			Description: info.Description,
			Unit:        info.Unit,
			Type:        metricdata.TypeGaugeFloat64, // Because some fields like "mean" are always floats
			LabelKeys:   []metricdata.LabelKey{{Key: "percentile"}},
		},
		TimeSeries: timeSeries,
		Resource:   &resource.Resource{Type: metricKey},
	}

	// Put the count, if present, in its own metric, as it is not the same type as the other values
	if countValue, ok := item["count"]; ok {
		countName := metricKey + "_count"
		r.metrics[countName] = &metricdata.Metric{
			Descriptor: metricdata.Descriptor{
				Name:        countName,
				Description: info.Description + " (count)",
				Unit:        metricdata.UnitDimensionless,
				Type:        metricdata.TypeGaugeInt64, // a count is always an int
			},
			TimeSeries: []*metricdata.TimeSeries{{
				Points:    []metricdata.Point{r.newPoint(metricTime, countValue)},
				StartTime: metricTime,
			}},
			Resource: &resource.Resource{Type: countName},
		}
	}
}

// newPoint creates a Point structure using the specific type of the value provided.
// Note that currently all of the mechanisms for generating a Point do exactly the same
// thing, and that Point.Value is an interface{} internally, so the only real benefit of
// the type switch is to prevent non-numeric data from getting into a Point struct
// (and log a warning to that effect).
func (r *Reporter) newPoint(t time.Time, value interface{}) metricdata.Point {
	// Type-switches don't support "fallthrough" so each individual possible type must have its own
	// somewhat-redundant code block.  Not all types are used by Sarama at the moment; if a new type is
	// added, a warning will be logged here.
	switch value := value.(type) {
	case float64:
		return metricdata.NewFloat64Point(t, value)
	case float32:
		return metricdata.NewFloat64Point(t, float64(value))
	case int64:
		return metricdata.NewInt64Point(t, value)
	case int32:
		return metricdata.NewInt64Point(t, int64(value))
	case int:
		return metricdata.NewInt64Point(t, int64(value))
	default:
		r.logger.Warn("Could not interpret Sarama measurement as a number; using zero", zap.Any("Sarama Value", value))
		return metricdata.NewInt64Point(t, 0)
	}
}

// isPercentileMetric returns true if the ReportingItem provided is a collection of percentile values
// For example, an item containing values for 75%, 95%, 99%, and 99.9%
func isPercentileMetric(item ReportingItem) bool {
	for key := range item {
		// Assume that if any item key has a "%" in it, then it should be treated as a percentile metric
		if strings.Contains(key, "%") {
			return true
		}
	}
	return false
}

// getMetricUnit returns the proper unit for a given Sarama metric name
func getMetricUnit(metricKey string) metricdata.Unit {
	if strings.Contains(metricKey, "-in-ms") {
		return metricdata.UnitMilliseconds
	}
	return metricdata.UnitDimensionless
}

// getMetricInfo returns pretty descriptions for known Sarama metrics
func getMetricInfo(metricKey string) saramaMetricInfo {
	if cachedReplacement, ok := replacementCache[metricKey]; ok {
		return cachedReplacement
	}
	newString := metricKey
	for _, replacement := range regexDescriptions {
		newString = replacement.Search.ReplaceAllString(newString, replacement.Replace)
	}

	info := saramaMetricInfo{
		Name:        metricKey,
		Description: newString,
		Unit:        getMetricUnit(metricKey),
	}
	replacementCache[metricKey] = info
	return info
}

// getMetricSubInfo returns pretty descriptions for known Sarama submetrics
func getMetricSubInfo(main string, sub string) saramaMetricInfo {
	if cachedReplacement, ok := replacementCache[main+sub]; ok {
		return cachedReplacement
	}
	// Run through the list of known replacements that should be made (multiple replacements may happen)
	info := getMetricInfo(main)
	info.Name = fmt.Sprintf("%s.%s", main, sub)
	info.Description += ": " + getSubDescription(sub)
	info.Unit = getMetricUnit(main)
	replacementCache[main+sub] = info
	return info
}

// getSubDescription returns pretty descriptions for known Sarama sub-metric categories
func getSubDescription(sub string) string {
	switch sub {
	case "1m.rate":
		return "1-Minute Rate"
	case "5m.rate":
		return "5-Minute Rate"
	case "15m.rate":
		return "15-Minute Rate"
	case "count":
		return "Count"
	case "max":
		return "Maximum"
	case "mean":
		return "Mean"
	case "mean.rate":
		return "Mean Rate"
	case "median":
		return "Median"
	case "min":
		return "Minimum"
	case "stddev":
		return "Standard Deviation"
	default:
		return sub
	}
}
