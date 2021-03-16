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
	"context"
	"fmt"
	"regexp"

	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
	"go.uber.org/zap"
	"knative.dev/pkg/metrics"
)

// StatsReporter defines the interface for sending ingress metrics.
type StatsReporter interface {
	Report(ReportingList)
}

// Verify StatsReporter Implements StatsReporter Interface
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

// Since regular expressions are somewhat costly and the metrics are repetitive, this cache will hold a simple
// string-to-string direct replacement
var replacementCache = map[string]string{}

// Define StatsReporter Structure
type Reporter struct {
	views        map[string]*view.View
	measurements map[string]stats.Measure
	logger       *zap.Logger
	tagCtx       context.Context
}

// StatsReporter Constructor
func NewStatsReporter(log *zap.Logger) StatsReporter {
	return &Reporter{
		views:        make(map[string]*view.View),
		logger:       log,
		tagCtx:       context.Background(),
		measurements: make(map[string]stats.Measure)}
}

// Our RecordWrapper, which defaults to the knative metrics.Record()
// This wrapper function facilitates minimally-invasive unit testing of the
// Report functionality without requiring live servers to be started.
var RecordWrapper = metrics.Record

// Some type aliases for the otherwise unwieldy metric collection map-of-maps-to-interfaces
type ReportingItem = map[string]interface{}
type ReportingList = map[string]ReportingItem

//
// Report The Sarama Metrics (go-metrics) Via Knative / OpenCensus Metrics
//
func (r *Reporter) Report(list ReportingList) {

	// Validate The Metrics
	if len(list) > 0 {

		// Loop Over The Observed Metrics
		for metricKey, metricValue := range list {

			// Record Each Individual Metric Item
			for saramaKey, saramaValue := range metricValue {
				r.recordMeasurement(metricKey, saramaKey, saramaValue)
			}
		}
	}
}

// Creates and registers a new view in the OpenCensus context, adding it to the Reporter's known views
// Note:  OpenCensus has a "view.Distribution" that can be used for the Aggregation field, but the Sarama
//        measurements (1-Minute Rate, 5-Minute Rate, etc.) are already in a collective form, so the only way
//        to record them via OpenCensus is to use the individual measurements and offer them to the end-user
//        as individual views for whatever purpose they desire.  This isn't perfect and it may be better in
//        the future to switch to something other than the default OpenCensus recorder.
func (r *Reporter) createView(ctx context.Context, measure stats.Measure, name string, description string) (*view.View, error) {
	key, err := tag.NewKey(name)
	if err != nil {
		return nil, err
	}
	ctx, err = tag.New(ctx, tag.Insert(key, name))
	if err != nil {
		return nil, err
	}

	newView := &view.View{
		Name:        name,
		Description: description,
		Measure:     measure,
		Aggregation: view.LastValue(), // Sarama already sums or otherwise aggregates its metrics, so only LastValue is useful here
	}
	err = view.Register(newView)
	if err != nil {
		return nil, err
	}
	r.views[name] = newView
	return newView, nil
}

// Record a measurement to the metrics backend, creating a new OpenCensus view if this is a new measurement
func (r *Reporter) recordMeasurement(metricKey string, saramaKey string, value interface{}) {

	name := fmt.Sprintf("%s.%s", metricKey, saramaKey)
	description := getDescription(metricKey, saramaKey)

	// Type-switches don't support "fallthrough" so each individual possible type must have its own
	// somewhat-redundant code block.  Not all types are used by Sarama at the moment; if a new type is
	// added, a warning will be logged here.
	// Note:  The Int64Measure wrapper converts to a float64 internally anyway so there is no particular
	//        advantage in treating int-types separately here.
	switch value := value.(type) {
	case int32:
		r.recordFloat(float64(value), name, description)
	case int64:
		r.recordFloat(float64(value), name, description)
	case float64:
		r.recordFloat(value, name, description)
	case float32:
		r.recordFloat(float64(value), name, description)
	default:
		r.logger.Warn("Could not interpret Sarama measurement as a number", zap.Any("Sarama Value", value))
	}
}

// Record a measurement that is represented by a float64 value
func (r *Reporter) recordFloat(value float64, name string, description string) {
	var err error
	floatView, ok := r.views[name]
	if !ok {
		floatView, err = r.createView(r.tagCtx, stats.Float64(name, description, stats.UnitDimensionless), name, description)
		if err != nil {
			r.logger.Error("Failed to register OpenCensus views", zap.Error(err))
			return
		}
	}
	RecordWrapper(r.tagCtx, floatView.Measure.(*stats.Float64Measure).M(value))
}

// Returns pretty descriptions for known Sarama metrics
func getDescription(main string, sub string) string {
	if cachedReplacement, ok := replacementCache[main+sub]; ok {
		return cachedReplacement
	}
	// Run through the list of known replacements that should be made (multiple replacements may happen)
	newString := main
	for _, replacement := range regexDescriptions {
		newString = replacement.Search.ReplaceAllString(newString, replacement.Replace)
	}
	newString += ": " + getSubDescription(sub)
	replacementCache[main+sub] = newString
	return newString
}

// Returns pretty descriptions for known Sarama sub-metric categories
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
