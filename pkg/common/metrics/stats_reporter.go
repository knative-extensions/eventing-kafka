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
// first replace "request-rate" with "Request Rate" and then also replace "-for-broker-0" with " for Broker 0"
var regexDescriptions = []struct {
	Search  *regexp.Regexp
	Replace string
}{
	{regexp.MustCompile(`incoming-byte-rate`), "Incoming Byte Rate"},
	{regexp.MustCompile(`request-rate`), "Request Rate"},
	{regexp.MustCompile(`request-size`), "Request Size"},
	{regexp.MustCompile(`request-latency-in-ms`), "Request Latency (ms)"},
	{regexp.MustCompile(`outgoing-byte-rate`), "Outgoing Byte Rate"},
	{regexp.MustCompile(`response-rate`), "Response Rate"},
	{regexp.MustCompile(`response-size`), "Response Size"},
	{regexp.MustCompile(`requests-in-flight`), "Requests in Flight"},
	{regexp.MustCompile(`compression-ratio`), "Compression Ratio"},
	{regexp.MustCompile(`batch-size`), "Batch Size"},
	{regexp.MustCompile(`record-send-rate`), "Record Send Rate"},
	{regexp.MustCompile(`records-per-request`), "Records Per Request"},
	{regexp.MustCompile(`-for-topic-`), " for Topic "},
	{regexp.MustCompile(`-for-broker-`), " for Broker "},
}

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
func (r *Reporter) createView(ctx context.Context, measure stats.Measure, name string, description string) context.Context {
	key, err := tag.NewKey(name)
	if err != nil {
		r.logger.Error("Failed To Create New OpenCensus Key For Sarama Metric", zap.String("Key", name), zap.Error(err))
		return ctx
	}
	ctx, err = tag.New(ctx, tag.Insert(key, name))
	if err != nil {
		r.logger.Error("Failed To Create New OpenCensus Tag For Sarama Metric", zap.String("Key", name), zap.Error(err))
		return ctx
	}

	newView := &view.View{
		Name:        name,
		Description: description,
		Measure:     measure,
		Aggregation: view.LastValue(),
	}
	err = view.Register(newView)
	if err != nil {
		r.logger.Error("failed to register opencensus views", zap.Error(err))
		return ctx
	}
	r.views[name] = newView
	return ctx
}

// Record a measurement to the metrics backend, creating a new OpenCensus view if this is a new measurement
func (r *Reporter) recordMeasurement(metricKey string, saramaKey string, value interface{}) {

	name := fmt.Sprintf("%s.%s", metricKey, saramaKey)
	description := getDescription(metricKey, saramaKey)

	// Type-switches don't support "fallthrough" so each individual possible type must have its own
	// somewhat-redundant code block.  Not all types are used by Sarama at the moment; if a new type is
	// added, a warning will be logged here.
	switch value := value.(type) {
	case int32:   r.recordInt(int64(value), name, description)
	case int64:   r.recordInt(value, name, description)
	case float64: r.recordFloat(value, name, description)
	case float32: r.recordFloat(float64(value), name, description)
	default:
		r.logger.Warn("Could not interpret Sarama measurement as a number", zap.Any("Sarama Value", value))
	}
}

// Record a measurement that is represented by an int64 value
func (r *Reporter) recordInt(value int64, name string, description string) {
	measure := stats.Int64(name, description, stats.UnitDimensionless)
	RecordWrapper(r.tagCtx, measure.M(value))
	r.createViewIfNecessary(measure, name, description)
}

// Record a measurement that is represented by a float64 value
func (r *Reporter) recordFloat(value float64, name string, description string) {
	measure := stats.Float64(name, description, stats.UnitDimensionless)
	RecordWrapper(r.tagCtx, measure.M(value))
	r.createViewIfNecessary(measure, name, description)
}

// Adds a view for this measurement to this Reporter's map of known views, if not already present
func (r *Reporter) createViewIfNecessary(measure stats.Measure, name string, description string) {
	if _, ok := r.views[name]; !ok {
		r.tagCtx = r.createView(r.tagCtx, measure, name, description)
	}
}

// Returns pretty descriptions for known Sarama metrics
func getDescription(main string, sub string) string {
	// Run through the list of known replacements that should be made (multiple replacements may happen)
	for _, replacement := range regexDescriptions {
		main = replacement.Search.ReplaceAllString(main, replacement.Replace)
	}
	return main + ": " + getSubDescription(sub)
}

// Returns pretty descriptions for known Sarama sub-metric categories
func getSubDescription(sub string) string {
	switch sub {
	case "1m.rate": return "1-Minute Rate"
	case "5m.rate": return "5-Minute Rate"
	case "15m.rate": return "15-Minute Rate"
	case "count": return "Count"
	case "max": return "Maximum"
	case "mean": return "Mean"
	case "mean.rate": return "Mean Rate"
	case "median": return "Median"
	case "min": return "Minimum"
	case "stddev": return "Standard Deviation"
	default: return sub
	}
}
