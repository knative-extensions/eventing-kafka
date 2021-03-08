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

type ReportingItem = map[string]interface{}
type ReportingList = map[string]ReportingItem

//
// Report The Sarama Metrics (go-metrics) Via Knative / OpenCensus Metrics
//
// NOTE - Sarama provides lots of metrics which would be good to expose, but for now
//        we're just quickly parsing out message counts.  This is for rough parity
//        with the prior Confluent implementation and due to uncertainty around
//        integrating with Knative Observability and potentially Sarama v2 using
//        OpenTelemetry directly as described here...
//
//			https://github.com/Shopify/sarama/issues/1340
//
//        If we do decide to expose all available metrics, the following library might be useful...
//
//          https://github.com/deathowl/go-metrics-prometheus
//
//        Further the Sarama Consumer metrics don't track messages so we might need/want
//        to manually track produced/consumed messages at the Topic/Partition/ConsumerGroup
//        level.
//
func (r *Reporter) Report(list ReportingList) {

	// Validate The Metrics
	if len(list) > 0 {

		// Loop Over The Observed Metrics
		for metricKey, metricValue := range list {

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

	intMeasure := stats.Int64(name, description, stats.UnitDimensionless)
	floatMeasure := stats.Float64(name, description, stats.UnitDimensionless)
	var measure stats.Measure

	switch value := value.(type) {
	case int64:
		measure = intMeasure
		RecordWrapper(r.tagCtx, intMeasure.M(value))
	case int32:
		measure = intMeasure
		RecordWrapper(r.tagCtx, intMeasure.M(int64(value)))
	case int:
		measure = intMeasure
		RecordWrapper(r.tagCtx, intMeasure.M(int64(value)))
	case float64:
		measure = floatMeasure
		RecordWrapper(r.tagCtx, floatMeasure.M(value))
	case float32:
		measure = floatMeasure
		RecordWrapper(r.tagCtx, floatMeasure.M(float64(value)))
	default:
		r.logger.Warn("Could not interpret measurement as a number", zap.Any("Sarama Value", value))
	}

	if _, ok := r.views[name]; !ok {
		// This is the first time this measurement is being taken; add it to the views
		r.tagCtx = r.createView(r.tagCtx, measure, name, description)
	}
}

// Returns pretty descriptions for known Sarama metrics
func getDescription(main string, sub string) string {
	switch main {
	case "incoming-byte-rate":
		return "Incoming Byte Rate: " + getSubDescription(sub)
	case "request-rate":
		return "Request Rate: " + getSubDescription(sub)
	case "request-size":
		return "Request Size: " + getSubDescription(sub)
	case "request-latency-in-ms":
		return "Request Latency (ms): " + getSubDescription(sub)
	case "outgoing-byte-rate":
		return "Outgoing Byte Rate: " + getSubDescription(sub)
	case "response-rate":
		return "Response Rate: " + getSubDescription(sub)
	case "response-size":
		return "Response Size: " + getSubDescription(sub)
	case "requests-in-flight":
		return "Requests in Flight: " + getSubDescription(sub)
	default:
		return main + ": " + getSubDescription(sub)
	}
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
