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
	"log"
	"strings"

	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
	"go.uber.org/zap"
	"knative.dev/pkg/metrics"
)

const (

	// LabelTopic is the label for the immutable name of the topic.
	LabelTopic = "topic"

	// Sarama Metrics
	RecordSendRateForTopicPrefix = "record-send-rate-for-topic-"
)

var (
	// Counter For The Number Of Events Produced To A Kafka Topic
	producedMessageCount = stats.Int64(
		"produced_msg_count", // The METRICS_DOMAIN will be prepended to the name.
		"Produced Message Count",
		stats.UnitDimensionless,
	)

	// Create the tag keys that will be used to add tags to our measurements in order to validate
	// that they conform to the restrictions described in go.opencensus.io/tag/validate.go.
	// Currently those restrictions are...
	//   - Length between 1 and 255 inclusive
	//   - Characters are printable US-ASCII
	topic = tag.MustNewKey(LabelTopic)
)

// Register the OpenCensus View Structures
func init() {

	// Create A View To See Our Metric
	err := view.Register(&view.View{
		Description: producedMessageCount.Description(),
		Measure:     producedMessageCount,
		Aggregation: view.LastValue(),
		TagKeys:     []tag.Key{topic},
	})
	if err != nil {
		log.Printf("failed to register opencensus views, %v", err)
	}
}

// StatsReporter defines the interface for sending ingress metrics.
type StatsReporter interface {
	Report(map[string]map[string]interface{})
}

// Verify StatsReporter Implements StatsReporter Interface
var _ StatsReporter = &Reporter{}

// Define StatsReporter Structure
type Reporter struct {
	logger *zap.Logger
}

// StatsReporter Constructor
func NewStatsReporter(log *zap.Logger) StatsReporter {
	return &Reporter{logger: log}
}

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
func (r *Reporter) Report(stats map[string]map[string]interface{}) {

	// Validate The Metrics
	if len(stats) > 0 {

		// Loop Over The Observed Metrics
		for metricKey, metricValue := range stats {

			// Only Handle Specific Metrics
			if strings.HasPrefix(metricKey, RecordSendRateForTopicPrefix) {
				topicName := strings.TrimPrefix(metricKey, RecordSendRateForTopicPrefix)
				msgCount, ok := metricValue["count"].(int64)
				if ok {

					// Create A New OpenCensus Tag / Context for The Topic
					ctx, err := tag.New(
						context.Background(),
						tag.Insert(topic, topicName),
					)
					if err != nil {
						r.logger.Error("Failed To Create New OpenCensus Tag For Kafka Topic", zap.String("Topic", topicName))
						return
					}

					// Record The Produced Message Count Metric
					metrics.Record(ctx, producedMessageCount.M(msgCount))

				} else {
					r.logger.Warn("Encountered Non Int64 'count' Field In Metric", zap.String("Metric", metricKey))
				}
			}
		}
	}
}
