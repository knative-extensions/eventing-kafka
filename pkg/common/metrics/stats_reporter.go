package metrics

import (
	"context"
	"encoding/json"
	"log"
	"strconv"

	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
	"go.uber.org/zap"
	"knative.dev/pkg/metrics"
)

const (
	// LabelProducer is the label for the immutable name of the producer.
	LabelProducer = "producer"

	// LabelConsumer is the label for the immutable name of the consumer.
	LabelConsumer = "consumer"

	// LabelTopic is the label for the immutable name of the topic.
	LabelTopic = "topic"

	// LabelPartition is the label for the immutable name of the partition.
	LabelPartition = "partition"
)

var (
	// producedMessageCount is a counter which records the number of events produced
	producedMessageCount = stats.Int64(
		"produced_msg_count", // The METRICS_DOMAIN will be prepended to the name.
		"Produced Message Count",
		stats.UnitDimensionless,
	)

	// consumedMessageCount is a counter which records the number of events consumed
	consumedMessageCount = stats.Int64(
		"consumed_msg_count", // The METRICS_DOMAIN will be prepended to the name.
		"Consumed Message Count",
		stats.UnitDimensionless,
	)

	// Create the tag keys that will be used to add tags to our measurements.
	// Tag keys must conform to the restrictions described in
	// go.opencensus.io/tag/validate.go. Currently those restrictions are:
	// - length between 1 and 255 inclusive
	// - characters are printable US-ASCII
	producer  = tag.MustNewKey(LabelProducer)
	consumer  = tag.MustNewKey(LabelConsumer)
	topic     = tag.MustNewKey(LabelTopic)
	partition = tag.MustNewKey(LabelPartition)
)

// ReportArgs is the parent structure for the producer and consumer child structures
type ReportArgs struct {
	Topic     string
	Partition string
}

// ProducerReportArgs contains the fields representing the values of interest for the produced_msg_count custom metric
type ProducerReportArgs struct {
	ReportArgs
	Producer string
}

// ConsumerReportArgs contains the fields representing the values of interest for the consumed_msg_count custom metric
type ConsumerReportArgs struct {
	ReportArgs
	Consumer string
}

func init() {
	register()
}

// StatsReporter defines the interface for sending ingress metrics.
type StatsReporter interface {
	ReportProducedEvent(args *ProducerReportArgs, msgCount int64) error
	ReportConsumedEvent(args *ConsumerReportArgs, msgCount int64) error
	Report(stats string)
}

var _ StatsReporter = (*reporter)(nil)
var emptyContext = context.Background()

// Reporter holds cached metric objects to report message metrics
type reporter struct {
	logger *zap.Logger
}

// NewStatsReporter creates a reporter that collects and reports ingress metrics
func NewStatsReporter(log *zap.Logger) StatsReporter {
	return &reporter{logger: log}
}

// Register the opencensus View structures so that we can record our custom metrics
func register() {
	// Create view to see our measurements.
	err := view.Register(&view.View{
		Description: producedMessageCount.Description(),
		Measure:     producedMessageCount,
		Aggregation: view.LastValue(),
		TagKeys: []tag.Key{
			producer,
			topic,
			partition},
	}, &view.View{
		Description: consumedMessageCount.Description(),
		Measure:     consumedMessageCount,
		Aggregation: view.LastValue(),
		TagKeys: []tag.Key{
			consumer,
			topic,
			partition},
	})
	if err != nil {
		log.Printf("failed to register opencensus views, %v", err)
	}

}

// ReportProducedEventCount captures the event count
func (r *reporter) ReportProducedEvent(args *ProducerReportArgs, msgCount int64) error {
	ctx, err := r.generateProducerTag(args)
	if err != nil {
		return err
	}
	metrics.Record(ctx, producedMessageCount.M(msgCount))
	return nil
}

// ReportConsumedEventCount captures dispatch times
func (r *reporter) ReportConsumedEvent(args *ConsumerReportArgs, msgCount int64) error {
	ctx, err := r.generateConsumerTag(args)
	if err != nil {
		return err
	}
	metrics.Record(ctx, consumedMessageCount.M(msgCount))
	return nil
}

// Adds the producer, topic, and partition tags to a context for the metrics.Record call
func (r *reporter) generateProducerTag(args *ProducerReportArgs) (context.Context, error) {
	return tag.New(
		emptyContext,
		tag.Insert(producer, args.Producer),
		tag.Insert(topic, args.Topic),
		tag.Insert(partition, args.Partition),
	)
}

// Adds the consumer, topic, and partition tags to a context for the metrics.Record call
func (r *reporter) generateConsumerTag(args *ConsumerReportArgs) (context.Context, error) {
	return tag.New(
		emptyContext,
		tag.Insert(consumer, args.Consumer),
		tag.Insert(topic, args.Topic),
		tag.Insert(partition, args.Partition),
	)
}

// Report parses a collection of Kafka metrics taken from a kafkaproducer.ProducerInterface structure
// and extracts the ones that are of interest to eventing-kafka (namely the produced and received
// message counts).  It then updates those individual metrics with the help of the knative-eventing
// metrics.Record function.
func (r *reporter) Report(stats string) {
	r.logger.Info("New Producer Metrics Reported")
	var statsMap map[string]interface{}
	err := json.Unmarshal([]byte(stats), &statsMap)

	// If Unable To Parse Log And Move On
	if err != nil {
		r.logger.Warn("Unable To Parse Kafka Metrics", zap.Error(err))
		return
	}

	// Name Of Producer Or Consumer
	name := statsMap["name"].(string)

	// Loop Over Topics And Partitions Updating Metrics
	for _, t := range statsMap["topics"].(map[string]interface{}) {
		topic := t.(map[string]interface{})
		topicName := topic["topic"].(string)

		for _, partition := range topic["partitions"].(map[string]interface{}) {
			r.updateProducedMessageCount(name, topicName, partition.(map[string]interface{}), "txmsgs")
			r.updateReceivedMessageCount(name, topicName, partition.(map[string]interface{}), "rxmsgs")
		}
	}
}

// Extract the single produced message count metric from the partition map and report it as a custom metric
func (r *reporter) updateProducedMessageCount(name string, topicName string, partition map[string]interface{}, field string) {

	partitionNum := partition["partition"].(float64)
	partitionNumStr := strconv.FormatFloat(partitionNum, 'f', -1, 64)

	if partitionNumStr != "-1" {
		msgCount := partition[field].(float64)

		reportArgs := &ProducerReportArgs{
			ReportArgs: ReportArgs{Topic: topicName, Partition: partitionNumStr},
			Producer:   name,
		}
		err := r.ReportProducedEvent(reportArgs, int64(msgCount))
		if err != nil {
			r.logger.Error("ReportProducedEvent failed", zap.Error(err))
		}
	}
}

// Extract the single consumed message count metric from the partition map and report it as a custom metric
func (r *reporter) updateReceivedMessageCount(name string, topicName string, partition map[string]interface{}, field string) {

	partitionNum := partition["partition"].(float64)
	partitionNumStr := strconv.FormatFloat(partitionNum, 'f', -1, 64)

	if partitionNumStr != "-1" {
		msgCount := partition[field].(float64)

		reportArgs := &ConsumerReportArgs{
			ReportArgs: ReportArgs{Topic: topicName, Partition: partitionNumStr},
			Consumer:   name,
		}
		err := r.ReportConsumedEvent(reportArgs, int64(msgCount))
		if err != nil {
			r.logger.Error("ReportConsumedEvent failed", zap.Error(err))
		}
	}
}
