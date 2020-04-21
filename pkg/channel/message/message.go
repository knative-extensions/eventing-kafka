package message

import (
	"github.com/cloudevents/sdk-go/v1/cloudevents"
	"github.com/cloudevents/sdk-go/v1/cloudevents/types"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"knative.dev/eventing-kafka/pkg/channel/constants"
	"go.uber.org/zap"
	"strconv"
	"time"
)

// Create A Kafka Message From The Specified CloudEvent / Topic
func CreateKafkaMessage(logger *zap.Logger, event cloudevents.Event, kafkaTopic string) (*kafka.Message, error) {

	// Get The Event's Data Bytes
	eventBytes, err := event.DataBytes()
	if err != nil {
		logger.Error("Failed To Get CloudEvent's DataBytes", zap.Error(err))
		return nil, err
	}

	// Get Kafka Message Headers From The Specified CloudEvent Context
	kafkaHeaders := getKafkaHeaders(logger, event.Context)

	// Get The Partition Key From The CloudEvent
	partitionKey := getPartitionKey(event)

	// Create The Kafka Message
	kafkaMessage := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &kafkaTopic,
			Partition: kafka.PartitionAny, // Required For Producer Level Partitioner! (see KafkaProducerConfigPropertyPartitioner)
		},
		Key:     partitionKey,
		Value:   eventBytes,
		Headers: kafkaHeaders,
	}

	// Return The Constructed Kafka Message
	return kafkaMessage, nil
}

// Create Kafka Message Headers From The Specified CloudEvent Context
func getKafkaHeaders(logger *zap.Logger, context cloudevents.EventContext) []kafka.Header {

	kafkaHeaders := []kafka.Header{
		{Key: constants.CeKafkaHeaderKeySpecVersion, Value: []byte(context.GetSpecVersion())},
		{Key: constants.CeKafkaHeaderKeyType, Value: []byte(context.GetType())},
		{Key: constants.CeKafkaHeaderKeySource, Value: []byte(context.GetSource())},
		{Key: constants.CeKafkaHeaderKeyId, Value: []byte(context.GetID())},
		{Key: constants.CeKafkaHeaderKeyTime, Value: []byte(context.GetTime().Format(time.RFC3339))},
	}

	if context.GetDataContentType() != "" {
		kafkaHeaders = append(kafkaHeaders, kafka.Header{Key: constants.CeKafkaHeaderKeyDataContentType, Value: []byte(context.GetDataContentType())})
	}

	if context.GetSubject() != "" {
		kafkaHeaders = append(kafkaHeaders, kafka.Header{Key: constants.CeKafkaHeaderKeySubject, Value: []byte(context.GetSubject())})
	}

	if context.GetDataSchema() != "" {
		kafkaHeaders = append(kafkaHeaders, kafka.Header{Key: constants.CeKafkaHeaderKeyDataSchema, Value: []byte(context.GetDataSchema())})
	}

	// Only Supports string, int, and float64 Extensions
	for k, v := range context.GetExtensions() {
		logger.Debug("Add Event Extensions", zap.Any(k, v))
		if vs, ok := v.(string); ok {
			kafkaHeaders = append(kafkaHeaders, kafka.Header{Key: "ce_" + k, Value: []byte(vs)})
		} else if vi, ok := v.(int); ok {
			strInt := strconv.Itoa(vi)
			kafkaHeaders = append(kafkaHeaders, kafka.Header{Key: "ce_" + k, Value: []byte(strInt)})
		} else if vf, ok := v.(float64); ok {
			strFloat := strconv.FormatFloat(vf, 'f', -1, 64)
			kafkaHeaders = append(kafkaHeaders, kafka.Header{Key: "ce_" + k, Value: []byte(strFloat)})
		}
	}

	return kafkaHeaders
}

// Precedence For Partitioning Is The CloudEvent PartitionKey Extension Followed By The CloudEvent Subject
func getPartitionKey(event cloudevents.Event) []byte {

	// Use The CloudEvent Extensions PartitionKey If It Exists
	pkExtension, err := types.ToString(event.Extensions()[constants.ExtensionKeyPartitionKey])
	if err == nil && len(pkExtension) > 0 {
		return []byte(pkExtension)
	}

	// Otherwise Attempt To Use The CloudEvent's Subject
	if len(event.Subject()) > 0 {
		return []byte(event.Subject())
	}

	// Otherwise Return Nil ?
	return nil
}
