package producer

import (
	"errors"
	"github.com/cloudevents/sdk-go/v1/cloudevents"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"knative.dev/eventing-kafka/pkg/channel/health"
	"knative.dev/eventing-kafka/pkg/channel/message"
	"knative.dev/eventing-kafka/pkg/channel/util"
	kafkaproducer "knative.dev/eventing-kafka/pkg/common/kafka/producer"
	"knative.dev/eventing-kafka/pkg/common/prometheus"
	"go.uber.org/zap"
	eventingChannel "knative.dev/eventing/pkg/channel"
)

// Package Variables
var (
	logger         *zap.Logger
	kafkaProducer  kafkaproducer.ProducerInterface
	stopChannel    chan struct{}
	stoppedChannel chan struct{}
	metrics        *prometheus.MetricsServer
)

// Wrapper Around Common Kafka Producer Creation To Facilitate Unit Testing
var createProducerFunctionWrapper = func(brokers string, username string, password string) (kafkaproducer.ProducerInterface, error) {
	return kafkaproducer.CreateProducer(brokers, username, password)
}

// Initialize The Producer
func InitializeProducer(lgr *zap.Logger, brokers string, username string, password string, metricsServer *prometheus.MetricsServer, healthServer *health.Server) error {

	// Set Package Level Logger Reference
	logger = lgr

	// Mark The Kafka Producer As Ready
	healthServer.SetProducerReady(false)

	// Create The Producer Using The Specified Kafka Authentication
	producer, err := createProducerFunctionWrapper(brokers, username, password)
	if err != nil {
		logger.Error("Failed To Create Kafka Producer - Exiting", zap.Error(err))
		return err
	}

	// Assign The Producer Singleton Instance
	logger.Info("Successfully Created Kafka Producer")
	kafkaProducer = producer

	// Reset The Stop Channels
	stopChannel = make(chan struct{})
	stoppedChannel = make(chan struct{})

	// Used For Reporting Kafka Metrics
	metrics = metricsServer

	// Fork A Go Routine To Process Kafka Events Asynchronously
	logger.Info("Starting Kafka Producer Event Processing")
	go processProducerEvents(healthServer)

	// Return Success
	logger.Info("Successfully Initialized Kafka Producer")
	healthServer.SetProducerReady(true)
	return nil
}

// Produce A KafkaMessage From The Specified CloudEvent To The Specified Topic And Wait For The Delivery Report
func ProduceKafkaMessage(event cloudevents.Event, channelReference eventingChannel.ChannelReference) error {

	// Validate The Kafka Producer (Must Be Pre-Initialized)
	if kafkaProducer == nil {
		logger.Error("Kafka Producer Not Initialized - Unable To Produce Message")
		return errors.New("uninitialized kafka producer - unable to produce message")
	}

	// Get The Topic Name From The ChannelReference
	topicName := util.TopicName(channelReference)
	logger := logger.With(zap.String("Topic", topicName))

	// Create A Kafka Message From The CloudEvent For The Appropriate Topic
	kafkaMessage, err := message.CreateKafkaMessage(logger, event, topicName)
	if err != nil {
		logger.Error("Failed To Create Kafka Message", zap.Error(err))
		return err
	}

	// Create A DeliveryReport Channel (Producer will report results for message here)
	deliveryReportChannel := make(chan kafka.Event)

	// Produce The Kafka Message To The Kafka Topic
	logger.Debug("Producing Kafka Message", zap.Any("Headers", kafkaMessage.Headers), zap.Any("Message", kafkaMessage.Value))
	err = kafkaProducer.Produce(kafkaMessage, deliveryReportChannel)
	if err != nil {
		logger.Error("Failed To Produce Kafka Message", zap.Error(err))
		return err
	}

	// Block On The DeliveryReport Channel For The Prior Message & Return Any m.TopicPartition.Errors
	select {
	case msg := <-deliveryReportChannel:
		close(deliveryReportChannel) // Close the DeliveryReport channel for safety
		switch ev := msg.(type) {
		case *kafka.Message:
			m := ev
			if m.TopicPartition.Error != nil {
				logger.Error("Delivery failed", zap.Error(m.TopicPartition.Error))
			} else {
				logger.Debug("Delivered message to kafka",
					zap.String("topic", *m.TopicPartition.Topic),
					zap.Int32("partition", m.TopicPartition.Partition),
					zap.String("offset", m.TopicPartition.Offset.String()))
			}
			return m.TopicPartition.Error
		case kafka.Error:
			logger.Warn("Kafka error", zap.Error(ev))
			return errors.New("kafka error occurred")
		default:
			logger.Info("Ignored event", zap.String("event", ev.String()))
			return errors.New("kafka ignored the event")
		}
	}
}

// Close The Producer (Stop Processing)
func Close() {

	// Stop Processing Success/Error Messages From Producer
	logger.Info("Stopping Kafka Producer Success/Error Processing")
	close(stopChannel)
	<-stoppedChannel // Block On Stop Completion

	// Close The Kafka Producer
	logger.Info("Closing Kafka Producer")
	kafkaProducer.Close()
}

// Infinite Loop For Processing Kafka Producer Events
func processProducerEvents(healthServer *health.Server) {
	for {
		select {
		case msg := <-kafkaProducer.Events():
			switch ev := msg.(type) {
			case *kafka.Message:
				logger.Warn("Kafka Message Arrived On The Wrong Channel", zap.Any("Message", msg))
			case kafka.Error:
				logger.Warn("Kafka Error", zap.Error(ev))
			case *kafka.Stats:
				// Update Kafka Prometheus Metrics
				metrics.Observe(ev.String())
			default:
				logger.Info("Ignored Event", zap.String("Event", ev.String()))
			}

		case <-stopChannel:
			logger.Info("Terminating Producer Event Processing")
			healthServer.SetProducerReady(false)
			close(stoppedChannel) // Inform On Stop Completion & Return
			return
		}
	}
}
