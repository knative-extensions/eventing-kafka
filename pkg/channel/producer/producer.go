package producer

import (
	"errors"
	"github.com/cloudevents/sdk-go/v1/cloudevents"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"go.uber.org/zap"
	"knative.dev/eventing-kafka/pkg/channel/health"
	"knative.dev/eventing-kafka/pkg/channel/message"
	"knative.dev/eventing-kafka/pkg/channel/util"
	kafkaproducer "knative.dev/eventing-kafka/pkg/common/kafka/producer"
	"knative.dev/eventing-kafka/pkg/common/prometheus"
	eventingChannel "knative.dev/eventing/pkg/channel"
)

// Producer Struct
type Producer struct {
	logger         *zap.Logger
	kafkaProducer  kafkaproducer.ProducerInterface
	stopChannel    chan struct{}
	stoppedChannel chan struct{}
	metrics        *prometheus.MetricsServer
}

// Initialize The Producer
func NewProducer(logger *zap.Logger, brokers string, username string, password string, metricsServer *prometheus.MetricsServer, healthServer *health.Server) (*Producer, error) {

	// Create The Producer Using The Specified Kafka Authentication
	kafkaProducer, err := createProducerFunctionWrapper(brokers, username, password)
	if err != nil {
		logger.Error("Failed To Create Kafka Producer - Exiting", zap.Error(err))
		return nil, err
	} else {
		logger.Info("Successfully Created Kafka Producer")
	}

	// Create A New Producer
	producer := &Producer{
		logger:         logger,
		kafkaProducer:  kafkaProducer,
		stopChannel:    make(chan struct{}),
		stoppedChannel: make(chan struct{}),
		metrics:        metricsServer,
	}

	// Mark The Kafka Producer As Ready
	healthServer.SetProducerReady(false)

	// Fork A Go Routine To Process Kafka Events Asynchronously
	logger.Info("Starting Kafka Producer Event Processing")
	go producer.processProducerEvents(healthServer)
	healthServer.SetProducerReady(true)

	// Return The New Producer
	logger.Info("Successfully Started Kafka Producer")
	return producer, nil
}

// Wrapper Around Common Kafka Producer Creation To Facilitate Unit Testing
var createProducerFunctionWrapper = func(brokers string, username string, password string) (kafkaproducer.ProducerInterface, error) {
	return kafkaproducer.CreateProducer(brokers, username, password)
}

// Produce A KafkaMessage From The Specified CloudEvent To The Specified Topic And Wait For The Delivery Report
func (p *Producer) ProduceKafkaMessage(event cloudevents.Event, channelReference eventingChannel.ChannelReference) error {

	// Validate The Kafka Producer (Must Be Pre-Initialized)
	if p.kafkaProducer == nil {
		p.logger.Error("Kafka Producer Not Initialized - Unable To Produce Message")
		return errors.New("uninitialized kafka producer - unable to produce message")
	}

	// Get The Topic Name From The ChannelReference
	topicName := util.TopicName(channelReference)
	logger := p.logger.With(zap.String("Topic", topicName))

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
	err = p.kafkaProducer.Produce(kafkaMessage, deliveryReportChannel)
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
func (p *Producer) Close() {

	// Stop Processing Success/Error Messages From Producer
	p.logger.Info("Stopping Kafka Producer Success/Error Processing")
	close(p.stopChannel)
	<-p.stoppedChannel // Block On Stop Completion

	// Close The Kafka Producer
	p.logger.Info("Closing Kafka Producer")
	p.kafkaProducer.Close()
}

// Infinite Loop For Processing Kafka Producer Events
func (p *Producer) processProducerEvents(healthServer *health.Server) {
	for {
		select {
		case msg := <-p.kafkaProducer.Events():
			switch ev := msg.(type) {
			case *kafka.Message:
				p.logger.Warn("Kafka Message Arrived On The Wrong Channel", zap.Any("Message", msg))
			case kafka.Error:
				p.logger.Warn("Kafka Error", zap.Error(ev))
			case *kafka.Stats:
				// Update Kafka Prometheus Metrics
				p.metrics.Observe(ev.String())
			default:
				p.logger.Info("Ignored Event", zap.String("Event", ev.String()))
			}

		case <-p.stopChannel:
			p.logger.Info("Terminating Producer Event Processing")
			healthServer.SetProducerReady(false)
			close(p.stoppedChannel) // Inform On Stop Completion & Return
			return
		}
	}
}
