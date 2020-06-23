package producer

import (
	"context"
	"errors"
	"github.com/Shopify/sarama"
	kafkasaramaprotocol "github.com/cloudevents/sdk-go/protocol/kafka_sarama/v2"
	"github.com/cloudevents/sdk-go/v2/binding"
	"go.uber.org/zap"
	"knative.dev/eventing-kafka/pkg/channel/health"
	"knative.dev/eventing-kafka/pkg/channel/util"
	kafkaproducer "knative.dev/eventing-kafka/pkg/common/kafka/producer"
	"knative.dev/eventing-kafka/pkg/common/prometheus"
	eventingChannel "knative.dev/eventing/pkg/channel"
)

// Producer Struct
type Producer struct {
	logger        *zap.Logger
	kafkaProducer sarama.SyncProducer
	healthServer  *health.Server
	metricsServer *prometheus.MetricsServer
}

// Initialize The Producer
func NewProducer(logger *zap.Logger, brokers []string, username string, password string, metricsServer *prometheus.MetricsServer, healthServer *health.Server) (*Producer, error) {

	// Create The Kafka Producer Using The Specified Kafka Authentication
	kafkaProducer, err := createSyncProducerWrapper(brokers, username, password)
	if err != nil {
		logger.Error("Failed To Create Kafka SyncProducer - Exiting", zap.Error(err), zap.Any("Brokers", brokers))
		return nil, err
	} else {
		logger.Info("Successfully Created Kafka SyncProducer")
	}

	// Create A New Producer
	producer := &Producer{
		logger:        logger,
		kafkaProducer: kafkaProducer,
		healthServer:  healthServer,
		metricsServer: metricsServer,
	}

	// Mark The Producer As Ready
	healthServer.SetProducerReady(true)

	// Return The New Producer
	logger.Info("Successfully Started Kafka Producer")
	return producer, nil
}

// Wrapper Around Common Kafka SyncProducer Creation To Facilitate Unit Testing
var createSyncProducerWrapper = func(brokers []string, username string, password string) (sarama.SyncProducer, error) {
	return kafkaproducer.CreateSyncProducer(brokers, username, password)
}

// Produce A KafkaMessage From The Specified CloudEvent To The Specified Topic And Wait For The Delivery Report
func (p *Producer) ProduceKafkaMessage(ctx context.Context, channelReference eventingChannel.ChannelReference, message binding.Message, transformers ...binding.Transformer) error {

	// Validate The Kafka Producer (Must Be Pre-Initialized)
	if p.kafkaProducer == nil {
		p.logger.Error("Kafka Producer Not Initialized - Unable To Produce Message")
		return errors.New("uninitialized kafka producer - unable to produce message")
	}

	// Get The Topic Name From The ChannelReference
	topicName := util.TopicName(channelReference)
	logger := p.logger.With(zap.String("Topic", topicName))

	// Initialize The Sarama ProducerMessage With The Specified Topic Name
	producerMessage := &sarama.ProducerMessage{Topic: topicName}

	// TODO - Previously we added the "ce_time" header to the KafkaMessage with context timestamp - kafka_sarama protocol doesn't do this for ProducerMessage - not sure if that's important?

	// Use The SaramaKafka Protocol To Convert The Binding Message To A ProducerMessage
	err := kafkasaramaprotocol.WriteProducerMessage(ctx, message, producerMessage, transformers...)

	// Produce The Kafka Message To The Kafka Topic
	logger.Debug("Producing Kafka Message", zap.Any("Headers", producerMessage.Headers), zap.Any("Message", producerMessage.Value))
	partition, offset, err := p.kafkaProducer.SendMessage(producerMessage)
	if err != nil {
		logger.Error("Failed To Send Message To Kafka", zap.Error(err))
		return err
	} else {
		logger.Debug("Successfully Sent Message To Kafka", zap.Int32("Partition", partition), zap.Int64("Offset", offset))
		return nil
	}
}

// Close The Producer (Stop Processing)
func (p *Producer) Close() {

	// Mark The Producer As No Longer Ready
	p.healthServer.SetProducerReady(false)

	// Close The Kafka Producer & Log Results
	err := p.kafkaProducer.Close()
	if err != nil {
		p.logger.Error("Failed To Close Kafka Producer", zap.Error(err))
	} else {
		p.logger.Error("Successfully Closed Kafka Producer")
	}
}

// TODO - WHERE / HOW TO HANDLE METRICS ?
//      - the whole usage of confluent stats ??? - this is the rcrowley/go-metricsServer
//      - https://github.com/Shopify/sarama/issues/1260
//      - https://godoc.org/github.com/Shopify/sarama#example-Config--Metrics
//      - https://github.com/Shopify/sarama/issues/683
//      - https://github.com/Shopify/sarama/issues/915
//  case *kafka.Stats:
//     Update Kafka Prometheus Metrics
//     p.metricsServer.Observe(ev.String())
//
// ORIGINAL CONFLUENT PRODUCER EVENT PROCESSING LOGIC
//// Infinite Loop For Processing Kafka Producer Events
//func (p *Producer) processProducerEvents(healthServer *health.Server) {
//	for {
//		select {
//		case msg := <-p.kafkaProducer.Events():
//			switch ev := msg.(type) {
//			case *kafka.Message:
//				p.logger.Warn("Kafka Message Arrived On The Wrong Channel", zap.Any("Message", msg))
//			case kafka.Error:
//				p.logger.Warn("Kafka Error", zap.Error(ev))
//			case *kafka.Stats:
//				// Update Kafka Prometheus Metrics
//				p.metricsServer.Observe(ev.String())
//			default:
//				p.logger.Info("Ignored Event", zap.String("Event", ev.String()))
//			}
//
//		case <-p.stopChannel:
//			p.logger.Info("Terminating Producer Event Processing")
//			healthServer.SetProducerReady(false)
//			close(p.stoppedChannel) // Inform On Stop Completion & Return
//			return
//		}
//	}
//}
