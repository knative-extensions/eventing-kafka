package producer

import (
	"context"
	"errors"
	"github.com/Shopify/sarama"
	kafkasaramaprotocol "github.com/cloudevents/sdk-go/protocol/kafka_sarama/v2"
	"github.com/cloudevents/sdk-go/v2/binding"
	"github.com/cloudevents/sdk-go/v2/binding/spec"
	"github.com/cloudevents/sdk-go/v2/binding/transformer"
	"github.com/cloudevents/sdk-go/v2/types"
	gometrics "github.com/rcrowley/go-metrics"
	"go.uber.org/zap"
	"knative.dev/eventing-kafka/pkg/channel/constants"
	"knative.dev/eventing-kafka/pkg/channel/health"
	"knative.dev/eventing-kafka/pkg/channel/util"
	kafkaproducer "knative.dev/eventing-kafka/pkg/common/kafka/producer"
	"knative.dev/eventing-kafka/pkg/common/metrics"
	eventingChannel "knative.dev/eventing/pkg/channel"
	"time"
)

// Producer Struct
type Producer struct {
	logger             *zap.Logger
	kafkaProducer      sarama.SyncProducer
	healthServer       *health.Server
	statsReporter      metrics.StatsReporter
	metricsRegistry    gometrics.Registry
	metricsStopChan    chan struct{}
	metricsStoppedChan chan struct{}
}

// Initialize The Producer
func NewProducer(logger *zap.Logger,
	clientId string,
	brokers []string,
	username string,
	password string,
	statsReporter metrics.StatsReporter,
	healthServer *health.Server) (*Producer, error) {

	// Create The Kafka Producer Using The Specified Kafka Authentication
	kafkaProducer, metricsRegistry, err := createSyncProducerWrapper(clientId, brokers, username, password)
	if err != nil {
		logger.Error("Failed To Create Kafka SyncProducer - Exiting", zap.Error(err), zap.Any("Brokers", brokers))
		return nil, err
	} else {
		logger.Info("Successfully Created Kafka SyncProducer")
	}

	// Create A New Producer
	producer := &Producer{
		logger:             logger,
		kafkaProducer:      kafkaProducer,
		healthServer:       healthServer,
		statsReporter:      statsReporter,
		metricsRegistry:    metricsRegistry,
		metricsStopChan:    make(chan struct{}),
		metricsStoppedChan: make(chan struct{}),
	}

	// Start Observing Metrics
	producer.ObserveMetrics(constants.MetricsInterval)

	// Mark The Producer As Ready
	healthServer.SetProducerReady(true)

	// Return The New Producer
	logger.Info("Successfully Started Kafka Producer")
	return producer, nil
}

// Wrapper Around Common Kafka SyncProducer Creation To Facilitate Unit Testing
var createSyncProducerWrapper = func(clientId string, brokers []string, username string, password string) (sarama.SyncProducer, gometrics.Registry, error) {
	return kafkaproducer.CreateSyncProducer(clientId, brokers, username, password)
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

	// Append Time Transformer To Add "ce_time" Extension If Not Present
	transformers = append(transformers, transformer.AddExtension("time", time.Now()))

	// Append PartitionKey Transformer To Default "partitionkey" Extension To "subject" If Not Present
	transformers = append(transformers, DefaultPartitionKeyTransformer())

	// Initialize The Sarama ProducerMessage With The Specified Topic Name
	producerMessage := &sarama.ProducerMessage{Topic: topicName}

	// Use The SaramaKafka Protocol To Convert The Binding Message To A ProducerMessage
	err := kafkasaramaprotocol.WriteProducerMessage(ctx, message, producerMessage, transformers...)
	if err != nil {
		p.logger.Error("Failed To Convert BindingMessage To Sarama ProducerMessage", zap.Error(err))
		return err
	}

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

// Returns A CloudEvents TransformerFunc For Setting The "partitionkey" Extension To The Message's Subject If Not Already Set
func DefaultPartitionKeyTransformer() binding.TransformerFunc {
	return func(reader binding.MessageMetadataReader, writer binding.MessageMetadataWriter) error {
		partitionKey := reader.GetExtension(constants.ExtensionKeyPartitionKey)
		if types.IsZero(partitionKey) {
			_, subjectValue := reader.GetAttribute(spec.Subject)
			if !types.IsZero(subjectValue) {
				return writer.SetExtension(constants.ExtensionKeyPartitionKey, subjectValue)
			}
		}
		return nil
	}
}

// Async Process For Observing Kafka Metrics
func (p *Producer) ObserveMetrics(interval time.Duration) {

	// Fork A New Process To Run Async Metrics Collection
	go func() {

		// Infinite Loop For Periodically Observing Sarama Metrics From Registry
		for {

			select {

			case <-p.metricsStopChan:
				p.logger.Info("Stopped Metrics Tracking")
				close(p.metricsStoppedChan)
				return

			default:

				// Get All The Sarama Metrics From The Producer's Metrics Registry
				kafkaMetrics := p.metricsRegistry.GetAll()

				// Forward Metrics To Prometheus For Observation
				p.statsReporter.Report(kafkaMetrics)
			}

			// Sleep The Specified Interval Before Collecting Metrics Again
			time.Sleep(interval)
		}
	}()
}

// Close The Producer (Stop Processing)
func (p *Producer) Close() {

	// Mark The Producer As No Longer Ready
	p.healthServer.SetProducerReady(false)

	// Stop Observing Metrics
	close(p.metricsStopChan)
	<-p.metricsStoppedChan

	// Close The Kafka Producer & Log Results
	err := p.kafkaProducer.Close()
	if err != nil {
		p.logger.Error("Failed To Close Kafka Producer", zap.Error(err))
	} else {
		p.logger.Error("Successfully Closed Kafka Producer")
	}
}
