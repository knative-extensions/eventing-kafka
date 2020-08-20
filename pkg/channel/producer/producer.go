package producer

import (
	"context"
	"errors"
	"time"

	"github.com/Shopify/sarama"
	kafkasaramaprotocol "github.com/cloudevents/sdk-go/protocol/kafka_sarama/v2"
	"github.com/cloudevents/sdk-go/v2/binding"
	gometrics "github.com/rcrowley/go-metrics"
	"go.uber.org/zap"
	v1 "k8s.io/api/core/v1"
	"knative.dev/eventing-kafka/pkg/channel/constants"
	"knative.dev/eventing-kafka/pkg/channel/health"
	"knative.dev/eventing-kafka/pkg/channel/util"
	commonconfig "knative.dev/eventing-kafka/pkg/common/config"
	kafkaproducer "knative.dev/eventing-kafka/pkg/common/kafka/producer"
	commonutil "knative.dev/eventing-kafka/pkg/common/kafka/util"
	"knative.dev/eventing-kafka/pkg/common/metrics"
	eventingChannel "knative.dev/eventing/pkg/channel"
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
	configuration      *sarama.Config
	brokers            []string
}

// Initialize The Producer
func NewProducer(logger *zap.Logger,
	config *sarama.Config,
	brokers []string,
	statsReporter metrics.StatsReporter,
	healthServer *health.Server) (*Producer, error) {

	// Create The Kafka Producer Using The Specified Kafka Authentication
	kafkaProducer, metricsRegistry, err := createSyncProducerWrapper(config, brokers)
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
		configuration:      config,
		brokers:            brokers,
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
var createSyncProducerWrapper = func(config *sarama.Config, brokers []string) (sarama.SyncProducer, gometrics.Registry, error) {
	return kafkaproducer.CreateSyncProducer(brokers, config)
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

			case <-time.After(interval):
				// Get All The Sarama Metrics From The Producer's Metrics Registry
				kafkaMetrics := p.metricsRegistry.GetAll()

				// Forward Metrics To Prometheus For Observation
				p.statsReporter.Report(kafkaMetrics)
			}
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
		p.logger.Info("Successfully Closed Kafka Producer")
	}
}

// ConfigChanged is called by the configMapObserver handler function in main() so that
// settings specific to the producer may be extracted and the producer restarted if necessary.
// The new configmap could technically have changes to the eventing-kafka section as well as the sarama
// section, but none of those matter to a currently-running Producer, so those are ignored here
// (which avoids the necessity of calling env.GetEnvironment and env.VerifyOverrides).  If those settings
// are needed in the future, the environment will also need to be re-parsed here.
// If there aren't any producer-specific differences between the current config and the new one,
// then just log that and move on; do not restart the Producer unnecessarily.
func (p *Producer) ConfigChanged(configMap *v1.ConfigMap) *Producer {
	p.logger.Debug("New ConfigMap Received", zap.String("configMap.Name", configMap.ObjectMeta.Name))

	newConfig := commonutil.NewSaramaConfig()

	// In order to compare configs "without the admin or consumer sections" we use a known-base config
	// and ensure that those sections are always the same.  The reason we can't just use, for example,
	// sarama.Config{}.Admin as the empty struct is that the Sarama calls to create objects like SyncProducer
	// still verify that certain fields (such as Admin.Timeout) are nonzero and fail otherwise.
	defaultAdmin := newConfig.Admin
	defaultConsumer := newConfig.Consumer

	err := commonconfig.MergeSaramaSettings(newConfig, configMap)
	if err != nil {
		p.logger.Error("Unable to merge sarama settings", zap.Error(err))
		return nil
	}

	// Don't care about Admin or Consumer sections; everything else is a change that needs to be implemented.
	newConfig.Admin = defaultAdmin
	newConfig.Consumer = defaultConsumer

	if p.configuration != nil {
		// Some of the current config settings may not be overridden by the configmap (username, password, etc.)
		commonutil.UpdateSaramaConfig(newConfig, p.configuration.ClientID, p.configuration.Net.SASL.User, p.configuration.Net.SASL.Password)

		// Create a shallow copy of the current config so that we can empty out the Admin and Consumer before comparing.
		configCopy := p.configuration

		// The current config should theoretically have these sections zeroed already because Reconfigure should have been passed
		// a newConfig with the structs empty, but this is more explicit as to what our goal is and doesn't hurt.
		configCopy.Admin = defaultAdmin
		configCopy.Consumer = defaultConsumer
		if commonconfig.SaramaConfigEqual(newConfig, configCopy) {
			p.logger.Info("No Producer changes detected in new config; ignoring")
			return nil
		}
	}

	p.logger.Info("Configuration received; applying new Producer settings")

	// "Reconfiguring" the Producer involves creating a new one, but we can re-use some
	// of the original components (logger, brokers, statsReporter, and healthServer don't change when reconfiguring)
	p.Close()
	reconfiguredKafkaProducer, err := NewProducer(p.logger, newConfig, p.brokers, p.statsReporter, p.healthServer)
	if err != nil {
		p.logger.Fatal("Failed To Reconfigure Kafka Producer", zap.Error(err))
		return nil
	}
	return reconfiguredKafkaProducer
}
