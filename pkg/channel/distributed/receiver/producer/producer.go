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

package producer

import (
	"context"
	"errors"
	"time"

	"github.com/Shopify/sarama"
	kafkasaramaprotocol "github.com/cloudevents/sdk-go/protocol/kafka_sarama/v2"
	"github.com/cloudevents/sdk-go/v2/binding"
	gometrics "github.com/rcrowley/go-metrics"
	"go.opencensus.io/trace"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/producer"
	"knative.dev/eventing-kafka/pkg/channel/distributed/receiver/constants"
	"knative.dev/eventing-kafka/pkg/channel/distributed/receiver/health"
	"knative.dev/eventing-kafka/pkg/channel/distributed/receiver/util"
	"knative.dev/eventing-kafka/pkg/common/client"
	commonconfig "knative.dev/eventing-kafka/pkg/common/config"
	kafkasarama "knative.dev/eventing-kafka/pkg/common/kafka/sarama"
	"knative.dev/eventing-kafka/pkg/common/metrics"
	"knative.dev/eventing-kafka/pkg/common/tracing"
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
	logger.Info("Creating Kafka SyncProducer")
	kafkaProducer, err := producer.CreateSyncProducer(brokers, config)
	if err != nil {
		logger.Error("Failed To Create Kafka SyncProducer - Exiting", zap.Error(err), zap.Any("brokers", brokers))
		return nil, err
	} else {
		logger.Info("Successfully Created Kafka SyncProducer")
	}

	// Create A New Producer
	newProducer := &Producer{
		logger:             logger,
		kafkaProducer:      kafkaProducer,
		healthServer:       healthServer,
		statsReporter:      statsReporter,
		metricsRegistry:    config.MetricRegistry,
		metricsStopChan:    make(chan struct{}),
		metricsStoppedChan: make(chan struct{}),
		configuration:      config,
		brokers:            brokers,
	}

	// Start Observing Metrics
	newProducer.ObserveMetrics(constants.MetricsInterval)

	// Mark The Producer As Ready
	healthServer.SetProducerReady(true)

	// Return The New Producer
	logger.Info("Successfully Started Kafka Producer")
	return newProducer, nil
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

	// Add The "traceparent" And "tracestate" Headers To The Message (Helps Tie Related Messages Together In Traces)
	producerMessage.Headers = append(producerMessage.Headers, tracing.SerializeTrace(trace.FromContext(ctx).SpanContext())...)

	// Produce The Kafka Message To The Kafka Topic
	if logger.Core().Enabled(zap.DebugLevel) {
		// Checked Logging Level First To Avoid Calling StringifyHeaders and Encode Functions In Production
		// There isn't actually any way for the sarama ByteEncoder to return an error and this is debug logging, so ignore the return value
		msgBytes, _ := producerMessage.Value.Encode()
		logger.Debug("Producing Kafka Message",
			zap.Any("Headers", kafkasarama.StringifyHeaders(producerMessage.Headers)), // Log human-readable strings, not base64
			zap.ByteString("Message", msgBytes))
	}
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

		metricsTimer := time.NewTimer(interval)

		// Infinite Loop For Periodically Observing Sarama Metrics From Registry
		for {

			select {

			case <-p.metricsStopChan:
				p.logger.Info("Stopped Metrics Tracking")
				close(p.metricsStoppedChan)
				return

			case <-metricsTimer.C:
				// Get All The Sarama Metrics From The Producer's Metrics Registry
				kafkaMetrics := p.metricsRegistry.GetAll()

				// Forward Metrics To Prometheus For Observation
				p.statsReporter.Report(kafkaMetrics)

				// Schedule Another Report
				metricsTimer.Reset(interval)
			}
		}
	}()
}

// SecretChanged is called by the secretObserver handler function in main() so that
// settings specific to the producer may be extracted and the producer restarted if necessary.
func (p *Producer) SecretChanged(ctx context.Context, secret *corev1.Secret) *Producer {

	// Debug Log The Secret Change
	p.logger.Debug("New Secret Received", zap.String("secret.Name", secret.ObjectMeta.Name))

	kafkaAuthCfg := commonconfig.GetAuthConfigFromSecret(secret)
	if kafkaAuthCfg == nil {
		p.logger.Warn("No auth config found in secret; ignoring update")
		return nil
	}

	// Don't Restart Producer If All Auth Settings Identical.
	if kafkaAuthCfg.SASL.HasSameSettings(p.configuration) {
		p.logger.Info("No relevant changes in Secret; ignoring update")
		return nil
	}

	// Build New Config Using Existing Config And New Auth Settings
	if kafkaAuthCfg.SASL.User == "" {
		// The config builder expects the entire config object to be nil if not using auth
		kafkaAuthCfg = nil
		// Any existing SASL config must be cleared explicitly or the "WithExisting" builder will keep the old values
		p.configuration.Net.SASL.Enable = false
		p.configuration.Net.SASL.User = ""
		p.configuration.Net.SASL.Password = ""
	}
	newConfig, err := client.NewConfigBuilder().WithExisting(p.configuration).WithAuth(kafkaAuthCfg).Build(ctx)
	if err != nil {
		p.logger.Error("Unable to merge new auth into sarama settings", zap.Error(err))
		return nil
	}

	// Create A New Producer With The New Configuration (Reusing All Other Existing Config)
	p.logger.Info("Changes Detected In New Secret - Closing & Recreating Producer")

	// Shut down the current producer and recreate it with new settings
	p.Close()
	reconfiguredKafkaProducer, err := NewProducer(p.logger, newConfig, p.brokers, p.statsReporter, p.healthServer)
	if err != nil {
		p.logger.Fatal("Failed To Create Kafka Producer With New Configuration", zap.Error(err))
		return nil
	}

	// Successfully Created New Producer - Close Old One And Return New One
	p.logger.Info("Successfully Created New Producer")
	return reconfiguredKafkaProducer
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
