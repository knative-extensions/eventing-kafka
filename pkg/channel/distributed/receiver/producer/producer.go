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
	kafkasarama "knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/sarama"
	"knative.dev/eventing-kafka/pkg/channel/distributed/common/metrics"
	"knative.dev/eventing-kafka/pkg/channel/distributed/receiver/constants"
	"knative.dev/eventing-kafka/pkg/channel/distributed/receiver/health"
	"knative.dev/eventing-kafka/pkg/channel/distributed/receiver/util"
	"knative.dev/eventing-kafka/pkg/common/client"
	commonconstants "knative.dev/eventing-kafka/pkg/common/constants"
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
		metricsRegistry:    config.MetricRegistry,
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
		// Checked Logging Level First To Avoid Calling zap.Any In Production
		logger.Debug("Producing Kafka Message", zap.Any("Headers", producerMessage.Headers), zap.Any("Message", producerMessage.Value))
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

// ConfigChanged is called by the configMapObserver handler function in main() so that
// settings specific to the producer may be extracted and the producer restarted if necessary.
// The new configmap could technically have changes to the eventing-kafka section as well as the sarama
// section, but none of those matter to a currently-running Producer, so those are ignored here
// (which avoids the necessity of calling env.GetEnvironment).  If those settings
// are needed in the future, the environment will also need to be re-parsed here.
// If there aren't any producer-specific differences between the current config and the new one,
// then just log that and move on; do not restart the Producer unnecessarily.
func (p *Producer) ConfigChanged(ctx context.Context, configMap *corev1.ConfigMap) *Producer {

	// Debug Log The ConfigMap Change
	p.logger.Debug("New ConfigMap Received", zap.String("configMap.Name", configMap.ObjectMeta.Name))

	// Validate The ConfigMap Data
	if configMap.Data == nil {
		p.logger.Error("Attempted to merge sarama settings with empty configmap")
		return nil
	}

	// Merge The ConfigMap Settings Into The Provided Config
	saramaSettingsYamlString := configMap.Data[commonconstants.SaramaSettingsConfigKey]

	// Merge The Sarama Config From ConfigMap Into New Sarama Config
	configBuilder := client.NewConfigBuilder().
		WithDefaults().
		FromYaml(saramaSettingsYamlString)

	if p.configuration != nil {
		configBuilder = configBuilder.WithClientId(p.configuration.ClientID)

		// Some of the current config settings may not be overridden by the configmap (username, password, etc.)
		if p.configuration.Net.SASL.User != "" {
			kafkaAuthCfg := &client.KafkaAuthConfig{
				SASL: &client.KafkaSaslConfig{
					User:     p.configuration.Net.SASL.User,
					Password: p.configuration.Net.SASL.Password,
					SaslType: string(p.configuration.Net.SASL.Mechanism),
				},
			}
			configBuilder = configBuilder.WithAuth(kafkaAuthCfg)
		}
	}

	newConfig, err := configBuilder.Build(ctx)
	if err != nil {
		p.logger.Error("Unable to merge sarama settings", zap.Error(err))
		return nil
	}

	// Validate Configuration (Should Always Be Present)
	if p.configuration != nil {
		// Enable Sarama Logging If Specified In ConfigMap
		if ekConfig, err := kafkasarama.LoadEventingKafkaSettings(configMap); err == nil && ekConfig != nil {
			kafkasarama.EnableSaramaLogging(ekConfig.Kafka.EnableSaramaLogging)
			p.logger.Debug("Updated Sarama logging", zap.Bool("Kafka.EnableSaramaLogging", ekConfig.Kafka.EnableSaramaLogging))
		} else {
			p.logger.Error("Could Not Extract Eventing-Kafka Setting From Updated ConfigMap", zap.Error(err))
		}

		// Ignore the "Admin" and "Consumer" sections when comparing, as changes to those do not require restarting the Producer
		if client.ConfigEqual(newConfig, p.configuration, newConfig.Admin, newConfig.Consumer) {
			p.logger.Info("No Producer Changes Detected In New Configuration - Ignoring")
			return nil
		}
	}

	// Create A New Producer With The New Configuration (Reusing All Other Existing Config)
	p.logger.Info("Producer Changes Detected In New Configuration - Closing & Recreating Producer")
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
