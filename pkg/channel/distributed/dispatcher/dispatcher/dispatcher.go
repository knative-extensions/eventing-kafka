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

package dispatcher

import (
	"context"
	"strings"
	"sync"
	"time"

	"knative.dev/eventing-kafka/pkg/common/client"
	"knative.dev/eventing-kafka/pkg/common/controlprotocol"

	"github.com/Shopify/sarama"
	gometrics "github.com/rcrowley/go-metrics"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	eventingduck "knative.dev/eventing/pkg/apis/duck/v1"
	"knative.dev/eventing/pkg/channel"

	commonkafkautil "knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/util"
	dispatcherconstants "knative.dev/eventing-kafka/pkg/channel/distributed/dispatcher/constants"
	commonconfig "knative.dev/eventing-kafka/pkg/common/config"
	commonconsumer "knative.dev/eventing-kafka/pkg/common/consumer"
	"knative.dev/eventing-kafka/pkg/common/metrics"
)

// DispatcherConfig Defines A Dispatcher Config Struct To Hold Configuration
type DispatcherConfig struct {
	Logger          *zap.Logger
	ClientId        string
	Brokers         []string
	Topic           string
	ChannelKey      string
	StatsReporter   metrics.StatsReporter
	MetricsRegistry gometrics.Registry
	SaramaConfig    *sarama.Config
	SubscriberSpecs []eventingduck.SubscriberSpec
}

// SubscriberWrapper Defines A Knative Eventing SubscriberSpec Wrapper Enhanced With Sarama ConsumerGroup ID
type SubscriberWrapper struct {
	eventingduck.SubscriberSpec
	GroupId string
}

// NewSubscriberWrapper Is The SubscriberWrapper Constructor
func NewSubscriberWrapper(subscriberSpec eventingduck.SubscriberSpec, groupId string) *SubscriberWrapper {
	return &SubscriberWrapper{subscriberSpec, groupId}
}

// Dispatcher Interface
type Dispatcher interface {
	SecretChanged(ctx context.Context, secret *corev1.Secret) Dispatcher
	Shutdown()
	UpdateSubscriptions(subscriberSpecs []eventingduck.SubscriberSpec) map[eventingduck.SubscriberSpec]error
}

// DispatcherImpl Is A Struct With Configuration & ConsumerGroup State
type DispatcherImpl struct {
	DispatcherConfig
	subscribers          map[types.UID]*SubscriberWrapper
	consumerGroupFactory commonconsumer.KafkaConsumerGroupFactory
	consumerUpdateLock   sync.Mutex
	messageDispatcher    channel.MessageDispatcher
	MetricsStopChan      chan struct{}
	MetricsStoppedChan   chan struct{}
	consumerMgr          commonconsumer.KafkaConsumerGroupManager
	controlServer        controlprotocol.ServerHandler
}

// Verify The DispatcherImpl Implements The Dispatcher Interface
var _ Dispatcher = &DispatcherImpl{}

// NewDispatcher Is The Dispatcher Constructor
func NewDispatcher(dispatcherConfig DispatcherConfig, controlServer controlprotocol.ServerHandler) Dispatcher {

	consumerGroupFactory := commonconsumer.NewConsumerGroupFactory(dispatcherConfig.Brokers, dispatcherConfig.SaramaConfig)
	consumerGroupManager := commonconsumer.NewConsumerGroupManager(controlServer, consumerGroupFactory)

	// Create The DispatcherImpl With Specified Configuration
	dispatcher := &DispatcherImpl{
		DispatcherConfig:     dispatcherConfig,
		subscribers:          make(map[types.UID]*SubscriberWrapper),
		consumerGroupFactory: consumerGroupFactory,
		messageDispatcher:    channel.NewMessageDispatcher(dispatcherConfig.Logger),
		MetricsStopChan:      make(chan struct{}),
		MetricsStoppedChan:   make(chan struct{}),
		controlServer:        controlServer,
		consumerMgr:          consumerGroupManager,
	}

	// Start Observing Metrics
	dispatcher.ObserveMetrics(dispatcherconstants.MetricsInterval)

	// Return The DispatcherImpl
	return dispatcher
}

// Shutdown The Dispatcher
func (d *DispatcherImpl) Shutdown() {

	// Stop Observing Metrics
	if d.MetricsStopChan != nil {
		close(d.MetricsStopChan)
		<-d.MetricsStoppedChan
	}

	// Close ConsumerGroups Of All Subscriptions
	for _, subscriber := range d.subscribers {
		d.closeConsumerGroup(subscriber)
	}
}

// UpdateSubscriptions manages the Dispatcher's Subscriptions to align with new state
func (d *DispatcherImpl) UpdateSubscriptions(subscriberSpecs []eventingduck.SubscriberSpec) map[eventingduck.SubscriberSpec]error {

	if d.SaramaConfig == nil {
		d.Logger.Error("Dispatcher has no config!")
		return nil
	}

	// Maps For Tracking Subscriber State
	activeSubscriptions := make(map[types.UID]bool)
	failedSubscriptions := make(map[eventingduck.SubscriberSpec]error)

	// Thread Safe ;)
	d.consumerUpdateLock.Lock()
	defer d.consumerUpdateLock.Unlock()

	// Loop Over All All The Specified Subscribers
	for _, subscriberSpec := range subscriberSpecs {

		// If The Subscriber Wrapper For The SubscriberSpec Does Not Exist Then Create One
		if _, ok := d.subscribers[subscriberSpec.UID]; !ok {

			// Format The GroupId For The Specified Subscriber
			groupId := commonkafkautil.GroupId(string(subscriberSpec.UID))

			// Create A ConsumerGroup Logger
			logger := d.Logger.With(zap.String("GroupId", groupId))

			// Create/Start A New ConsumerGroup With Custom Handler
			handler := NewHandler(logger, groupId, &subscriberSpec)
			err := d.consumerMgr.StartConsumerGroup(groupId, []string{d.Topic}, d.Logger.Sugar(), handler)
			if err != nil {

				// Log & Return Failure
				logger.Error("Failed To Create ConsumerGroup", zap.Error(err))
				failedSubscriptions[subscriberSpec] = err

			} else {

				// Create A New SubscriberWrapper With The ConsumerGroup
				subscriber := NewSubscriberWrapper(subscriberSpec, groupId)

				// Asynchronously Process ConsumerGroup's Error Channel
				go func() {
					logger.Info("ConsumerGroup Error Processing Initiated")
					for err := range d.consumerMgr.Errors(subscriber.GroupId) { // Closing ConsumerGroup Will Break Out Of This
						logger.Error("ConsumerGroup Error", zap.Error(err))
					}
					logger.Info("ConsumerGroup Error Processing Terminated")
				}()

				// Track The New SubscriberWrapper For The SubscriberSpec As Active
				d.subscribers[subscriberSpec.UID] = subscriber
				activeSubscriptions[subscriberSpec.UID] = true
			}

		} else {

			// Otherwise Just Add To List Of Active Subscribers
			activeSubscriptions[subscriberSpec.UID] = true
		}
	}

	// Save the current (active) subscriber specs so that SecretChanged() can use them to recreate the Dispatcher
	// if necessary without going through the inactive subscribers again.
	d.SubscriberSpecs = []eventingduck.SubscriberSpec{}

	// Close ConsumerGroups For Removed Subscriptions (In Map But No Longer Active)
	for _, subscriber := range d.subscribers {
		if !activeSubscriptions[subscriber.UID] {
			d.closeConsumerGroup(subscriber)
		} else {
			d.SubscriberSpecs = append(d.SubscriberSpecs, subscriber.SubscriberSpec)
		}
	}

	// Return Any Failed Subscriber Errors
	return failedSubscriptions
}

// closeConsumerGroup closes the ConsumerGroup associated with a single Subscriber
func (d *DispatcherImpl) closeConsumerGroup(subscriber *SubscriberWrapper) {

	// Create Logger With GroupId & Subscriber URI
	logger := d.Logger.With(zap.String("GroupId", subscriber.GroupId), zap.String("URI", subscriber.SubscriberURI.String()))

	// If The ConsumerGroup Is Valid
	if d.consumerMgr.IsValid(subscriber.GroupId) {

		// Close The ConsumerGroup
		err := d.consumerMgr.CloseConsumerGroup(subscriber.GroupId)
		if err != nil {
			// Simply Log ConsumerGroup Close Failures
			//   - Don't include in failedSubscriptions response as that is used to update Subscription Status.
			//   - Don't delete from ConsumerGroups Map to force retry of Close next time around.
			logger.Error("Failed To Close ConsumerGroup", zap.Error(err))
		} else {
			logger.Info("Successfully Closed ConsumerGroup")
			delete(d.subscribers, subscriber.UID)
		}
	} else {
		logger.Warn("Successfully Closed Subscriber With Nil ConsumerGroup")
		delete(d.subscribers, subscriber.UID)
	}
}

// SecretChanged is called by the secretObserver handler function in main() so that
// settings specific to the dispatcher may be extracted and the dispatcher restarted if necessary.
func (d *DispatcherImpl) SecretChanged(ctx context.Context, secret *corev1.Secret) {

	// Debug Log The Secret Change
	d.Logger.Debug("New Secret Received", zap.String("secret.Name", secret.ObjectMeta.Name))

	kafkaAuthCfg := commonconfig.GetAuthConfigFromSecret(secret)
	if kafkaAuthCfg == nil {
		d.Logger.Warn("No auth config found in secret; ignoring update")
		return
	}

	// Don't Restart Dispatcher If All Auth Settings Identical
	if kafkaAuthCfg.SASL.HasSameSettings(d.SaramaConfig) {
		d.Logger.Info("No relevant changes in Secret; ignoring update")
		return
	}

	// Build New Config Using Existing Config And New Auth Settings
	if kafkaAuthCfg.SASL.User == "" {
		// The config builder expects the entire config object to be nil if not using auth
		kafkaAuthCfg = nil
	}
	newConfig, err := client.NewConfigBuilder().WithExisting(d.SaramaConfig).WithAuth(kafkaAuthCfg).Build(ctx)
	if err != nil {
		d.Logger.Error("Unable to merge new auth into sarama settings", zap.Error(err))
		return
	}

	// Create A New Dispatcher With The New Configuration (Reusing All Other Existing Config)
	d.Logger.Info("Changes Detected In New Secret - Closing & Recreating Consumer Groups")
	d.reconfigure(newConfig, nil)
}

// reconfigure shuts down the current dispatcher and recreates it with new settings
func (d *DispatcherImpl) reconfigure(newConfig *sarama.Config, ekConfig *commonconfig.EventingKafkaConfig) {
	d.Shutdown()
	d.DispatcherConfig.SaramaConfig = newConfig
	if ekConfig != nil {
		// Currently the only thing that a new dispatcher might care about in the EventingKafkaConfig is the Brokers
		d.DispatcherConfig.Brokers = strings.Split(ekConfig.Kafka.Brokers, ",")
	}

	// Since we're using the Consumer Group Factory, that's the only thing that
	// needs active reconfiguration, once the desired DispatcherConfig has been set.
	d.consumerGroupFactory = commonconsumer.NewConsumerGroupFactory(d.DispatcherConfig.Brokers, d.DispatcherConfig.SaramaConfig)

	failedSubscriptions := d.UpdateSubscriptions(d.SubscriberSpecs)
	if len(failedSubscriptions) > 0 {
		d.Logger.Fatal("Failed To Subscribe Kafka Subscriptions For New Dispatcher", zap.Int("Count", len(failedSubscriptions)))
	}
}

// ObserveMetrics Is An Async Process For Observing Kafka Metrics
func (d *DispatcherImpl) ObserveMetrics(interval time.Duration) {

	// Fork A New Process To Run Async Metrics Collection
	go func() {

		metricsTimer := time.NewTimer(interval)

		// Infinite Loop For Periodically Observing Sarama Metrics From Registry
		for {

			select {

			case <-d.MetricsStopChan:
				d.Logger.Info("Stopped Metrics Tracking")
				close(d.MetricsStoppedChan)
				return

			case <-metricsTimer.C:
				// Get All The Sarama Metrics From The Producer's Metrics Registry
				kafkaMetrics := d.MetricsRegistry.GetAll()

				// Forward Metrics To Prometheus For Observation
				d.StatsReporter.Report(kafkaMetrics)

				// Schedule Another Report
				metricsTimer.Reset(interval)
			}
		}
	}()
}
