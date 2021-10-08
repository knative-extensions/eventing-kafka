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
	"sync"
	"time"

	"github.com/Shopify/sarama"
	gometrics "github.com/rcrowley/go-metrics"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	eventingduck "knative.dev/eventing/pkg/apis/duck/v1"
	"knative.dev/eventing/pkg/channel"

	commonkafkautil "knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/util"
	dispatcherconstants "knative.dev/eventing-kafka/pkg/channel/distributed/dispatcher/constants"
	"knative.dev/eventing-kafka/pkg/common/client"
	commonconfig "knative.dev/eventing-kafka/pkg/common/config"
	commonconsumer "knative.dev/eventing-kafka/pkg/common/consumer"
	"knative.dev/eventing-kafka/pkg/common/controlprotocol"
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
	SecretChanged(ctx context.Context, secret *corev1.Secret)
	Shutdown()
	UpdateSubscriptions(ctx context.Context, channelRef types.NamespacedName, subscriberSpecs []eventingduck.SubscriberSpec) commonconsumer.SubscriberStatusMap
}

// DispatcherImpl Is A Struct With Configuration & ConsumerGroup State
type DispatcherImpl struct {
	DispatcherConfig
	subscribers        map[types.UID]*SubscriberWrapper
	consumerUpdateLock sync.Mutex
	messageDispatcher  channel.MessageDispatcher
	MetricsStopChan    chan struct{}
	MetricsStoppedChan chan struct{}
	consumerMgr        commonconsumer.KafkaConsumerGroupManager
}

// Verify The DispatcherImpl Implements The Dispatcher Interface
var _ Dispatcher = &DispatcherImpl{}

// NewDispatcher Is The Dispatcher Constructor
func NewDispatcher(dispatcherConfig DispatcherConfig, controlServer controlprotocol.ServerHandler, enqueue func(ref types.NamespacedName)) (Dispatcher, <-chan commonconsumer.ManagerEvent) {

	consumerGroupManager := commonconsumer.NewConsumerGroupManager(dispatcherConfig.Logger, controlServer, dispatcherConfig.Brokers, dispatcherConfig.SaramaConfig, &commonconsumer.NoopConsumerGroupOffsetsChecker{}, enqueue)

	// Create The DispatcherImpl With Specified Configuration
	dispatcher := &DispatcherImpl{
		DispatcherConfig:   dispatcherConfig,
		subscribers:        make(map[types.UID]*SubscriberWrapper),
		messageDispatcher:  channel.NewMessageDispatcher(dispatcherConfig.Logger),
		MetricsStopChan:    make(chan struct{}),
		MetricsStoppedChan: make(chan struct{}),
		consumerMgr:        consumerGroupManager,
	}

	// Start Observing Metrics
	dispatcher.ObserveMetrics(dispatcherconstants.MetricsInterval)

	// Return The DispatcherImpl
	return dispatcher, consumerGroupManager.GetNotificationChannel()
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

	// Close the Consumer Group Manager notification channels
	d.consumerMgr.ClearNotifications()
}

// UpdateSubscriptions manages the Dispatcher's Subscriptions to align with new state
func (d *DispatcherImpl) UpdateSubscriptions(ctx context.Context, channelRef types.NamespacedName, subscriberSpecs []eventingduck.SubscriberSpec) commonconsumer.SubscriberStatusMap {

	if d.SaramaConfig == nil {
		d.Logger.Error("Dispatcher has no config!")
		return nil
	}

	// Maps For Tracking Subscriber State
	subscriptions := make(commonconsumer.SubscriberStatusMap)

	// Thread Safe ;)
	d.consumerUpdateLock.Lock()
	defer d.consumerUpdateLock.Unlock()

	// Loop Over All The Specified Subscribers
	for _, subscriberSpec := range subscriberSpecs {

		// Format The GroupId For The Specified Subscriber
		groupId := commonkafkautil.GroupId(string(subscriberSpec.UID))

		// If The Subscriber Wrapper For The SubscriberSpec Does Not Exist Then Create One
		if _, ok := d.subscribers[subscriberSpec.UID]; !ok {

			// Create A ConsumerGroup Logger
			logger := d.Logger.With(zap.String("GroupId", groupId))

			// Create/Start A New ConsumerGroup With Custom Handler
			handler := NewHandler(logger, groupId, &subscriberSpec)
			err := d.consumerMgr.StartConsumerGroup(ctx, groupId, []string{d.Topic}, handler, channelRef)
			if err != nil {

				// Log & Return Failure
				logger.Error("Failed To Create ConsumerGroup", zap.Error(err))
				subscriptions[subscriberSpec.UID] = commonconsumer.SubscriberStatus{Error: err}

			} else {

				// Create A New SubscriberWrapper With The ConsumerGroup
				subscriber := NewSubscriberWrapper(subscriberSpec, groupId)

				// Asynchronously Process ConsumerGroup's Error Channel
				go func() {
					logger.Info("ConsumerGroup Error Processing Initiated")
					for groupErr := range d.consumerMgr.Errors(subscriber.GroupId) { // Closing ConsumerGroup Will Break Out Of This
						logger.Error("ConsumerGroup Error", zap.Error(groupErr))
					}
					logger.Info("ConsumerGroup Error Processing Terminated")
				}()

				// Track The New SubscriberWrapper For The SubscriberSpec As Active
				d.subscribers[subscriberSpec.UID] = subscriber
				subscriptions[subscriberSpec.UID] = commonconsumer.SubscriberStatus{}
			}

		} else {

			// Otherwise, Just Add To List Of Active Subscribers
			subscriptions[subscriberSpec.UID] = commonconsumer.SubscriberStatus{}

			// If the group is stopped, it's still active but the reconciler needs to know about it in order
			// to not treat it as a failure (which would re-create the group, effectively un-stopping it)
			if d.consumerMgr.IsStopped(groupId) {
				d.Logger.Debug("Adding Stopped ConsumerGroup To Stopped Map", zap.String("GroupId", groupId))
				subscriptions[subscriberSpec.UID] = commonconsumer.SubscriberStatus{Stopped: true}
			}
		}
	}

	// Close ConsumerGroups For Removed/Failed Subscriptions (In Map But No Longer Active)
	for _, subscriber := range d.subscribers {
		subscription, ok := subscriptions[subscriber.UID]
		if !ok || subscription.Error != nil {
			d.closeConsumerGroup(subscriber)
		}
	}

	// Return Any Failed Subscriber Errors
	return subscriptions
}

// closeConsumerGroup closes the ConsumerGroup associated with a single Subscriber
func (d *DispatcherImpl) closeConsumerGroup(subscriber *SubscriberWrapper) {

	// Create Logger With GroupId & Subscriber URI
	logger := d.Logger.With(zap.String("GroupId", subscriber.GroupId), zap.String("URI", subscriber.SubscriberURI.String()))

	// If The ConsumerGroup Is Valid
	if d.consumerMgr.IsManaged(subscriber.GroupId) {

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
// settings specific to the dispatcher may be changed if necessary
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
		// Any existing SASL config must be cleared explicitly or the "WithExisting" builder will keep the old values
		d.SaramaConfig.Net.SASL.Enable = false
		d.SaramaConfig.Net.SASL.User = ""
		d.SaramaConfig.Net.SASL.Password = ""
	}
	newConfig, err := client.NewConfigBuilder().WithExisting(d.SaramaConfig).WithAuth(kafkaAuthCfg).Build(ctx)
	if err != nil {
		d.Logger.Error("Unable to merge new auth into sarama settings", zap.Error(err))
		return
	}

	// The only values in the secret that matter here are the username, password, and SASL type, which are all part
	// of the SaramaConfig, so that's all that needs to be modified
	d.DispatcherConfig.SaramaConfig = newConfig

	// Replace The Dispatcher's ConsumerGroupFactory With Updated Version Using New Config
	// Note:  This will close and recreate all managed ConsumerGroups
	reconfigureErr := d.consumerMgr.Reconfigure(d.DispatcherConfig.Brokers, d.DispatcherConfig.SaramaConfig)
	if reconfigureErr != nil {

		// Remove All Failed Subscribers From List To Allow Recreation Next Reconcile Loop (Expects Caller To Requeue KafkaChannel!)
		d.Logger.Error("Failed To Reconfigure Consumer Group Manager Using Updated Secret", zap.Error(reconfigureErr))
		for _, groupId := range reconfigureErr.GroupIds {
			subscriberUID := commonkafkautil.Uid(groupId)
			delete(d.subscribers, subscriberUID)
		}
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
