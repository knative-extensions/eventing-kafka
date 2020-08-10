package dispatcher

import (
	"context"
	"fmt"
	"sync"

	"github.com/Shopify/sarama"
	"go.uber.org/zap"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	commonconfig "knative.dev/eventing-kafka/pkg/common/config"
	"knative.dev/eventing-kafka/pkg/common/kafka/consumer"
	kafkaconsumer "knative.dev/eventing-kafka/pkg/common/kafka/consumer"
	"knative.dev/eventing-kafka/pkg/common/metrics"
	eventingduck "knative.dev/eventing/pkg/apis/duck/v1"
	"knative.dev/eventing/pkg/channel"
)

// Define A Dispatcher Config Struct To Hold Configuration
type DispatcherConfig struct {
	Logger                 *zap.Logger
	ClientId               string
	Brokers                []string
	Topic                  string
	Username               string
	Password               string
	ChannelKey             string
	StatsReporter          metrics.StatsReporter
	ExponentialBackoff     bool
	InitialRetryInterval   int64
	MaxRetryTime           int64
	currentConfig          *sarama.Config
	currentSubscriberSpecs []eventingduck.SubscriberSpec
}

// Knative Eventing SubscriberSpec Wrapper Enhanced With Sarama ConsumerGroup
type SubscriberWrapper struct {
	eventingduck.SubscriberSpec
	GroupId       string
	ConsumerGroup sarama.ConsumerGroup
	StopChan      chan struct{}
}

const (
	ConfigChangedLogMsg    = "Configuration received; applying new Consumer settings"
	ConfigNotChangedLogMsg = "No Consumer changes detected in new config; ignoring"
)

// SubscriberWrapper Constructor
func NewSubscriberWrapper(subscriberSpec eventingduck.SubscriberSpec, groupId string, consumerGroup sarama.ConsumerGroup) *SubscriberWrapper {
	return &SubscriberWrapper{subscriberSpec, groupId, consumerGroup, make(chan struct{})}
}

//  Dispatcher Interface
type Dispatcher interface {
	ConfigChanged(*v1.ConfigMap) Dispatcher
	Shutdown()
	UpdateSubscriptions(subscriberSpecs []eventingduck.SubscriberSpec) map[eventingduck.SubscriberSpec]error
}

// Define A DispatcherImpl Struct With Configuration & ConsumerGroup State
type DispatcherImpl struct {
	DispatcherConfig
	subscribers        map[types.UID]*SubscriberWrapper
	consumerUpdateLock sync.Mutex
	messageDispatcher  channel.MessageDispatcher
}

// Verify The DispatcherImpl Implements The Dispatcher Interface
var _ Dispatcher = &DispatcherImpl{}

// Dispatcher Constructor
func NewDispatcher(dispatcherConfig DispatcherConfig) Dispatcher {

	// Create The DispatcherImpl With Specified Configuration
	dispatcher := &DispatcherImpl{
		DispatcherConfig:  dispatcherConfig,
		subscribers:       make(map[types.UID]*SubscriberWrapper),
		messageDispatcher: channel.NewMessageDispatcher(dispatcherConfig.Logger),
	}

	// Return The DispatcherImpl
	return dispatcher
}

// Shutdown The Dispatcher
func (d *DispatcherImpl) Shutdown() {

	// Close ConsumerGroups Of All Subscriptions
	for _, subscriber := range d.subscribers {
		d.closeConsumerGroup(subscriber)
	}
}

// Update The Dispatcher's Subscriptions To Align With New State
func (d *DispatcherImpl) UpdateSubscriptions(subscriberSpecs []eventingduck.SubscriberSpec) map[eventingduck.SubscriberSpec]error {

	// Save the current subscriber specs so that reconfigure() can use them to recreate the Dispatcher if necessary
	d.currentSubscriberSpecs = subscriberSpecs

	if d.currentConfig == nil {
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
			groupId := fmt.Sprintf("kafka.%s", subscriberSpec.UID)

			// Create A ConsumerGroup Logger
			logger := d.Logger.With(zap.String("GroupId", groupId))

			// Attempt To Create A Kafka ConsumerGroup
			consumerGroup, _, err := consumer.CreateConsumerGroup(d.Brokers, d.currentConfig, groupId)
			if err != nil {

				// Log & Return Failure
				logger.Error("Failed To Create ConsumerGroup", zap.Error(err))
				failedSubscriptions[subscriberSpec] = err

			} else {

				// Create A New SubscriberWrapper With The ConsumerGroup
				subscriber := NewSubscriberWrapper(subscriberSpec, groupId, consumerGroup)

				// Should start observing metrics from Sarama Config.MetricsRegistry from CreateConsumerGroup() above ; )

				// Start The ConsumerGroup Processing Messages
				d.startConsuming(subscriber)

				// Track The New SubscriberWrapper For The SubscriberSpec As Active
				d.subscribers[subscriberSpec.UID] = subscriber
				activeSubscriptions[subscriberSpec.UID] = true
			}

		} else {

			// Otherwise Just Add To List Of Active Subscribers
			activeSubscriptions[subscriberSpec.UID] = true
		}
	}

	// Close ConsumerGroups For Removed Subscriptions (In Map But No Longer Active)
	for _, subscriber := range d.subscribers {
		if !activeSubscriptions[subscriber.UID] {
			d.closeConsumerGroup(subscriber)
		}
	}

	// Return Any Failed Subscriber Errors
	return failedSubscriptions
}

// Start Consuming Messages With The Specified Subscriber's ConsumerGroup
func (d *DispatcherImpl) startConsuming(subscriber *SubscriberWrapper) {

	// Validate The Subscriber / ConsumerGroup
	if subscriber != nil && subscriber.ConsumerGroup != nil {

		// Setup The ConsumerGroup Level Logger
		logger := d.Logger.With(zap.String("GroupId", subscriber.GroupId))

		// Asynchronously Process ConsumerGroup's Error Channel
		go func() {
			logger.Info("ConsumerGroup Error Processing Initiated")
			for err := range subscriber.ConsumerGroup.Errors() { // Closing ConsumerGroup Will Break Out Of This
				logger.Error("ConsumerGroup Error", zap.Error(err))
			}
			logger.Info("ConsumerGroup Error Processing Terminated")
		}()

		// Create A New ConsumerGroupHandler To Consume Messages With
		handler := NewHandler(logger, &subscriber.SubscriberSpec, d.ExponentialBackoff, d.InitialRetryInterval, d.MaxRetryTime)

		// Consume Messages Asynchronously
		go func() {

			// Infinite Loop To Support Server-Side ConsumerGroup Re-Balance Which Ends Consume() Execution
			ctx := context.Background()
			for {
				select {

				// Non-Blocking Stop Channel Check
				case <-subscriber.StopChan:
					logger.Info("ConsumerGroup Closed - Ceasing Consumption")
					return

				// Start ConsumerGroup Consumption
				default:
					logger.Info("ConsumerGroup Message Consumption Initiated")
					err := subscriber.ConsumerGroup.Consume(ctx, []string{d.Topic}, handler)
					if err != nil {
						if err == sarama.ErrClosedConsumerGroup {
							logger.Info("ConsumerGroup Closed Error - Ceasing Consumption") // Should be caught above but here as added precaution.
							break
						} else {
							logger.Error("ConsumerGroup Failed To Consume Messages", zap.Error(err))
						}
					}
				}
			}
		}()
	}
}

// Close The ConsumerGroup Associated With A Single Subscriber
func (d *DispatcherImpl) closeConsumerGroup(subscriber *SubscriberWrapper) {

	// Get The ConsumerGroup Associated with The Specified Subscriber
	consumerGroup := subscriber.ConsumerGroup

	// Create Logger With GroupId & Subscriber URI
	logger := d.Logger.With(zap.String("GroupId", subscriber.GroupId), zap.String("URI", subscriber.SubscriberURI.String()))

	// If The ConsumerGroup Is Valid
	if consumerGroup != nil {

		// Mark The Subscriber's ConsumerGroup As Stopped
		close(subscriber.StopChan)

		// Close The ConsumerGroup
		err := consumerGroup.Close()
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

// ConfigChanged is called by the configMapObserver handler function in main() so that
// settings specific to the dispatcher may be extracted and the ConsumerGroups restarted if necessary.
func (d *DispatcherImpl) ConfigChanged(configMap *v1.ConfigMap) Dispatcher {
	d.Logger.Debug("New ConfigMap Received", zap.String("configMap.Name", configMap.ObjectMeta.Name))

	// If there aren't any consumer-specific differences between the current config and the new one,
	// then just log that and move on; do not restart the ConsumerGroups unnecessarily.

	newConfig := sarama.NewConfig()
	err := commonconfig.MergeSaramaSettings(newConfig, configMap)
	if err != nil {
		d.Logger.Error("Unable to merge sarama settings", zap.Error(err))
		return nil
	}

	// Don't care about Admin or Producer sections; everything else is a change that needs to be implemented.
	newConfig.Admin = sarama.Config{}.Admin
	newConfig.Producer = sarama.Config{}.Producer

	if d.currentConfig != nil {
		// Some of the current config settings may not be overridden by the configmap (username, password, etc.)
		kafkaconsumer.UpdateConfig(newConfig, d.currentConfig.ClientID, d.currentConfig.Net.SASL.User, d.currentConfig.Net.SASL.Password)

		// Create a shallow copy of the current config so that we can empty out the Admin and Consumer before comparing.
		configCopy := d.currentConfig

		// The current config should theoretically have these sections zeroed already because Reconfigure should have been passed
		// a newConfig with the structs empty, but this is more explicit as to what our goal is and doesn't hurt.
		configCopy.Admin = sarama.Config{}.Admin
		configCopy.Producer = sarama.Config{}.Producer
		if commonconfig.SaramaConfigEqual(newConfig, configCopy) {
			d.Logger.Info(ConfigNotChangedLogMsg)
			return nil
		}
	}

	return d.reconfigure(newConfig)
}

// Reconfigure takes a new sarama.Config struct and applies the updated settings,
// restarting the Dispatcher/ConsumerGroups if required
func (d *DispatcherImpl) reconfigure(config *sarama.Config) Dispatcher {
	d.Logger.Info(ConfigChangedLogMsg)

	// "Reconfiguring" the Dispatcher involves creating a new one, but we can re-use some
	// of the original components, such as the list of current subscribers
	d.Shutdown()
	d.DispatcherConfig.currentConfig = config
	newDispatcher := NewDispatcher(d.DispatcherConfig)
	newDispatcher.UpdateSubscriptions(d.currentSubscriberSpecs)
	d.Logger.Info("Dispatcher Reconfigured; Switching to New Dispatcher")
	return newDispatcher
}
