package dispatcher

import (
	"errors"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"go.uber.org/zap"
	kafkaconsumer "knative.dev/eventing-kafka/pkg/common/kafka/consumer"
	"knative.dev/eventing-kafka/pkg/common/metrics"
	eventingduck "knative.dev/eventing/pkg/apis/duck/v1alpha1"
	"knative.dev/eventing/pkg/channel"
	"strings"
	"sync"
	"time"
)

// Define A Dispatcher Config Struct To Hold Configuration
type DispatcherConfig struct {
	Logger                      *zap.Logger
	Brokers                     string
	Topic                       string
	Offset                      string
	PollTimeoutMillis           int
	OffsetCommitCount           int64
	OffsetCommitDuration        time.Duration
	OffsetCommitDurationMinimum time.Duration
	Username                    string
	Password                    string
	ChannelKey                  string
	Reporter                    metrics.StatsReporter
	ExponentialBackoff          bool
	InitialRetryInterval        int64
	MaxRetryTime                int64
}

type ConsumerOffset struct {
	consumer         kafkaconsumer.ConsumerInterface
	lastOffsetCommit time.Time
	offsets          map[int32]kafka.Offset
	stopCh           chan bool
	stoppedCh        chan bool
}

type Subscription struct {
	eventingduck.SubscriberSpec
	GroupId string
}

// Define a Dispatcher Struct to hold Dispatcher Config and dispatcher implementation details
type Dispatcher struct {
	DispatcherConfig
	consumers          map[Subscription]*ConsumerOffset
	consumerUpdateLock sync.Mutex
	messageDispatcher  channel.MessageDispatcher
}

// Create A New Dispatcher Of Specified Configuration
func NewDispatcher(dispatcherConfig DispatcherConfig) *Dispatcher {

	// Create The Dispatcher With Specified Configuration
	dispatcher := &Dispatcher{
		DispatcherConfig:  dispatcherConfig,
		consumers:         make(map[Subscription]*ConsumerOffset),
		messageDispatcher: channel.NewMessageDispatcher(dispatcherConfig.Logger),
	}

	// Return The Dispatcher
	return dispatcher
}

// Stop All Consumers
func (d *Dispatcher) StopConsumers() {
	for subscription := range d.consumers {
		d.stopConsumer(subscription)
	}
}

// Stop An Individual Consumer
func (d *Dispatcher) stopConsumer(subscription Subscription) {
	d.Logger.Info("Stopping Consumer", zap.String("GroupId", subscription.GroupId), zap.String("topic", d.Topic), zap.String("URI", subscription.SubscriberURI.String()))
	consumerOffset := d.consumers[subscription]
	consumerOffset.stopCh <- true // Send Stop Signal
	<-consumerOffset.stoppedCh    // Wait Until Stop Completes
	delete(d.consumers, subscription)
}

// Create And Start A Consumer
func (d *Dispatcher) initConsumer(subscription Subscription) (*ConsumerOffset, error) {

	// Create Consumer
	d.Logger.Info("Creating Consumer", zap.String("GroupId", subscription.GroupId), zap.String("topic", d.Topic), zap.String("URI", subscription.SubscriberURI.String()))
	consumer, err := kafkaconsumer.CreateConsumer(d.Brokers, subscription.GroupId, d.Offset, d.Username, d.Password)
	if err != nil {
		d.Logger.Error("Failed To Create New Consumer", zap.Error(err))
		return nil, err
	}

	// Subscribe To The Topic
	err = consumer.Subscribe(d.Topic, nil)
	if err != nil {
		d.Logger.Error("Failed To Subscribe To Topic", zap.String("Topic", d.Topic), zap.Error(err))
		return nil, err
	}

	// Create The ConsumerOffset For Tracking State
	consumerOffset := ConsumerOffset{
		consumer:         consumer,
		lastOffsetCommit: time.Now(),
		offsets:          make(map[int32]kafka.Offset),
		stopCh:           make(chan bool),
		stoppedCh:        make(chan bool),
	}

	// Start Consuming Messages From Topic (Async)
	go d.handleKafkaMessages(consumerOffset, subscription)

	// Return The Consumer
	return &consumerOffset, nil
}

// Consumer Message Handler - Wait For Consumer Messages, Log Them & Commit The Offset
func (d *Dispatcher) handleKafkaMessages(consumerOffset ConsumerOffset, subscription Subscription) {

	// Configure The Logger
	logger := d.Logger.With(zap.String("GroupID", subscription.GroupId))

	// Message Processing Loop
	stopped := false
	for !stopped {

		// Poll For A New Event Message Until We Get One (Timeout is how long to wait before requesting again)
		event := consumerOffset.consumer.Poll(d.PollTimeoutMillis)

		select {

		// Non-Blocking Channel Check
		case <-consumerOffset.stopCh:
			stopped = true // Handle Shutdown Case - Break Out Of Loop

		default:

			// No Events To Process At The Moment
			if event == nil {

				// Commit Offsets If The Amount Of Time Since Last Commit Is "OffsetCommitDuration" Or More
				currentTimeDuration := time.Now().Sub(consumerOffset.lastOffsetCommit)
				if currentTimeDuration > d.OffsetCommitDurationMinimum && currentTimeDuration >= d.OffsetCommitDuration {
					d.commitOffsets(logger, &consumerOffset)
				}

				// Continue Polling For Events
				continue
			}

			// Handle Event Messages / Errors Based On Type
			switch e := event.(type) {

			case *kafka.Message:

				// Debug Log Kafka Message Dispatching
				logger.Debug("Received Kafka Message - Dispatching",
					zap.String("Message", string(e.Value)),
					zap.String("Topic", *e.TopicPartition.Topic),
					zap.Int32("Partition", e.TopicPartition.Partition),
					zap.Any("Offset", e.TopicPartition.Offset))

				// Convert The Kafka Message To A Cloud Event
				cloudEvent, err := d.convertToCloudEvent(e)
				if err != nil {
					logger.Error("Unable To Convert Kafka Message To CloudEvent, Skipping", zap.Any("message", e))
					continue
				}

				// Dispatch (Send!) The CloudEvent To The Subscription URL  (Ignore Errors - Dispatcher Will Retry And We're Moving On!)
				_ = d.Dispatch(cloudEvent, subscription)

				// Update Stored Offsets Based On The Processed Message
				d.updateOffsets(consumerOffset.consumer, e)
				currentTimeDuration := time.Now().Sub(consumerOffset.lastOffsetCommit)

				// If "OffsetCommitCount" Number Of Messages Have Been Processed Since Last Offset Commit, Then Do One Now
				if currentTimeDuration > d.OffsetCommitDurationMinimum &&
					int64(e.TopicPartition.Offset-consumerOffset.offsets[e.TopicPartition.Partition]) >= d.OffsetCommitCount {
					d.commitOffsets(logger, &consumerOffset)
				}

			case *kafka.Stats:
				// Update Kafka Prometheus Metrics
				d.Reporter.Report(e.String())

			case kafka.Error:
				logger.Warn("Received Kafka Error", zap.Error(e))

			default:
				logger.Info("Received Unknown Event Type - Ignoring", zap.String("Message", e.String()))
			}
		}
	}

	// Commit Offsets One Last Time
	logger.Debug("Final Offset Commit")
	d.commitOffsets(logger, &consumerOffset)

	// Safe To Shutdown The Consumer
	logger.Debug("Shutting Down Consumer")
	err := consumerOffset.consumer.Close()
	if err != nil {
		logger.Error("Unable To Stop Consumer", zap.Error(err))
	}
	consumerOffset.stoppedCh <- true
}

// Store Updated Offsets For The Partition If Consumer Still Has It Assigned
func (d *Dispatcher) updateOffsets(consumer kafkaconsumer.ConsumerInterface, message *kafka.Message) {
	offsets := []kafka.TopicPartition{message.TopicPartition}
	offsets[0].Offset++
	topicPartitions, err := consumer.StoreOffsets(offsets)
	if err != nil {
		d.Logger.Error("Kafka Consumer Failed To Store Offsets", zap.Any("TopicPartitions", topicPartitions), zap.Error(err))
	}
}

// Commit The Stored Offsets For Partitions Still Assigned To This Consumer
func (d *Dispatcher) commitOffsets(logger *zap.Logger, consumerOffset *ConsumerOffset) {
	partitions, err := consumerOffset.consumer.Commit()
	if err != nil {
		// Don't Log Error If There Weren't Any Offsets To Commit
		if err.(kafka.Error).Code() != kafka.ErrNoOffset {
			logger.Error("Error Committing Offsets", zap.Error(err))
		}
	} else {
		logger.Debug("Committed Partitions", zap.Any("partitions", partitions))
		consumerOffset.offsets = make(map[int32]kafka.Offset)
		for _, partition := range partitions {
			consumerOffset.offsets[partition.Partition] = partition.Offset
		}
	}
	consumerOffset.lastOffsetCommit = time.Now()
}

// Update The Dispatcher's Subscriptions
func (d *Dispatcher) UpdateSubscriptions(subscriptions []Subscription) map[Subscription]error {

	failedSubscriptions := make(map[Subscription]error)
	activeSubscriptions := make(map[Subscription]bool)

	d.consumerUpdateLock.Lock()
	defer d.consumerUpdateLock.Unlock()

	for _, subscription := range subscriptions {

		// Create Consumer If Doesn't Already Exist, Tracking Active Subscriptions
		if _, ok := d.consumers[subscription]; !ok {
			consumer, err := d.initConsumer(subscription)
			if err != nil {
				failedSubscriptions[subscription] = err
			} else {
				d.consumers[subscription] = consumer
				activeSubscriptions[subscription] = true
			}
		} else {
			activeSubscriptions[subscription] = true
		}
	}

	// Stop Consumers For Removed Subscriptions
	for runningSubscription := range d.consumers {
		if !activeSubscriptions[runningSubscription] {
			d.stopConsumer(runningSubscription)
		}
	}

	return failedSubscriptions
}

// Convert Kafka Message To A Cloud Event, Eventually We Should Consider Writing A Cloud Event SDK Codec For This
func (d *Dispatcher) convertToCloudEvent(message *kafka.Message) (*cloudevents.Event, error) {
	var specVersion string

	// Determine CloudEvent Version
	for _, header := range message.Headers {
		if header.Key == "ce_specversion" {
			specVersion = string(header.Value)
			break
		}
	}

	if len(specVersion) == 0 {
		return nil, errors.New("could not determine CloudEvent version from Kafka message")
	}

	// Generate CloudEvent
	event := cloudevents.NewEvent(specVersion)
	for _, header := range message.Headers {
		h := header.Key
		var v = string(header.Value)
		switch h {
		case "ce_datacontenttype":
			err := event.SetData(v, message.Value)
			if err != nil {
				d.Logger.Error("Failed To Set CloudEvent Data From Kafka Message", zap.Error(err))
				return nil, err
			}
		case "ce_type":
			event.SetType(v)
		case "ce_source":
			event.SetSource(v)
		case "ce_id":
			event.SetID(v)
		case "ce_time":
			t, _ := time.Parse(time.RFC3339, v)
			event.SetTime(t)
		case "ce_subject":
			event.SetSubject(v)
		case "ce_dataschema":
			event.SetDataSchema(v)
		case "ce_specversion":
			// Already Handled So Don't Do Anything
		default:
			// Must Be An Extension, Remove The "ce_" Prefix
			event.SetExtension(strings.TrimPrefix(h, "ce_"), v)
		}
	}

	return &event, nil
}
