/*
Copyright 2021 The Knative Authors

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
	"encoding/json"
	"fmt"
	nethttp "net/http"
	"strings"
	"sync"

	"github.com/Shopify/sarama"
	protocolkafka "github.com/cloudevents/sdk-go/protocol/kafka_sarama/v2"
	"github.com/cloudevents/sdk-go/v2/binding"
	"github.com/google/uuid"
	"go.opencensus.io/trace"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"knative.dev/pkg/kmeta"
	"knative.dev/pkg/logging"

	eventingchannels "knative.dev/eventing/pkg/channel"
	"knative.dev/eventing/pkg/channel/fanout"
	"knative.dev/eventing/pkg/kncloudevents"

	"knative.dev/eventing-kafka/pkg/channel/consolidated/utils"
	"knative.dev/eventing-kafka/pkg/channel/distributed/common/env"
	"knative.dev/eventing-kafka/pkg/common/client"
	"knative.dev/eventing-kafka/pkg/common/consumer"
	"knative.dev/eventing-kafka/pkg/common/tracing"
)

const (
	dispatcherReadySubHeader = "K-Subscriber-Status"
)

type KafkaDispatcher struct {
	receiver   *eventingchannels.MessageReceiver
	dispatcher *eventingchannels.MessageDispatcherImpl

	// Receiver data structures
	// map[string]eventingchannels.ChannelReference
	hostToChannelMap  sync.Map
	kafkaSyncProducer sarama.SyncProducer

	// Dispatcher data structures
	// consumerUpdateLock must be used to update all the below maps
	consumerUpdateLock   sync.Mutex
	channelSubscriptions map[types.NamespacedName]*KafkaSubscription
	subsConsumerGroups   map[types.UID]sarama.ConsumerGroup
	subscriptions        map[types.UID]Subscription
	kafkaConsumerFactory consumer.KafkaConsumerGroupFactory

	topicFunc TopicFunc
	logger    *zap.SugaredLogger
}

type Subscription struct {
	UID types.UID
	fanout.Subscription
}

func (sub Subscription) String() string {
	var s strings.Builder
	s.WriteString("UID: " + string(sub.UID))
	s.WriteRune('\n')
	if sub.Subscriber != nil {
		s.WriteString("Subscriber: " + sub.Subscriber.String())
		s.WriteRune('\n')
	}
	if sub.Reply != nil {
		s.WriteString("Reply: " + sub.Reply.String())
		s.WriteRune('\n')
	}
	if sub.DeadLetter != nil {
		s.WriteString("DeadLetter: " + sub.DeadLetter.String())
		s.WriteRune('\n')
	}
	return s.String()
}

func (d *KafkaDispatcher) ServeHTTP(w nethttp.ResponseWriter, r *nethttp.Request) {
	if r.Method != nethttp.MethodGet {
		w.WriteHeader(nethttp.StatusMethodNotAllowed)
		d.logger.Errorf("Received request method that wasn't GET: %s", r.Method)
		return
	}
	uriSplit := strings.Split(r.RequestURI, "/")
	if len(uriSplit) != 3 {
		w.WriteHeader(nethttp.StatusNotFound)
		d.logger.Errorf("Unable to process request: %s", r.RequestURI)
		return
	}
	channelRefNamespace, channelRefName := uriSplit[1], uriSplit[2]
	channelRef := types.NamespacedName{
		Name:      channelRefName,
		Namespace: channelRefNamespace,
	}
	if _, ok := d.channelSubscriptions[channelRef]; !ok {
		w.WriteHeader(nethttp.StatusNotFound)
		return
	}
	d.channelSubscriptions[channelRef].readySubscriptionsLock.RLock()
	defer d.channelSubscriptions[channelRef].readySubscriptionsLock.RUnlock()
	var subscriptions = make(map[string][]int32)
	w.Header().Set(dispatcherReadySubHeader, channelRefName)
	for s, ps := range d.channelSubscriptions[channelRef].channelReadySubscriptions {
		subscriptions[s] = ps.List()
	}
	jsonResult, err := json.Marshal(subscriptions)
	if err != nil {
		d.logger.Errorf("Error marshalling json for sub-status channelref: %s/%s, %w", channelRefNamespace, channelRefName, err)
		return
	}
	_, err = w.Write(jsonResult)
	if err != nil {
		d.logger.Errorf("Error writing jsonResult to serveHTTP writer: %w", err)
	}
}

func NewDispatcher(ctx context.Context, args *KafkaDispatcherArgs) (*KafkaDispatcher, error) {
	conf, err := client.NewConfigBuilder().
		WithClientId(args.ClientID).
		WithVersion(&sarama.V2_0_0_0).
		WithDefaults().
		WithAuth(args.KafkaAuthConfig).
		Build()

	if err != nil {
		return nil, fmt.Errorf("Error updating the Sarama Auth config: %w", err)
	}

	producer, err := sarama.NewSyncProducer(args.Brokers, conf)
	if err != nil {
		return nil, fmt.Errorf("unable to create kafka producer against Kafka bootstrap servers %v : %v", args.Brokers, err)
	}

	// Configured with the same connection arguments of IMC
	kncloudevents.ConfigureConnectionArgs(&kncloudevents.ConnectionArgs{
		MaxIdleConns:        1000,
		MaxIdleConnsPerHost: 100,
	})

	dispatcher := &KafkaDispatcher{
		dispatcher:           eventingchannels.NewMessageDispatcher(logging.FromContext(ctx).Desugar()),
		kafkaConsumerFactory: consumer.NewConsumerGroupFactory(args.Brokers, conf),
		channelSubscriptions: make(map[types.NamespacedName]*KafkaSubscription),
		subsConsumerGroups:   make(map[types.UID]sarama.ConsumerGroup),
		subscriptions:        make(map[types.UID]Subscription),
		kafkaSyncProducer:    producer,
		logger:               logging.FromContext(ctx),
		topicFunc:            args.TopicFunc,
	}

	go func() {
		dispatcher.logger.Fatal(nethttp.ListenAndServe(":8081", dispatcher))
	}()

	podName, err := env.GetRequiredConfigValue(logging.FromContext(ctx).Desugar(), env.PodNameEnvVarKey)
	if err != nil {
		return nil, err
	}
	containerName, err := env.GetRequiredConfigValue(logging.FromContext(ctx).Desugar(), env.ContainerNameEnvVarKey)
	if err != nil {
		return nil, err
	}
	reporter := eventingchannels.NewStatsReporter(containerName, kmeta.ChildName(podName, uuid.New().String()))
	receiverFunc, err := eventingchannels.NewMessageReceiver(
		func(ctx context.Context, channel eventingchannels.ChannelReference, message binding.Message, transformers []binding.Transformer, _ nethttp.Header) error {
			kafkaProducerMessage := sarama.ProducerMessage{
				Topic: dispatcher.topicFunc(utils.KafkaChannelSeparator, channel.Namespace, channel.Name),
			}

			dispatcher.logger.Debugw("Received a new message from MessageReceiver, dispatching to Kafka", zap.Any("channel", channel))
			err := protocolkafka.WriteProducerMessage(ctx, message, &kafkaProducerMessage, transformers...)
			if err != nil {
				return err
			}

			kafkaProducerMessage.Headers = append(kafkaProducerMessage.Headers, tracing.SerializeTrace(trace.FromContext(ctx).SpanContext())...)

			partition, offset, err := dispatcher.kafkaSyncProducer.SendMessage(&kafkaProducerMessage)

			if err == nil {
				dispatcher.logger.Debugw("message sent", zap.Int32("partition", partition), zap.Int64("offset", offset))
			} else {
				dispatcher.logger.Warnw("message not sent", zap.Error(err))
			}

			return err
		},
		logging.FromContext(ctx).Desugar(),
		reporter,
		eventingchannels.ResolveMessageChannelFromHostHeader(dispatcher.getChannelReferenceFromHost))
	if err != nil {
		return nil, err
	}

	dispatcher.receiver = receiverFunc
	return dispatcher, nil
}

type TopicFunc func(separator, namespace, name string) string

type KafkaDispatcherArgs struct {
	KnCEConnectionArgs *kncloudevents.ConnectionArgs
	ClientID           string
	Brokers            []string
	KafkaAuthConfig    *client.KafkaAuthConfig
	TopicFunc          TopicFunc
}

type ChannelConfig struct {
	Namespace     string
	Name          string
	HostName      string
	Subscriptions []Subscription
}

func (cc ChannelConfig) SubscriptionsUIDs() []string {
	res := make([]string, 0, len(cc.Subscriptions))
	for _, s := range cc.Subscriptions {
		res = append(res, string(s.UID))
	}
	return res
}

// UpdateError is the error returned from the ReconcileConsumers method, with the details of which
// subscriptions failed to subscribe to.
type UpdateError map[types.UID]error

func (k UpdateError) Error() string {
	errs := make([]string, 0, len(k))
	for uid, err := range k {
		errs = append(errs, fmt.Sprintf("subscription %s: %v", uid, err))
	}
	return strings.Join(errs, ",")
}

// ReconcileConsumers will be called by new CRD based kafka channel dispatcher controller.
func (d *KafkaDispatcher) ReconcileConsumers(config *ChannelConfig) error {
	channelNamespacedName := types.NamespacedName{
		Namespace: config.Namespace,
		Name:      config.Name,
	}

	// Aux data structures to reconcile
	toAddSubs := make(map[types.UID]Subscription)
	toRemoveSubs := sets.NewString()

	d.consumerUpdateLock.Lock()
	defer d.consumerUpdateLock.Unlock()

	// This loop takes care of filling toAddSubs and toRemoveSubs for new and existing channels
	thisChannelKafkaSubscriptions := d.channelSubscriptions[channelNamespacedName]

	var existingSubsForThisChannel sets.String
	if thisChannelKafkaSubscriptions != nil {
		existingSubsForThisChannel = thisChannelKafkaSubscriptions.subs
	} else {
		existingSubsForThisChannel = sets.NewString()
	}

	newSubsForThisChannel := sets.NewString(config.SubscriptionsUIDs()...)

	// toRemoveSubs += existing subs of this channel - new subs of this channel
	thisChannelToRemoveSubs := existingSubsForThisChannel.Difference(newSubsForThisChannel).UnsortedList()
	toRemoveSubs.Insert(
		thisChannelToRemoveSubs...,
	)

	// toAddSubs += new subs of this channel - existing subs of this channel
	thisChannelToAddSubs := newSubsForThisChannel.Difference(existingSubsForThisChannel)
	for _, subSpec := range config.Subscriptions {
		if thisChannelToAddSubs.Has(string(subSpec.UID)) {
			toAddSubs[subSpec.UID] = subSpec
		}
	}

	d.logger.Debug("Number of new subs", zap.Any("subs", len(toAddSubs)))
	d.logger.Debug("Number of old subs", zap.Any("subs", len(toRemoveSubs)))

	failedToSubscribe := make(UpdateError)
	for subUid, subSpec := range toAddSubs {
		if err := d.subscribe(channelNamespacedName, subSpec); err != nil {
			failedToSubscribe[subUid] = err
		}
	}
	d.logger.Debug("Number of subs failed to subscribe", zap.Any("subs", len(failedToSubscribe)))

	for _, subUid := range toRemoveSubs.UnsortedList() {
		// We don't signal to the caller the unsubscribe invocation
		if err := d.unsubscribe(channelNamespacedName, d.subscriptions[types.UID(subUid)]); err != nil {
			d.logger.Warnw("Error while unsubscribing", zap.Error(err))
		}
	}

	if len(failedToSubscribe) == 0 {
		return nil
	}
	return failedToSubscribe
}

// RegisterChannelHost adds a new channel to the host-channel mapping.
func (d *KafkaDispatcher) RegisterChannelHost(channelConfig *ChannelConfig) error {
	old, ok := d.hostToChannelMap.LoadOrStore(channelConfig.HostName, eventingchannels.ChannelReference{
		Name:      channelConfig.Name,
		Namespace: channelConfig.Namespace,
	})
	if ok {
		oldChannelRef := old.(eventingchannels.ChannelReference)
		if !(oldChannelRef.Namespace == channelConfig.Namespace && oldChannelRef.Name == channelConfig.Name) {
			// If something is already there, but it's not the same channel, then fail
			return fmt.Errorf(
				"duplicate hostName found. Each channel must have a unique host header. HostName:%s, channel:%s.%s, channel:%s.%s",
				channelConfig.HostName,
				old.(eventingchannels.ChannelReference).Namespace,
				old.(eventingchannels.ChannelReference).Name,
				channelConfig.Namespace,
				channelConfig.Name,
			)
		}
	}
	return nil
}

func (d *KafkaDispatcher) CleanupChannel(name, namespace, hostname string) error {
	channelRef := types.NamespacedName{
		Name:      name,
		Namespace: namespace,
	}

	// Remove from the hostToChannel map the mapping with this channel
	d.hostToChannelMap.Delete(hostname)

	// Remove all subs
	d.consumerUpdateLock.Lock()
	defer d.consumerUpdateLock.Unlock()

	if d.channelSubscriptions[channelRef] == nil {
		// No subs to remove
		return nil
	}

	for _, s := range d.channelSubscriptions[channelRef].subs.UnsortedList() {
		if err := d.unsubscribe(channelRef, d.subscriptions[types.UID(s)]); err != nil {
			return err
		}
	}

	return nil
}

// Start starts the kafka dispatcher's message processing.
func (d *KafkaDispatcher) Start(ctx context.Context) error {
	if d.receiver == nil {
		return fmt.Errorf("message receiver is not set")
	}

	return d.receiver.Start(ctx)
}

// subscribe reads kafkaConsumers which gets updated in UpdateConfig in a separate go-routine.
// subscribe must be called under updateLock.
func (d *KafkaDispatcher) subscribe(channelRef types.NamespacedName, sub Subscription) error {
	d.logger.Infow("Subscribing to Kafka Channel", zap.Any("channelRef", channelRef), zap.Any("subscription", sub.UID))

	topicName := d.topicFunc(utils.KafkaChannelSeparator, channelRef.Namespace, channelRef.Name)
	groupID := fmt.Sprintf("kafka.%s.%s.%s", channelRef.Namespace, channelRef.Name, string(sub.UID))

	// Get or create the channel kafka subscription
	kafkaSubscription, ok := d.channelSubscriptions[channelRef]
	if !ok {
		kafkaSubscription = NewKafkaSubscription(d.logger)
		d.channelSubscriptions[channelRef] = kafkaSubscription
	}

	handler := &consumerMessageHandler{
		d.logger,
		sub,
		d.dispatcher,
		kafkaSubscription,
		groupID,
	}
	d.logger.Debugw("Starting consumer group", zap.Any("channelRef", channelRef),
		zap.Any("subscription", sub.UID), zap.String("topic", topicName), zap.String("consumer group", groupID))
	consumerGroup, err := d.kafkaConsumerFactory.StartConsumerGroup(groupID, []string{topicName}, d.logger, handler)

	if err != nil {
		// we can not create a consumer - logging that, with reason
		d.logger.Infow("Could not create proper consumer", zap.Error(err))
		return err
	}

	// sarama reports error in consumerGroup.Error() channel
	// this goroutine logs errors incoming
	go func() {
		for err = range consumerGroup.Errors() {
			d.logger.Warnw("Error in consumer group", zap.Error(err))
		}
	}()

	// Update the data structures that holds the reconciliation data
	kafkaSubscription.subs.Insert(string(sub.UID))
	d.subscriptions[sub.UID] = sub
	d.subsConsumerGroups[sub.UID] = consumerGroup

	return nil
}

// unsubscribe reads kafkaConsumers which gets updated in UpdateConfig in a separate go-routine.
// unsubscribe must be called under updateLock.
func (d *KafkaDispatcher) unsubscribe(channelRef types.NamespacedName, sub Subscription) error {
	d.logger.Infow("Unsubscribing from channel", zap.Any("channel", channelRef), zap.Any("subscription", sub.UID))

	// Remove the sub spec
	delete(d.subscriptions, sub.UID)

	// Remove the sub from the channel
	kafkaSubscription, ok := d.channelSubscriptions[channelRef]
	if !ok {
		// If this happens, then there's a bug somewhere...
		return nil
	}
	kafkaSubscription.subs.Delete(string(sub.UID))
	if kafkaSubscription.subs.Len() == 0 {
		// We can get rid of this
		delete(d.channelSubscriptions, channelRef)
	}

	// Delete the consumer group
	if consumerGroup, ok := d.subsConsumerGroups[sub.UID]; ok {
		delete(d.subsConsumerGroups, sub.UID)
		d.logger.Debugw("Closing cached consumerGroup group", zap.Any("consumer group", consumerGroup))
		return consumerGroup.Close()
	}
	return nil
}

func (d *KafkaDispatcher) getChannelReferenceFromHost(host string) (eventingchannels.ChannelReference, error) {
	cr, ok := d.hostToChannelMap.Load(host)
	if !ok {
		return eventingchannels.ChannelReference{}, eventingchannels.UnknownHostError(host)
	}
	return cr.(eventingchannels.ChannelReference), nil
}
