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

package controller

import (
	"context"
	"fmt"
	"reflect"

	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	eventingduck "knative.dev/eventing/pkg/apis/duck/v1"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"

	kafkav1beta1 "knative.dev/eventing-kafka/pkg/apis/messaging/v1beta1"
	"knative.dev/eventing-kafka/pkg/channel/distributed/dispatcher/constants"
	"knative.dev/eventing-kafka/pkg/channel/distributed/dispatcher/dispatcher"
	"knative.dev/eventing-kafka/pkg/client/clientset/versioned"
	"knative.dev/eventing-kafka/pkg/client/clientset/versioned/scheme"
	informers "knative.dev/eventing-kafka/pkg/client/informers/externalversions/messaging/v1beta1"
	listers "knative.dev/eventing-kafka/pkg/client/listers/messaging/v1beta1"
	commonconsumer "knative.dev/eventing-kafka/pkg/common/consumer"
)

const (
	// ReconcilerName is the name of the reconciler.
	ReconcilerName = "KafkaChannels"

	// corev1.Events emitted
	channelReconciled         = "ChannelReconciled"
	channelReconcileFailed    = "ChannelReconcileFailed"
	channelUpdateStatusFailed = "ChannelUpdateStatusFailed"
)

// Reconciler reconciles KafkaChannels.
type Reconciler struct {
	logger               *zap.Logger
	channelKey           string
	dispatcher           dispatcher.Dispatcher
	kafkachannelInformer cache.SharedIndexInformer
	kafkachannelLister   listers.KafkaChannelLister
	impl                 *controller.Impl
	recorder             record.EventRecorder
	kafkaClientSet       versioned.Interface
}

var _ controller.Reconciler = Reconciler{}

// NewController initializes the controller and is called by the generated code.
// Registers event handlers to enqueue events.
func NewController(
	ctx context.Context,
	logger *zap.Logger,
	channelKey string,
	dispatcher dispatcher.Dispatcher,
	kafkachannelInformer informers.KafkaChannelInformer,
	kubeClient kubernetes.Interface,
	kafkaClientSet versioned.Interface,
	stopChannel <-chan struct{},
	managerEvents <-chan commonconsumer.ManagerEvent,
) *controller.Impl {

	reconciler := &Reconciler{
		logger:               logger,
		channelKey:           channelKey,
		dispatcher:           dispatcher,
		kafkachannelInformer: kafkachannelInformer.Informer(),
		kafkachannelLister:   kafkachannelInformer.Lister(),
		kafkaClientSet:       kafkaClientSet,
	}
	reconciler.impl = controller.NewContext(ctx, reconciler, controller.ControllerOptions{
		WorkQueueName: ReconcilerName,
		Logger:        logger.Sugar(),
	})

	reconciler.logger.Info("Setting Up Event Handlers")

	// Watch for kafka channels.
	kafkachannelInformer.Informer().AddEventHandler(controller.HandleAll(reconciler.impl.Enqueue))
	logger.Debug("Creating event broadcaster")
	eventBroadcaster := record.NewBroadcaster()
	watches := []watch.Interface{
		eventBroadcaster.StartLogging(logger.Sugar().Named("event-broadcaster").Infof),
		eventBroadcaster.StartRecordingToSink(
			&typedcorev1.EventSinkImpl{Interface: kubeClient.CoreV1().Events("")}),
	}
	reconciler.recorder = eventBroadcaster.NewRecorder(
		scheme.Scheme, corev1.EventSource{Component: ReconcilerName})
	go func() {
		<-stopChannel
		for _, w := range watches {
			w.Stop()
		}
	}()
	if err := reconciler.processManagerEvents(managerEvents); err != nil {
		logger.Warn("Could not begin processing events from the Consumer Group Manager", zap.Error(err))
	}
	return reconciler.impl
}

// processManagerEvents will listen on the channel provided by the KafkaConsumerGroupManager for events
// related to a change in the status of a managed ConsumerGroup.  This function is non-blocking, and the
// internal goroutine will exit when the channel is closed.
func (r Reconciler) processManagerEvents(events <-chan commonconsumer.ManagerEvent) error {
	// If the events channel is nil, there's no point listening to it; it will just block forever
	if events == nil {
		return fmt.Errorf("no event channel provided")
	}
	// Find the namespace and name of the KafkaChannel this dispatcher is monitoring
	namespace, name, err := cache.SplitMetaNamespaceKey(r.channelKey)
	if err != nil {
		return fmt.Errorf("invalid resource key")
	}
	key := types.NamespacedName{Namespace: namespace, Name: name}
	go func() {
		for event := range events {
			groupLogger := r.logger.With(zap.String("groupId", event.GroupId))
			// Stopping or Starting a consumer group requires a reconciliation in order to adjust the status block
			// of the KafkaChannel - EnqueueKey will do that
			switch event.Event {
			case commonconsumer.GroupStopped:
				groupLogger.Debug("Processing GroupStopped Event From Consumer Group Manager")
				r.impl.EnqueueKey(key)
			case commonconsumer.GroupStarted:
				groupLogger.Debug("Processing GroupStarted Event From Consumer Group Manager")
				r.impl.EnqueueKey(key)
			case commonconsumer.GroupCreated:
				groupLogger.Debug("Processing GroupCreated Event From Consumer Group Manager")
			case commonconsumer.GroupClosed:
				groupLogger.Debug("Processing GroupClosed Event From Consumer Group Manager")
			default:
				groupLogger.Warn("Received Unexpected Event From Consumer Group Manager", zap.Int("Event", int(event.Event)))
			}
		}
		r.logger.Debug("Manager event channel closed")
	}()
	return nil
}

func (r Reconciler) Reconcile(ctx context.Context, key string) error {

	r.logger.Info("Reconcile", zap.String("key", key))

	// Only Reconcile KafkaChannel Associated With This Dispatcher
	if r.channelKey != key {
		return nil
	}

	// Convert the namespace/name string into a distinct namespace and name.
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		logging.FromContext(ctx).Error("invalid resource key", zap.String("Key", key), zap.Error(err))
		return nil
	}

	// Get the KafkaChannel resource with this namespace/name.
	original, err := r.kafkachannelLister.KafkaChannels(namespace).Get(name)
	if err != nil {
		if apierrs.IsNotFound(err) {
			r.logger.Warn("KafkaChannel No Longer Exists", zap.String("namespace", namespace), zap.String("name", name))
			return nil
		}
		r.logger.Error("Error Retrieving KafkaChannel", zap.Error(err), zap.String("namespace", namespace), zap.String("name", name))
		return err
	}

	if !original.Status.IsReady() {
		return fmt.Errorf("channel is not ready - cannot configure and update subscriber status")
	}

	// Don't modify the informers copy
	channel := original.DeepCopy()

	// Perform the reconciliation (will update KafkaChannel.Status)
	reconcileError := r.reconcile(ctx, channel)
	if reconcileError != nil {
		r.logger.Error("Error Reconciling KafkaChannel", zap.Error(reconcileError))
		r.recorder.Eventf(channel, corev1.EventTypeWarning, channelReconcileFailed, "KafkaChannel Reconciliation Failed: %v", reconcileError)
	} else {
		r.logger.Debug("KafkaChannel Reconciled Successfully")
		r.recorder.Event(channel, corev1.EventTypeNormal, channelReconciled, "KafkaChannel Reconciled")
	}

	// Push KafkaChannel Status Changes To K8S
	_, updateStatusErr := r.updateStatus(ctx, channel)
	if updateStatusErr != nil {
		r.logger.Error("Failed To Update KafkaChannel Status", zap.Error(updateStatusErr))
		r.recorder.Eventf(channel, corev1.EventTypeWarning, channelUpdateStatusFailed, "Failed to update KafkaChannel's status: %v", updateStatusErr)
		return updateStatusErr
	} else {
		r.logger.Info("Successfully Verified / Updated KafkaChannel Status")
	}

	// Return Reconciliation Errors To Requeue
	return reconcileError
}

// Reconcile The Specified KafkaChannel
func (r Reconciler) reconcile(ctx context.Context, channel *kafkav1beta1.KafkaChannel) error {

	// The KafkaChannel's Subscribers
	var subscribers []eventingduck.SubscriberSpec

	// Clone The Subscribers If They Exist - Otherwise Create Empty Array
	if channel.Spec.Subscribers != nil {
		subscribers = channel.Spec.Subscribers
	} else {
		subscribers = make([]eventingduck.SubscriberSpec, 0)
	}

	// Update The ConsumerGroups To Align With Current KafkaChannel Subscribers
	channelRef := types.NamespacedName{
		Namespace: channel.GetNamespace(),
		Name:      channel.GetName(),
	}
	subscriptions := r.dispatcher.UpdateSubscriptions(ctx, channelRef, subscribers)

	// Update The KafkaChannel Subscribable Status Based On ConsumerGroup Creation Status
	channel.Status.SubscribableStatus = r.createSubscribableStatus(channel.Spec.Subscribers, subscriptions)

	// Log Failed Subscriptions & Return Error
	if failed := subscriptions.FailedCount(); failed > 0 {
		r.logger.Error("Failed To Subscribe Kafka Subscriptions", zap.Int("Count", failed))
		return fmt.Errorf("some kafka subscribers failed to subscribe")
	}

	// Return Success
	return nil
}

// Create The SubscribableStatus Block Based On The Updated Subscriptions
func (r *Reconciler) createSubscribableStatus(subscribers []eventingduck.SubscriberSpec, subscriptions commonconsumer.SubscriberStatusMap) eventingduck.SubscribableStatus {

	subscriberStatus := make([]eventingduck.SubscriberStatus, 0)

	for _, subscriber := range subscribers {
		status := eventingduck.SubscriberStatus{
			UID:                subscriber.UID,
			ObservedGeneration: subscriber.Generation,
			Ready:              corev1.ConditionTrue,
		}
		// If the UID isn't in the subscription map, the zero-value of the subscriptionStatus will have a nil Error
		// and Stopped will be false.
		subscriptionStatus := subscriptions[subscriber.UID]
		if subscriptionStatus.Error != nil {
			status.Ready = corev1.ConditionFalse
			status.Message = subscriptionStatus.Error.Error()
		} else if subscriptionStatus.Stopped {
			// A stopped group isn't an "error" but it does represent a group that isn't "Ready" as far
			// as subscriber status goes.
			status.Ready = corev1.ConditionFalse
			status.Message = constants.GroupStoppedMessage
		}

		subscriberStatus = append(subscriberStatus, status)
	}

	return eventingduck.SubscribableStatus{
		Subscribers: subscriberStatus,
	}
}

func (r *Reconciler) updateStatus(ctx context.Context, desired *kafkav1beta1.KafkaChannel) (*kafkav1beta1.KafkaChannel, error) {
	kc, err := r.kafkachannelLister.KafkaChannels(desired.Namespace).Get(desired.Name)
	if err != nil {
		return nil, err
	}

	if reflect.DeepEqual(kc.Status, desired.Status) {
		r.logger.Debug("KafkaChannel Status Already Current - Skipping Update")
		return kc, nil
	}

	// Don't modify the informers copy.
	existing := kc.DeepCopy()
	existing.Status = desired.Status
	updated, err := r.kafkaClientSet.MessagingV1beta1().KafkaChannels(desired.Namespace).UpdateStatus(ctx, existing, metav1.UpdateOptions{})
	return updated, err
}
