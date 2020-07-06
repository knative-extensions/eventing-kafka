package controller

import (
	"context"
	"fmt"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	kafkav1alpha1 "knative.dev/eventing-contrib/kafka/channel/pkg/apis/messaging/v1alpha1"
	"knative.dev/eventing-contrib/kafka/channel/pkg/client/clientset/versioned"
	"knative.dev/eventing-contrib/kafka/channel/pkg/client/clientset/versioned/scheme"
	"knative.dev/eventing-contrib/kafka/channel/pkg/client/informers/externalversions/messaging/v1alpha1"
	listers "knative.dev/eventing-contrib/kafka/channel/pkg/client/listers/messaging/v1alpha1"
	"knative.dev/eventing-kafka/pkg/dispatcher/dispatcher"
	eventingduckv1alpha1 "knative.dev/eventing/pkg/apis/duck/v1alpha1"
	eventingduckv1beta1 "knative.dev/eventing/pkg/apis/duck/v1beta1"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"
	"reflect"
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
	logger *zap.Logger,
	channelKey string,
	dispatcher dispatcher.Dispatcher,
	kafkachannelInformer v1alpha1.KafkaChannelInformer,
	kubeClient kubernetes.Interface,
	kafkaClientSet versioned.Interface,
	stopChannel <-chan struct{},
) *controller.Impl {

	reconciler := &Reconciler{
		logger:               logger,
		channelKey:           channelKey,
		dispatcher:           dispatcher,
		kafkachannelInformer: kafkachannelInformer.Informer(),
		kafkachannelLister:   kafkachannelInformer.Lister(),
		kafkaClientSet:       kafkaClientSet,
	}
	reconciler.impl = controller.NewImpl(reconciler, reconciler.logger.Sugar(), ReconcilerName)

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

	return reconciler.impl
}

func (r Reconciler) Reconcile(ctx context.Context, key string) error {

	r.logger.Info("Reconcile", zap.String("key", key))

	// Convert the namespace/name string into a distinct namespace and name.
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		logging.FromContext(ctx).Error("invalid resource key")
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

	// Only Reconcile KafkaChannel Associated With This Dispatcher
	if r.channelKey != key {
		return nil
	}

	if !original.Status.IsReady() {
		return fmt.Errorf("channel is not ready - cannot configure and update subscriber status")
	}

	// Don't modify the informers copy
	channel := original.DeepCopy()

	reconcileError := r.reconcile(channel)
	if reconcileError != nil {
		r.logger.Error("Error Reconciling KafkaChannel", zap.Error(reconcileError))
		r.recorder.Eventf(channel, corev1.EventTypeWarning, channelReconcileFailed, "KafkaChannel Reconciliation Failed: %v", reconcileError)
	} else {
		r.logger.Debug("KafkaChannel Reconciled Successfully")
		r.recorder.Event(channel, corev1.EventTypeNormal, channelReconciled, "KafkaChannel Reconciled")
	}

	_, updateStatusErr := r.updateStatus(channel)
	if updateStatusErr != nil {
		r.logger.Error("Failed To Update KafkaChannel Status", zap.Error(updateStatusErr))
		r.recorder.Eventf(channel, corev1.EventTypeWarning, channelUpdateStatusFailed, "Failed to update KafkaChannel's status: %v", updateStatusErr)
		return updateStatusErr
	} else {
		r.logger.Info("Successfully Verified / Updated KafkaChannel Status")
	}

	// Return Success
	return nil
}

// Reconcile The Specified KafkaChannel
func (r Reconciler) reconcile(channel *kafkav1alpha1.KafkaChannel) error {

	// The KafkaChannel's Subscribers
	var subscribers []eventingduckv1alpha1.SubscriberSpec

	// Clone The Subscribers If They Exist - Otherwise Create Empty Array
	if channel.Spec.Subscribable != nil && channel.Spec.Subscribable.Subscribers != nil {
		subscribers = channel.Spec.Subscribable.Subscribers
	} else {
		subscribers = make([]eventingduckv1alpha1.SubscriberSpec, 0)
	}

	// Update The ConsumerGroups To Align With Current KafkaChannel Subscribers
	failedSubscriptions := r.dispatcher.UpdateSubscriptions(subscribers)

	// Update The KafkaChannel Subscribable Status Based On ConsumerGroup Creation Status
	channel.Status.SubscribableStatus = r.createSubscribableStatus(channel.Spec.Subscribable, failedSubscriptions)

	// Log Failed Subscriptions & Return Error
	if len(failedSubscriptions) > 0 {
		r.logger.Error("Failed To Subscribe Kafka Subscriptions", zap.Int("Count", len(failedSubscriptions)))
		return fmt.Errorf("some kafka subscribers failed to subscribe")
	}

	// Return Success
	return nil
}

// Create The SubscribableStatus Block Based On The Updated Subscriptions
func (r *Reconciler) createSubscribableStatus(subscribable *eventingduckv1alpha1.Subscribable, failedSubscriptions map[eventingduckv1alpha1.SubscriberSpec]error) *eventingduckv1alpha1.SubscribableStatus {

	subscriberStatus := make([]eventingduckv1beta1.SubscriberStatus, 0)

	if subscribable != nil && subscribable.Subscribers != nil {
		for _, subscriber := range subscribable.Subscribers {
			status := eventingduckv1beta1.SubscriberStatus{
				UID:                subscriber.UID,
				ObservedGeneration: subscriber.Generation,
				Ready:              corev1.ConditionTrue,
			}
			if err, ok := failedSubscriptions[subscriber]; ok {
				status.Ready = corev1.ConditionFalse
				status.Message = err.Error()
			}
			subscriberStatus = append(subscriberStatus, status)
		}
	}

	return &eventingduckv1alpha1.SubscribableStatus{
		Subscribers: subscriberStatus,
	}
}

func (r *Reconciler) updateStatus(desired *kafkav1alpha1.KafkaChannel) (*kafkav1alpha1.KafkaChannel, error) {
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
	updated, err := r.kafkaClientSet.MessagingV1alpha1().KafkaChannels(desired.Namespace).UpdateStatus(existing)
	return updated, err
}
