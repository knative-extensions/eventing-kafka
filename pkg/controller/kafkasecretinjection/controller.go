package kafkasecretinjection

import (
	"context"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/scheme"
	v1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/record"
	"knative.dev/eventing-kafka/pkg/controller/kafkasecretinformer"
	"knative.dev/eventing-kafka/pkg/controller/util"
	"knative.dev/pkg/client/injection/kube/client"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"
)

// Code manually created based on generated implementation in eventing-contrib/kafka - SEE README !!!

const (
	defaultControllerAgentName = "kafkasecret-controller"
	defaultQueueName           = "secrets"
)

var (
	// Need Prefix For Valid Finalizer On Native K8S Resources (Secrets)
	defaultFinalizerName = util.KubernetesResourceFinalizerName("kafkasecrets.eventing-kafka.knative.dev")
)

// NewImpl returns a controller.Impl that handles queuing and feeding work from
// the queue through an implementation of controller.Reconciler, delegating to
// the provided Interface and optional Finalizer methods.
func NewImpl(ctx context.Context, r Interface) *controller.Impl {
	logger := logging.FromContext(ctx)
	kafkaSecretInformer := kafkasecretinformer.Get(ctx)

	recorder := controller.GetEventRecorder(ctx)
	if recorder == nil {
		// Create event broadcaster
		logger.Debug("Creating event broadcaster")
		eventBroadcaster := record.NewBroadcaster()
		watches := []watch.Interface{
			eventBroadcaster.StartLogging(logger.Named("event-broadcaster").Infof),
			eventBroadcaster.StartRecordingToSink(
				&v1.EventSinkImpl{Interface: client.Get(ctx).CoreV1().Events("")}),
		}
		recorder = eventBroadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: defaultControllerAgentName})
		go func() {
			<-ctx.Done()
			for _, w := range watches {
				w.Stop()
			}
		}()
	}

	rec := &reconcilerImpl{
		Client:     kubeclient.Get(ctx).CoreV1(),
		Lister:     kafkaSecretInformer.Lister(),
		Recorder:   recorder,
		reconciler: r,
	}
	return controller.NewImpl(rec, logger, defaultQueueName)
}
