package kafkasecretinjection

import (
	context "context"
	fmt "fmt"
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/kafkasecretinformer"
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/util"
	reflect "reflect"
	strings "strings"

	corev1 "k8s.io/api/core/v1"
	labels "k8s.io/apimachinery/pkg/labels"
	types "k8s.io/apimachinery/pkg/types"
	watch "k8s.io/apimachinery/pkg/watch"
	scheme "k8s.io/client-go/kubernetes/scheme"
	v1 "k8s.io/client-go/kubernetes/typed/core/v1"
	record "k8s.io/client-go/tools/record"
	versionedscheme "knative.dev/eventing-contrib/kafka/channel/pkg/client/clientset/versioned/scheme"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	controller "knative.dev/pkg/controller"
	logging "knative.dev/pkg/logging"
	reconciler "knative.dev/pkg/reconciler"
)

// Code manually created based on generated implementation in eventing-contrib/kafka - SEE README !!!

const (
	defaultControllerAgentName = "kafkasecret-controller"
)

var (
	// Need Prefix For Valid Finalizer On Native K8S Resources (Secrets)
	defaultFinalizerName = util.KubernetesResourceFinalizerName("kafkasecrets.eventing-kafka.knative.dev")
)

// NewImpl returns a controller.Impl that handles queuing and feeding work from
// the queue through an implementation of controller.Reconciler, delegating to
// the provided Interface and optional Finalizer methods. OptionsFn is used to return
// controller.Options to be used but the internal reconciler.
func NewImpl(ctx context.Context, r Interface, optionsFns ...controller.OptionsFn) *controller.Impl {
	logger := logging.FromContext(ctx)

	// Check the options function input. It should be 0 or 1.
	if len(optionsFns) > 1 {
		logger.Fatalf("up to one options function is supported, found %d", len(optionsFns))
	}

	kafkaSecretInformer := kafkasecretinformer.Get(ctx)

	lister := kafkaSecretInformer.Lister()

	rec := &reconcilerImpl{
		LeaderAwareFuncs: reconciler.LeaderAwareFuncs{
			PromoteFunc: func(bkt reconciler.Bucket, enq func(reconciler.Bucket, types.NamespacedName)) error {
				all, err := lister.List(labels.Everything())
				if err != nil {
					return err
				}
				for _, elt := range all {
					// TODO: Consider letting users specify a filter in options.
					enq(bkt, types.NamespacedName{
						Namespace: elt.GetNamespace(),
						Name:      elt.GetName(),
					})
				}
				return nil
			},
		},
		Client:        kubeclient.Get(ctx).CoreV1(),
		Lister:        lister,
		reconciler:    r,
		finalizerName: defaultFinalizerName,
	}

	t := reflect.TypeOf(r).Elem()
	queueName := fmt.Sprintf("%s.%s", strings.ReplaceAll(t.PkgPath(), "/", "-"), t.Name())

	impl := controller.NewImpl(rec, logger, queueName)
	agentName := defaultControllerAgentName

	// Pass impl to the options. Save any optional results.
	for _, fn := range optionsFns {
		opts := fn(impl)
		if opts.ConfigStore != nil {
			rec.configStore = opts.ConfigStore
		}
		if opts.FinalizerName != "" {
			rec.finalizerName = opts.FinalizerName
		}
		if opts.AgentName != "" {
			agentName = opts.AgentName
		}
		if opts.SkipStatusUpdates {
			rec.skipStatusUpdates = true
		}
	}

	rec.Recorder = createRecorder(ctx, agentName)

	return impl
}

func createRecorder(ctx context.Context, agentName string) record.EventRecorder {
	logger := logging.FromContext(ctx)

	recorder := controller.GetEventRecorder(ctx)
	if recorder == nil {
		// Create event broadcaster
		logger.Debug("Creating event broadcaster")
		eventBroadcaster := record.NewBroadcaster()
		watches := []watch.Interface{
			eventBroadcaster.StartLogging(logger.Named("event-broadcaster").Infof),
			eventBroadcaster.StartRecordingToSink(
				&v1.EventSinkImpl{Interface: kubeclient.Get(ctx).CoreV1().Events("")}),
		}
		recorder = eventBroadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: agentName})
		go func() {
			<-ctx.Done()
			for _, w := range watches {
				w.Stop()
			}
		}()
	}

	return recorder
}

func init() {
	versionedscheme.AddToScheme(scheme.Scheme)
}
