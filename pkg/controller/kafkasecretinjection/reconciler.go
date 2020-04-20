package kafkasecretinjection

import (
	"context"
	"encoding/json"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	corev1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/reconciler"
)

// Code manually created based on generated implementation in eventing-contrib/kafka - SEE README !!!

// Interface defines the strongly typed interfaces to be implemented by a
// controller reconciling corev1.Secret.
type Interface interface {
	// ReconcileKind implements custom logic to reconcile corev1.Secret. Any changes
	// to the objects .Status or .Finalizers will be propagated to the stored
	// object. It is recommended that implementors do not call any update calls
	// for the Kind inside of ReconcileKind, it is the responsibility of the calling
	// controller to propagate those properties. The resource passed to ReconcileKind
	// will always have an empty deletion timestamp.
	ReconcileKind(ctx context.Context, o *corev1.Secret) reconciler.Event
}

// Finalizer defines the strongly typed interfaces to be implemented by a
// controller finalizing corev1.Secret.
type Finalizer interface {
	// FinalizeKind implements custom logic to finalize corev1.Secret. Any changes
	// to the objects .Status or .Finalizers will be ignored. Returning a nil or
	// Normal type reconciler.Event will allow the finalizer to be deleted on
	// the resource. The resource passed to FinalizeKind will always have a set
	// deletion timestamp.
	FinalizeKind(ctx context.Context, o *corev1.Secret) reconciler.Event
}

// reconcilerImpl implements controller.Reconciler for corev1.Secret resources.
type reconcilerImpl struct {

	// Client is used to write back finalizer updates
	Client corev1client.CoreV1Interface

	// Listers index properties about resources
	Lister corev1listers.SecretLister

	// Recorder is an event recorder for recording Event resources to the
	// Kubernetes API.
	Recorder record.EventRecorder

	// reconciler is the implementation of the business logic of the resource.
	reconciler Interface
}

// Check that our Reconciler implements controller.Reconciler
var _ controller.Reconciler = (*reconcilerImpl)(nil)

func NewReconciler(_ context.Context, _ *zap.SugaredLogger, client corev1client.CoreV1Interface, lister corev1listers.SecretLister, recorder record.EventRecorder, r Interface) controller.Reconciler {
	return &reconcilerImpl{
		Client:     client,
		Lister:     lister,
		Recorder:   recorder,
		reconciler: r,
	}
}

// Reconcile implements controller.Reconciler
func (r *reconcilerImpl) Reconcile(ctx context.Context, key string) error {

	// Get Logger From Context
	logger := logging.FromContext(ctx)

	// Add the recorder to context.
	ctx = controller.WithEventRecorder(ctx, r.Recorder)

	// Convert the namespace/name string into a distinct namespace and name
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		logger.Errorf("invalid resource key: %s", key)
		return nil
	}

	// Get the resource with this namespace/name.
	original, err := r.Lister.Secrets(namespace).Get(name)
	if errors.IsNotFound(err) {
		// The resource may no longer exist, in which case we stop processing.
		logger.Errorf("resource %q no longer exists", key)
		return nil
	} else if err != nil {
		return err
	}

	// Don't modify the informers copy.
	resource := original.DeepCopy()

	var reconcileEvent reconciler.Event
	if resource.GetDeletionTimestamp().IsZero() {
		// Set and update the finalizer on resource if r.reconciler implements Finalizer
		if err := r.setFinalizerIfFinalizer(ctx, resource); err != nil {
			logger.Warnw("Failed to set finalizers", zap.Error(err))
		}

		// Reconcile this copy of the resource and then write back any status
		// updates regardless of whether the reconciliation errored out.
		reconcileEvent = r.reconciler.ReconcileKind(ctx, resource)
	} else if fin, ok := r.reconciler.(Finalizer); ok {
		// For finalizing reconcilers, if this resource being marked for deletion
		// and reconciled cleanly (nil or normal event), remove the finalizer.
		reconcileEvent = fin.FinalizeKind(ctx, resource)
		if err := r.clearFinalizer(ctx, resource, reconcileEvent); err != nil {
			logger.Warnw("Failed to clear finalizers", zap.Error(err))
		}
	}

	// Report the reconciler event, if any.
	if reconcileEvent != nil {
		var event *reconciler.ReconcilerEvent
		if reconciler.EventAs(reconcileEvent, &event) {
			logger.Infow("ReconcileKind returned an event", zap.Any("event", reconcileEvent))
			r.Recorder.Eventf(resource, event.EventType, event.Reason, event.Format, event.Args...)
			return nil
		} else {
			logger.Errorw("ReconcileKind returned an error", zap.Error(reconcileEvent))
			r.Recorder.Event(resource, corev1.EventTypeWarning, "InternalError", reconcileEvent.Error())
			return reconcileEvent
		}
	}
	return nil
}

// updateFinalizersFiltered will update the Finalizers of the resource.
// TODO: this method could be generic and sync all finalizers. For now it only updates defaultFinalizerName.
func (r *reconcilerImpl) updateFinalizersFiltered(_ context.Context, resource *corev1.Secret) error {
	finalizerName := defaultFinalizerName

	actual, err := r.Lister.Secrets(resource.Namespace).Get(resource.Name)
	if err != nil {
		return err
	}

	// Don't modify the informers copy.
	existing := actual.DeepCopy()

	var finalizers []string

	// If there's nothing to update, just return.
	existingFinalizers := sets.NewString(existing.Finalizers...)
	desiredFinalizers := sets.NewString(resource.Finalizers...)

	if desiredFinalizers.Has(finalizerName) {
		if existingFinalizers.Has(finalizerName) {
			// Nothing to do.
			return nil
		}
		// Add the finalizer.
		finalizers = append(existing.Finalizers, finalizerName)
	} else {
		if !existingFinalizers.Has(finalizerName) {
			// Nothing to do.
			return nil
		}
		// Remove the finalizer.
		existingFinalizers.Delete(finalizerName)
		finalizers = existingFinalizers.List()
	}

	mergePatch := map[string]interface{}{
		"metadata": map[string]interface{}{
			"finalizers":      finalizers,
			"resourceVersion": existing.ResourceVersion,
		},
	}

	patch, err := json.Marshal(mergePatch)
	if err != nil {
		return err
	}

	_, err = r.Client.Secrets(resource.Namespace).Patch(resource.Name, types.MergePatchType, patch)
	if err != nil {
		r.Recorder.Eventf(resource, corev1.EventTypeWarning, "FinalizerUpdateFailed",
			"Failed to update finalizers for %q: %v", resource.Name, err)
	} else {
		r.Recorder.Eventf(resource, corev1.EventTypeNormal, "FinalizerUpdate",
			"Updated %q finalizers", resource.GetName())
	}
	return err
}

func (r *reconcilerImpl) setFinalizerIfFinalizer(ctx context.Context, resource *corev1.Secret) error {
	if _, ok := r.reconciler.(Finalizer); !ok {
		return nil
	}

	finalizers := sets.NewString(resource.Finalizers...)

	// If this resource is not being deleted, mark the finalizer.
	if resource.GetDeletionTimestamp().IsZero() {
		finalizers.Insert(defaultFinalizerName)
	}

	resource.Finalizers = finalizers.List()

	// Synchronize the finalizers filtered by defaultFinalizerName.
	return r.updateFinalizersFiltered(ctx, resource)
}

func (r *reconcilerImpl) clearFinalizer(ctx context.Context, resource *corev1.Secret, reconcileEvent reconciler.Event) error {
	if _, ok := r.reconciler.(Finalizer); !ok {
		return nil
	}
	if resource.GetDeletionTimestamp().IsZero() {
		return nil
	}

	finalizers := sets.NewString(resource.Finalizers...)

	if reconcileEvent != nil {
		var event *reconciler.ReconcilerEvent
		if reconciler.EventAs(reconcileEvent, &event) {
			if event.EventType == corev1.EventTypeNormal {
				finalizers.Delete(defaultFinalizerName)
			}
		}
	} else {
		finalizers.Delete(defaultFinalizerName)
	}

	resource.Finalizers = finalizers.List()

	// Synchronize the finalizers filtered by defaultFinalizerName.
	return r.updateFinalizersFiltered(ctx, resource)
}
