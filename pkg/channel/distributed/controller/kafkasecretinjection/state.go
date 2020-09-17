package kafkasecretinjection

import (
	fmt "fmt"
	v1 "k8s.io/api/core/v1"

	types "k8s.io/apimachinery/pkg/types"
	cache "k8s.io/client-go/tools/cache"
	reconciler "knative.dev/pkg/reconciler"
)

// Code manually created based on generated implementation in eventing-contrib/kafka - SEE README !!!

// state is used to track the state of a reconciler in a single run.
type state struct {
	// Key is the original reconciliation key from the queue.
	key string
	// Namespace is the namespace split from the reconciliation key.
	namespace string
	// Namespace is the name split from the reconciliation key.
	name string
	// reconciler is the reconciler.
	reconciler Interface
	// rof is the read only interface cast of the reconciler.
	roi ReadOnlyInterface
	// IsROI (Read Only Interface) the reconciler only observes reconciliation.
	isROI bool
	// rof is the read only finalizer cast of the reconciler.
	rof ReadOnlyFinalizer
	// IsROF (Read Only Finalizer) the reconciler only observes finalize.
	isROF bool
	// IsLeader the instance of the reconciler is the elected leader.
	isLeader bool
}

func newState(key string, r *reconcilerImpl) (*state, error) {
	// Convert the namespace/name string into a distinct namespace and name
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return nil, fmt.Errorf("invalid resource key: %s", key)
	}

	roi, isROI := r.reconciler.(ReadOnlyInterface)
	rof, isROF := r.reconciler.(ReadOnlyFinalizer)

	isLeader := r.IsLeaderFor(types.NamespacedName{
		Namespace: namespace,
		Name:      name,
	})

	return &state{
		key:        key,
		namespace:  namespace,
		name:       name,
		reconciler: r.reconciler,
		roi:        roi,
		isROI:      isROI,
		rof:        rof,
		isROF:      isROF,
		isLeader:   isLeader,
	}, nil
}

// isNotLeaderNorObserver checks to see if this reconciler with the current
// state is enabled to do any work or not.
// isNotLeaderNorObserver returns true when there is no work possible for the
// reconciler.
func (s *state) isNotLeaderNorObserver() bool {
	if !s.isLeader && !s.isROI && !s.isROF {
		// If we are not the leader, and we don't implement either ReadOnly
		// interface, then take a fast-path out.
		return true
	}
	return false
}

func (s *state) reconcileMethodFor(o *v1.Secret) (string, doReconcile) {
	if o.GetDeletionTimestamp().IsZero() {
		if s.isLeader {
			return reconciler.DoReconcileKind, s.reconciler.ReconcileKind
		} else if s.isROI {
			return reconciler.DoObserveKind, s.roi.ObserveKind
		}
	} else if fin, ok := s.reconciler.(Finalizer); s.isLeader && ok {
		return reconciler.DoFinalizeKind, fin.FinalizeKind
	} else if !s.isLeader && s.isROF {
		return reconciler.DoObserveFinalizeKind, s.rof.ObserveFinalizeKind
	}
	return "unknown", nil
}
