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

package statefulset

import (
	"context"
	"math"
	"time"

	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	clientappsv1 "k8s.io/client-go/kubernetes/typed/apps/v1"

	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/logging"
)

type Autoscaler interface {
	Start(ctx context.Context)

	Autoscale(pending int32)
}

type autoscaler struct {
	statefulSetClient clientappsv1.StatefulSetInterface
	statefulSetName   string
	logger            *zap.SugaredLogger
	stateAccessor     stateAccessor
	trigger           chan int32

	// capacity is the total number of virtual replicas available per pod.
	capacity int32

	// refreshPeriod is how often to try attempting scaling down the statefulset
	refreshPeriod time.Duration
}

func NewAutoscaler(ctx context.Context, namespace, name string, stateAccessor stateAccessor, refreshPeriod time.Duration) Autoscaler {
	return &autoscaler{
		logger:            logging.FromContext(ctx),
		statefulSetClient: kubeclient.Get(ctx).AppsV1().StatefulSets(namespace),
		statefulSetName:   name,
		stateAccessor:     stateAccessor,
		trigger:           make(chan int32, 1),
		capacity:          int32(10),
		refreshPeriod:     refreshPeriod,
	}
}

func (a *autoscaler) Start(ctx context.Context) {
	attemptScaleDown := false
	pending := int32(0)
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(a.refreshPeriod):
			attemptScaleDown = true
		case pending = <-a.trigger:
			attemptScaleDown = false
		}

		// Retry a few times, just so that we don't have to wait for the next beat when
		// a transient error occurs
		wait.Poll(500*time.Millisecond, 5*time.Second, func() (bool, error) {
			err := a.doautoscale(ctx, attemptScaleDown, pending)
			return err == nil, nil
		})

		pending = int32(0)
	}
}

func (a *autoscaler) Autoscale(pending int32) {
	a.trigger <- pending
}

func (a *autoscaler) doautoscale(ctx context.Context, attemptScaleDown bool, pending int32) error {
	state, err := a.stateAccessor.State()
	if err != nil {
		a.logger.Info("error while refreshing scheduler state (will retry)", zap.Error(err))
		return err
	}

	scale, err := a.statefulSetClient.GetScale(ctx, a.statefulSetName, metav1.GetOptions{})
	if err != nil {
		// skip a beat
		a.logger.Infow("failed to get scale subresource", zap.Error(err))
		return err
	}

	a.logger.Infow("checking adapter capacity",
		zap.Int32("pending", pending),
		zap.Int32("replicas", scale.Spec.Replicas),
		zap.Int32("last ordinal", state.lastOrdinal))

	newreplicas := state.lastOrdinal + 1 // Ideal number

	// Take into account pending replicas
	if pending > 0 {
		// The number of replicas may be lower than the last ordinal, for instance
		// when the statefulset is manually scaled down. In that case, replicas above
		// scale.Spec.Replicas have not been considered when scheduling vreplicas.
		// Adjust accordindly
		pending -= state.freeCapacity()

		// Still need more?
		if pending > 0 {
			// Make sure to allocate enough pods for holding all pending replicas.
			newreplicas += int32(math.Ceil(float64(pending) / float64(a.capacity)))
		}
	}

	// Make sure to never scale down past the last ordinal
	if newreplicas <= state.lastOrdinal {
		newreplicas = state.lastOrdinal + 1
	}

	// Only scale down if permitted
	if !attemptScaleDown && newreplicas < scale.Spec.Replicas {
		newreplicas = scale.Spec.Replicas
	}

	if newreplicas != scale.Spec.Replicas {
		scale.Spec.Replicas = newreplicas
		a.logger.Infow("updating adapter replicas", zap.Int32("replicas", scale.Spec.Replicas))

		_, err = a.statefulSetClient.UpdateScale(ctx, a.statefulSetName, scale, metav1.UpdateOptions{})
		if err != nil {
			a.logger.Errorw("updating scale subresource failed", zap.Error(err))
			return err
		}
	}

	return nil
}
