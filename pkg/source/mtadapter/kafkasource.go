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

package mtadapter

import (
	"context"

	"go.uber.org/zap"
	"knative.dev/pkg/kmeta"
	"knative.dev/pkg/reconciler"

	"knative.dev/eventing-kafka/pkg/apis/sources/v1beta1"
	kafkasourcereconciler "knative.dev/eventing-kafka/pkg/client/injection/reconciler/sources/v1beta1/kafkasource"
)

// Reconciler updates the internal Adapter cache kafkaSources
type Reconciler struct {
	mtadapter MTAdapter
	logger    *zap.SugaredLogger
}

// Check that our Reconciler implements ReconcileKind.
var _ kafkasourcereconciler.Interface = (*Reconciler)(nil)

func (r *Reconciler) ReconcileKind(ctx context.Context, source *v1beta1.KafkaSource) reconciler.Event {
	if !source.Status.IsReady() {
		r.logger.Warn("warning: KafkaSource is not ready")
		return nil
	}

	// Update the adapter state
	return r.mtadapter.Update(ctx, source)
}

func (r *Reconciler) deleteFunc(obj interface{}) {
	if obj == nil {
		return
	}
	acc, err := kmeta.DeletionHandlingAccessor(obj)
	if err != nil {
		return
	}
	source, ok := acc.(*v1beta1.KafkaSource)
	if !ok || source == nil {
		return
	}
	r.mtadapter.Remove(source)
}
