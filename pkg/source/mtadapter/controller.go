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

	"k8s.io/client-go/tools/cache"

	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"

	"knative.dev/eventing/pkg/adapter/v2"

	"knative.dev/eventing-kafka/pkg/apis/sources/v1beta1"
	kakfasourceinformer "knative.dev/eventing-kafka/pkg/client/injection/informers/sources/v1beta1/kafkasource"
	kafkasourcereconciler "knative.dev/eventing-kafka/pkg/client/injection/reconciler/sources/v1beta1/kafkasource"
)

// MTAdapter is the interface the multi-tenant KafkaSource adapter must implement
type MTAdapter interface {
	// Update is called when the source is ready and when the specification and/or status has changed.
	Update(ctx context.Context, source *v1beta1.KafkaSource) error

	// Remove is called when the source has been deleted.
	Remove(source *v1beta1.KafkaSource)
}

// NewController initializes the controller and
// registers event handlers to enqueue events.
func NewController(ctx context.Context, adapter adapter.Adapter) *controller.Impl {
	mtadapter, ok := adapter.(*Adapter)
	logger := logging.FromContext(ctx)
	if !ok {
		logger.Fatal("Multi-tenant adapters must implement the MTAdapter interface")
	}

	r := &Reconciler{
		mtadapter: mtadapter,
		logger:    logger,
	}

	impl := kafkasourcereconciler.NewImpl(ctx, r, func(impl *controller.Impl) controller.Options {
		return controller.Options{
			SkipStatusUpdates: true,
		}
	})

	logging.FromContext(ctx).Info("Setting up event handlers")
	kafkasourceInformer := kakfasourceinformer.Get(ctx)
	kafkasourceInformer.Informer().AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    impl.Enqueue,
			UpdateFunc: controller.PassNew(impl.Enqueue),
			DeleteFunc: r.deleteFunc,
		})

	return impl
}
