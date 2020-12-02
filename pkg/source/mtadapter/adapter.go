/*
Copyright 2019 The Knative Authors

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
	"sync"

	"golang.org/x/time/rate"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"go.uber.org/zap"

	"knative.dev/pkg/logging"
	pkgsource "knative.dev/pkg/source"

	"knative.dev/eventing/pkg/adapter/v2"
	"knative.dev/eventing/pkg/kncloudevents"

	"knative.dev/eventing-kafka/pkg/apis/sources/v1beta1"
	"knative.dev/eventing-kafka/pkg/common/scheduler"
	"knative.dev/eventing-kafka/pkg/source"
	stadapter "knative.dev/eventing-kafka/pkg/source/adapter"
)

type AdapterConfig struct {
	adapter.EnvConfig

	PodName string `envconfig:"POD_NAME" required:"true"`
}

func NewEnvConfig() adapter.EnvConfigAccessor {
	return new(AdapterConfig)
}

type Adapter struct {
	config      *AdapterConfig
	logger      *zap.SugaredLogger
	client      cloudevents.Client
	adapterCtor adapter.MessageAdapterConstructor

	sourcesMu sync.RWMutex
	sources   map[string]context.CancelFunc
}

var _ adapter.Adapter = (*Adapter)(nil)

func NewAdapter(ctx context.Context, processed adapter.EnvConfigAccessor, ceClient cloudevents.Client) adapter.Adapter {
	return newAdapter(ctx, processed, ceClient, stadapter.NewAdapter)
}

func newAdapter(ctx context.Context, processed adapter.EnvConfigAccessor, ceClient cloudevents.Client, adapterCtor adapter.MessageAdapterConstructor) adapter.Adapter {
	logger := logging.FromContext(ctx)
	config := processed.(*AdapterConfig)

	return &Adapter{
		client:      ceClient,
		config:      config,
		logger:      logger,
		adapterCtor: adapterCtor,
		sourcesMu:   sync.RWMutex{},
		sources:     make(map[string]context.CancelFunc),
	}
}

func (a *Adapter) Start(ctx context.Context) error {
	<-ctx.Done()
	a.logger.Info("Shutting down...")
	return nil
}

// Implements MTAdapter

func (a *Adapter) Update(ctx context.Context, obj *v1beta1.KafkaSource) {
	key := obj.Namespace + "/" + obj.Name

	a.sourcesMu.RLock()
	cancel, ok := a.sources[key]
	a.sourcesMu.RUnlock()

	if ok {
		// TODO: do not stop if the only thing that changes is the number of vreplicas
		a.logger.Info("stopping adapter", zap.String("key", key))
		cancel()
	}

	placement := scheduler.GetPlacementForPod(obj.GetPlacements(), a.config.PodName)
	if placement == nil || placement.VReplicas == 0 {
		// this pod does not handle this source. Skipping
		a.logger.Infow("no replicas assigned to this pod. skipping", zap.String("key", key))
		return
	}

	config := stadapter.AdapterConfig{
		EnvConfig: adapter.EnvConfig{
			Component: "kafkasource",
		},
		KafkaEnvConfig: source.KafkaEnvConfig{
			BootstrapServers: obj.Spec.BootstrapServers,
		},
		Topics:        obj.Spec.Topics,
		ConsumerGroup: obj.Spec.ConsumerGroup,
		Name:          obj.Name,
	}

	if val, ok := obj.GetLabels()[v1beta1.KafkaKeyTypeLabel]; ok {
		config.KeyType = val
	}

	reporter, err := pkgsource.NewStatsReporter()
	if err != nil {
		a.logger.Error("error building statsreporter", zap.Error(err))
	}

	httpBindingsSender, err := kncloudevents.NewHTTPMessageSenderWithTarget(obj.Status.SinkURI.String())
	if err != nil {
		a.logger.Fatal("error building cloud event client", zap.Error(err))
	}

	adapter := a.adapterCtor(ctx, &config, httpBindingsSender, reporter)

	if a, ok := adapter.(*stadapter.Adapter); ok {
		// TODO: configurable
		a.SetRateLimits(rate.Limit(10.0*placement.VReplicas), 20*int(placement.VReplicas))
	}

	ctx, cancelFn := context.WithCancel(ctx)
	go func(ctx context.Context) {
		err := adapter.Start(ctx)
		if err != nil {
			a.logger.Error("adapter failed to start", zap.Error(err))
		}

	}(ctx)

	a.sourcesMu.Lock()
	a.sources[key] = cancelFn
	a.sourcesMu.Unlock()
}

func (a *Adapter) Remove(ctx context.Context, obj *v1beta1.KafkaSource) {
	key := obj.Namespace + "/" + obj.Name

	a.sourcesMu.RLock()
	cancel, ok := a.sources[key]
	a.sourcesMu.RUnlock()

	if !ok {
		return
	}

	cancel()

	a.sourcesMu.Lock()
	delete(a.sources, key)
	a.sourcesMu.Unlock()
}
