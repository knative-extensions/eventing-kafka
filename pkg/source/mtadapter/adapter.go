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
	"encoding/json"
	"math"
	"strconv"
	"sync"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/client-go/kubernetes"

	"knative.dev/eventing/pkg/adapter/v2"
	"knative.dev/eventing/pkg/kncloudevents"
	"knative.dev/eventing/pkg/metrics/source"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/logging"

	"knative.dev/eventing-kafka/pkg/apis/sources/v1beta1"
	"knative.dev/eventing-kafka/pkg/common/scheduler"
	stadapter "knative.dev/eventing-kafka/pkg/source/adapter"
	"knative.dev/eventing-kafka/pkg/source/client"
)

type AdapterConfig struct {
	adapter.EnvConfig

	PodName     string `envconfig:"POD_NAME" required:"true"`
	MPSLimit    int    `envconfig:"VREPLICA_LIMITS_MPS" required:"true"`
	MemoryLimit string `envconfig:"VREPLICA_LIMITS_MEMORY" required:"true"`
}

func NewEnvConfig() adapter.EnvConfigAccessor {
	return new(AdapterConfig)
}

type cancelContext struct {
	fn      context.CancelFunc
	stopped chan bool
}

type Adapter struct {
	config      *AdapterConfig
	logger      *zap.SugaredLogger
	client      cloudevents.Client
	adapterCtor adapter.MessageAdapterConstructor
	kubeClient  kubernetes.Interface
	memLimit    int32

	sourcesMu sync.RWMutex
	sources   map[string]cancelContext
}

var _ adapter.Adapter = (*Adapter)(nil)

func NewAdapter(ctx context.Context, processed adapter.EnvConfigAccessor, ceClient cloudevents.Client) adapter.Adapter {
	return newAdapter(ctx, processed, ceClient, stadapter.NewAdapter)
}

func newAdapter(ctx context.Context, processed adapter.EnvConfigAccessor, ceClient cloudevents.Client, adapterCtor adapter.MessageAdapterConstructor) adapter.Adapter {
	logger := logging.FromContext(ctx)
	config := processed.(*AdapterConfig)

	ml := resource.MustParse(config.MemoryLimit)
	return &Adapter{
		client:      ceClient,
		config:      config,
		logger:      logger,
		adapterCtor: adapterCtor,
		kubeClient:  kubeclient.Get(ctx),
		memLimit:    int32(ml.Value()),
		sourcesMu:   sync.RWMutex{},
		sources:     make(map[string]cancelContext),
	}
}

func (a *Adapter) Start(ctx context.Context) error {
	<-ctx.Done()
	a.logger.Info("Shutting down...")
	return nil
}

// Implements MTAdapter

func (a *Adapter) Update(ctx context.Context, obj *v1beta1.KafkaSource) error {
	a.sourcesMu.Lock()
	defer a.sourcesMu.Unlock()

	key := obj.Namespace + "/" + obj.Name

	logger := a.logger.With("key", key)
	logger.Info("updating source")

	cancel, ok := a.sources[key]

	if ok {
		// TODO: do not stop if the only thing that changes is the number of vreplicas
		logger.Info("stopping adapter")
		cancel.fn()

		// Wait for the adapter to stop
		<-cancel.stopped

		// Nothing to stop anymore
		delete(a.sources, key)
	}

	placement := scheduler.GetPlacementForPod(obj.GetPlacements(), a.config.PodName)
	if placement == nil || placement.VReplicas == 0 {
		// this pod does not handle this source. Skipping
		logger.Info("no replicas assigned to this source. skipping")
		return nil
	}

	kafkaEnvConfig, err := client.NewEnvConfigFromSpec(ctx, a.kubeClient, obj)
	if err != nil {
		return err
	}

	// Enforce memory limits
	if a.memLimit > 0 {
		// TODO: periodically enforce limits as the number of partitions can dynamically change
		fetchSizePerVReplica, err := a.partitionFetchSize(ctx, logger, &kafkaEnvConfig, obj.Spec.Topics, scheduler.GetPodCount(obj.Status.Placement))
		if err != nil {
			return err
		}
		fetchSize := fetchSizePerVReplica * int(placement.VReplicas)

		// Must handle at least 64k messages to the compliant with the CloudEvent spec
		maxFetchSize := fetchSize
		if fetchSize < 64*1024 {
			maxFetchSize = 64 * 1024
		}
		a.logger.Infow("setting partition fetch sizes", zap.Int("min", fetchSize), zap.Int("default", fetchSize), zap.Int("max", maxFetchSize))

		// TODO: find a better way to interact with the ST adapter.
		bufferSizeStr := strconv.Itoa(fetchSize)
		min := `\n    Min: ` + bufferSizeStr
		def := `\n    Default: ` + bufferSizeStr
		max := `\n    Max: ` + strconv.Itoa(maxFetchSize)
		kafkaEnvConfig.KafkaConfigJson = `{"SaramaYamlString": "Consumer:\n  Fetch:` + min + def + max + `"}`
	}

	config := stadapter.AdapterConfig{
		EnvConfig: adapter.EnvConfig{
			Component: "kafkasource",
			Namespace: obj.Namespace,
		},
		KafkaEnvConfig:       kafkaEnvConfig,
		Topics:               obj.Spec.Topics,
		ConsumerGroup:        obj.Spec.ConsumerGroup,
		Name:                 obj.Name,
		DisableControlServer: true,
	}

	if val, ok := obj.GetLabels()[v1beta1.KafkaKeyTypeLabel]; ok {
		config.KeyType = val
	}

	if obj.Spec.CloudEventOverrides != nil {
		// Cannot fail here.
		ceJson, _ := json.Marshal(obj.Spec.CloudEventOverrides)
		config.CEOverrides = string(ceJson)
	}

	reporter, err := source.NewStatsReporter()
	if err != nil {
		a.logger.Error("error building statsreporter", zap.Error(err))
		return err
	}

	httpBindingsSender, err := kncloudevents.NewHTTPMessageSenderWithTarget(obj.Status.SinkURI.String())
	if err != nil {
		a.logger.Errorw("error building cloud event client", zap.Error(err))
		return err
	}

	adapter := a.adapterCtor(ctx, &config, httpBindingsSender, reporter)

	// TODO: define Limit interface.
	if sta, ok := adapter.(*stadapter.Adapter); ok {
		sta.SetRateLimits(rate.Limit(a.config.MPSLimit*int(placement.VReplicas)), 2*a.config.MPSLimit*int(placement.VReplicas))
	}

	ctx, cancelFn := context.WithCancel(ctx)

	cancel = cancelContext{
		fn:      cancelFn,
		stopped: make(chan bool),
	}

	a.sources[key] = cancel

	go func(ctx context.Context) {
		err := adapter.Start(ctx)
		if err != nil {
			a.logger.Errorw("adapter failed to start", zap.Error(err))
		}
		cancel.stopped <- true
	}(ctx)

	a.logger.Infow("source added", "name", obj.Name)
	return nil
}

func (a *Adapter) Remove(obj *v1beta1.KafkaSource) {
	a.sourcesMu.Lock()
	defer a.sourcesMu.Unlock()
	a.logger.Infow("removing source", "name", obj.Name)

	key := obj.Namespace + "/" + obj.Name

	cancel, ok := a.sources[key]

	if !ok {
		a.logger.Infow("source was not running. removed.", "name", obj.Name)
		return
	}

	cancel.fn()
	<-cancel.stopped

	delete(a.sources, key)

	a.logger.Infow("source removed", "name", obj.Name, "remaining", len(a.sources))
}

// partitionFetchSize determines what should be the default fetch size (in bytes)
// so that the st adapter memory consumption does not exceed
// the allocated memory per vreplica (see MemoryLimit).
// Account for pod (consumer) partial outage by reducing the
// partition buffer size
func (a *Adapter) partitionFetchSize(ctx context.Context,
	logger *zap.SugaredLogger,
	kafkaEnvConfig *client.KafkaEnvConfig,
	topics []string,
	podCount int) (int, error) {

	// Compute the number of partitions handled by this source
	// TODO: periodically check for # of resources. Need control-protocol.
	adminClient, err := client.MakeAdminClient(ctx, kafkaEnvConfig)
	if err != nil {
		logger.Errorw("cannot create admin client", zap.Error(err))
		return 0, err
	}

	metas, err := adminClient.DescribeTopics(topics)
	if err != nil {
		logger.Errorw("cannot describe topics", zap.Error(err))
		return 0, err
	}

	totalPartitions := 0
	for _, meta := range metas {
		totalPartitions += len(meta.Partitions)
	}
	adminClient.Close()

	partitionsPerPod := int(math.Ceil(float64(totalPartitions) / float64(podCount)))

	// Ideally, partitions are evenly spread across Kafka consumers.
	// However, due to rebalancing or consumer (un)availability, a consumer
	// might have to handle more partitions than expected.
	// For now, account for 1 unavailable consumer.
	handledPartitions := 2 * partitionsPerPod
	if podCount < 3 {
		handledPartitions = totalPartitions
	}

	logger.Infow("partition count",
		zap.Int("total", totalPartitions),
		zap.Int("averagePerPod", partitionsPerPod),
		zap.Int("handled", handledPartitions))

	// A partition consumes about 2 * fetch partition size
	// Once by FetchResponse blocks and a second time when those blocks are converted to messages
	// see https://github.com/Shopify/sarama/blob/83d633e6e4f71b402df5e9c53ad5c1c334b7065d/consumer.go#L649
	return int(math.Floor(float64(a.memLimit) / float64(handledPartitions) / 2.0)), nil
}
