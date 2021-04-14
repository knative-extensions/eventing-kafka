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
	"math"
	"strconv"
	"sync"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"knative.dev/eventing/pkg/adapter/v2"
	"knative.dev/eventing/pkg/kncloudevents"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/logging"
	pkgsource "knative.dev/pkg/source"

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

type Adapter struct {
	config      *AdapterConfig
	logger      *zap.SugaredLogger
	client      cloudevents.Client
	adapterCtor adapter.MessageAdapterConstructor
	kubeClient  kubernetes.Interface
	memLimit    int64

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
	ml := resource.MustParse(config.MemoryLimit)

	return &Adapter{
		client:      ceClient,
		config:      config,
		logger:      logger,
		adapterCtor: adapterCtor,
		kubeClient:  kubeclient.Get(ctx),
		memLimit:    ml.Value(),
		sourcesMu:   sync.RWMutex{},
		sources:     make(map[string]context.CancelFunc),
	}
}

func (a *Adapter) Start(ctx context.Context) error {
	<-ctx.Done()
	a.logger.Info("Shutting down...")
	return nil
}

// bufferSize returns the maximum fetch buffer size (in bytes)
// so that the st adapter memory consumption does not exceed
// the allocated memory per vreplica (see MemoryLimit).
// Account pod (consumer) partial outage.
func (a *Adapter) bufferSize(ctx context.Context,
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

	// A partition consumes about 2 * fetch buffer size.
	return int(math.Floor(float64(a.memLimit) / float64(handledPartitions) / 2.0)), nil
}

// Implements MTAdapter

func (a *Adapter) Update(ctx context.Context, obj *v1beta1.KafkaSource) {
	a.sourcesMu.Lock()
	defer a.sourcesMu.Unlock()

	key := obj.Namespace + "/" + obj.Name

	logger := a.logger.With("key", key)
	logger.Info("updating source")

	cancel, ok := a.sources[key]

	if ok {
		// TODO: do not stop if the only thing that changes is the number of vreplicas
		logger.Info("stopping adapter")
		cancel()
	}

	placement := scheduler.GetPlacementForPod(obj.GetPlacements(), a.config.PodName)
	if placement == nil || placement.VReplicas == 0 {
		// this pod does not handle this source. Skipping
		logger.Info("no replicas assigned to this source. skipping")
		return
	}

	kafkaEnvConfig := client.KafkaEnvConfig{
		BootstrapServers: obj.Spec.BootstrapServers,
		Net: client.AdapterNet{
			SASL: client.AdapterSASL{
				Enable:   obj.Spec.Net.SASL.Enable,
				User:     a.ResolveSecret(ctx, obj.Namespace, obj.Spec.Net.SASL.User.SecretKeyRef),
				Password: a.ResolveSecret(ctx, obj.Namespace, obj.Spec.Net.SASL.Password.SecretKeyRef),
				Type:     a.ResolveSecret(ctx, obj.Namespace, obj.Spec.Net.SASL.Type.SecretKeyRef),
			},
			TLS: client.AdapterTLS{
				Enable: obj.Spec.Net.TLS.Enable,
				Cert:   a.ResolveSecret(ctx, obj.Namespace, obj.Spec.Net.TLS.Cert.SecretKeyRef),
				Key:    a.ResolveSecret(ctx, obj.Namespace, obj.Spec.Net.TLS.Key.SecretKeyRef),
				CACert: a.ResolveSecret(ctx, obj.Namespace, obj.Spec.Net.TLS.CACert.SecretKeyRef),
			},
		},
	}

	// Enforce memory limits
	if a.memLimit > 0 {
		// TODO: periodically enforce limits as the number of partitions can dynamically change
		bufferSizePerVReplica, err := a.bufferSize(ctx, logger, &kafkaEnvConfig, obj.Spec.Topics, scheduler.GetPodCount(obj.Status.Placement))
		if err != nil {
			return
		}
		bufferSize := bufferSizePerVReplica * int(placement.VReplicas)
		a.logger.Infow("setting fetch buffer size", zap.Int("size", bufferSize))

		// Nasty.
		bufferSizeStr := strconv.Itoa(bufferSize)
		min := `\n    Min: ` + bufferSizeStr
		def := `\n    Default: ` + bufferSizeStr
		max := `\n    Max: ` + bufferSizeStr
		kafkaEnvConfig.KafkaConfigJson = `{"sarama": "Consumer:\n  Fetch:` + min + def + max + `"}`
	}

	config := stadapter.AdapterConfig{
		EnvConfig: adapter.EnvConfig{
			Component: "kafkasource",
			Namespace: obj.Namespace,
		},
		KafkaEnvConfig: kafkaEnvConfig,
		Topics:         obj.Spec.Topics,
		ConsumerGroup:  obj.Spec.ConsumerGroup,
		Name:           obj.Name,
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
		a.logger.Fatalw("error building cloud event client", zap.Error(err))
	}

	adapter := a.adapterCtor(ctx, &config, httpBindingsSender, reporter)

	// TODO: define Limit interface.
	if sta, ok := adapter.(*stadapter.Adapter); ok {
		sta.SetRateLimits(rate.Limit(a.config.MPSLimit*int(placement.VReplicas)), 2*a.config.MPSLimit*int(placement.VReplicas))
	}

	ctx, cancelFn := context.WithCancel(ctx)
	go func(ctx context.Context) {
		err := adapter.Start(ctx)
		if err != nil {
			a.logger.Errorw("adapter failed to start", zap.Error(err))
		}
	}(ctx)

	a.sources[key] = cancelFn
	a.logger.Infow("source added", "name", obj.Name)
}

func (a *Adapter) Remove(obj *v1beta1.KafkaSource) {
	a.sourcesMu.Lock()
	defer a.sourcesMu.Unlock()
	a.logger.Infow("removing source", "name", obj.Name)

	key := obj.Namespace + "/" + obj.Name

	cancel, ok := a.sources[key]

	if !ok {
		a.logger.Infow("source not found", "name", obj.Name)
		return
	}

	cancel()

	delete(a.sources, key)
	a.logger.Infow("source removed", "name", obj.Name, "count", len(a.sources))
}

// ResolveSecret resolves the secret reference
func (a *Adapter) ResolveSecret(ctx context.Context, ns string, ref *corev1.SecretKeySelector) string {
	if ref == nil {
		return ""
	}
	secret, err := a.kubeClient.CoreV1().Secrets(ns).Get(ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
		a.logger.Fatalw("failed to read secret", zap.String("secretname", ref.Name), zap.Error(err))
		return ""
	}

	return string(secret.Data[ref.Key])
}
