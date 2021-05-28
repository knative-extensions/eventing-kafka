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
	"fmt"
	"sync"

	"github.com/Shopify/sarama"
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

	PodName       string `envconfig:"POD_NAME" required:"true"`
	MPSLimit      int    `envconfig:"VREPLICA_LIMITS_MPS" required:"true"`
	MemoryRequest string `envconfig:"REQUESTS_MEMORY" required:"true"`
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

	memoryRequest int64

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

	mr := resource.MustParse(config.MemoryRequest)

	return &Adapter{
		client:        ceClient,
		config:        config,
		logger:        logger,
		adapterCtor:   adapterCtor,
		kubeClient:    kubeclient.Get(ctx),
		memoryRequest: mr.Value(),
		sourcesMu:     sync.RWMutex{},
		sources:       make(map[string]cancelContext),
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
		a.adjustResponseSize()
	}

	placement := scheduler.GetPlacementForPod(obj.GetPlacements(), a.config.PodName)
	if placement == nil || placement.VReplicas == 0 {
		// this pod does not handle this source. Skipping
		logger.Info("no replicas assigned to this source. skipping")
		return nil
	}

	saslUser, err := a.ResolveSecret(ctx, obj.Namespace, obj.Spec.Net.SASL.User.SecretKeyRef)
	if err != nil {
		return err
	}

	saslPassword, err := a.ResolveSecret(ctx, obj.Namespace, obj.Spec.Net.SASL.Password.SecretKeyRef)
	if err != nil {
		return err
	}

	saslType, err := a.ResolveSecret(ctx, obj.Namespace, obj.Spec.Net.SASL.Type.SecretKeyRef)
	if err != nil {
		return err
	}

	tlsCert, err := a.ResolveSecret(ctx, obj.Namespace, obj.Spec.Net.TLS.Cert.SecretKeyRef)
	if err != nil {
		return err
	}

	tlsKey, err := a.ResolveSecret(ctx, obj.Namespace, obj.Spec.Net.TLS.Key.SecretKeyRef)
	if err != nil {
		return err
	}

	tlsCACert, err := a.ResolveSecret(ctx, obj.Namespace, obj.Spec.Net.TLS.CACert.SecretKeyRef)
	if err != nil {
		return err
	}

	kafkaEnvConfig := client.KafkaEnvConfig{
		BootstrapServers: obj.Spec.BootstrapServers,
		Net: client.AdapterNet{
			SASL: client.AdapterSASL{
				Enable:   obj.Spec.Net.SASL.Enable,
				User:     saslUser,
				Password: saslPassword,
				Type:     saslType,
			},
			TLS: client.AdapterTLS{
				Enable: obj.Spec.Net.TLS.Enable,
				Cert:   tlsCert,
				Key:    tlsKey,
				CACert: tlsCACert,
			},
		},
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

	reporter, err := pkgsource.NewStatsReporter()
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

	go func(ctx context.Context) {
		err := adapter.Start(ctx)
		if err != nil {
			a.logger.Errorw("adapter failed to start", zap.Error(err))
		}
		cancel.stopped <- true
	}(ctx)

	a.sources[key] = cancel
	a.adjustResponseSize()

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
	a.adjustResponseSize()

	a.logger.Infow("source removed", "name", obj.Name, "remaining", len(a.sources))
}

// ResolveSecret resolves the secret reference
func (a *Adapter) ResolveSecret(ctx context.Context, ns string, ref *corev1.SecretKeySelector) (string, error) {
	if ref == nil {
		return "", nil
	}
	secret, err := a.kubeClient.CoreV1().Secrets(ns).Get(ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
		a.logger.Errorw("failed to read secret", zap.String("secretname", ref.Name), zap.Error(err))
		return "", err
	}

	if value, ok := secret.Data[ref.Key]; ok && len(value) > 0 {
		return string(value), nil
	}

	a.logger.Errorw("missing secret key or empty secret value", zap.String("secretname", ref.Name), zap.String("secretkey", ref.Key))
	return "", fmt.Errorf("missing secret key or empty secret value (%s/%s)", ref.Name, ref.Key)
}

// adjustResponseSize ensures the sum of all Kafka clients memory usage does not exceed the container memory request.
func (a *Adapter) adjustResponseSize() {
	if a.memoryRequest > 0 {
		maxResponseSize := int32(float64(a.memoryRequest) / float64(len(a.sources)))

		// cap the response size to 100MB.
		if maxResponseSize > 100*1024*1024 {
			maxResponseSize = 100 * 1024 * 1024
		}
		// Check for compliance.
		if maxResponseSize < 64*1024 {
			// Not CloudEvent compliant (https://github.com/cloudevents/spec/blob/v1.0.1/spec.md#size-limits)
			a.logger.Warnw("Kafka response size is lower than 64KB. Increase the pod memory request and/or lower the pod capacity.",
				zap.Int32("responseSize", maxResponseSize))
		}

		sarama.MaxResponseSize = maxResponseSize
		a.logger.Infof("Setting MaxResponseSize to %d bytes", sarama.MaxResponseSize)
	}
}
