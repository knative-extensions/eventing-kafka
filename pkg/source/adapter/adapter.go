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

package kafka

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"strings"
	"time"

	protocolkafka "github.com/cloudevents/sdk-go/protocol/kafka_sarama/v2"
	"golang.org/x/time/rate"
	"k8s.io/apimachinery/pkg/types"

	"knative.dev/eventing-kafka/pkg/common/tracing"

	"github.com/Shopify/sarama"
	"go.uber.org/zap"

	"knative.dev/eventing/pkg/metrics/source"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	"knative.dev/pkg/logging"

	"knative.dev/eventing/pkg/adapter/v2"
	"knative.dev/eventing/pkg/auth"
	"knative.dev/eventing/pkg/kncloudevents"

	"knative.dev/eventing-kafka/pkg/common/consumer"
	"knative.dev/eventing-kafka/pkg/source/client"
)

const (
	resourceGroup = "kafkasources.sources.knative.dev"
)

type AdapterConfig struct {
	adapter.EnvConfig
	client.KafkaEnvConfig

	Topics        []string `envconfig:"KAFKA_TOPICS" required:"true"`
	ConsumerGroup string   `envconfig:"KAFKA_CONSUMER_GROUP" required:"true"`
	Name          string   `envconfig:"NAME" required:"true"`
	KeyType       string   `envconfig:"KEY_TYPE" required:"false"`

	// Turn off the control server.
	DisableControlServer bool
}

func NewEnvConfig() adapter.EnvConfigAccessor {
	return &AdapterConfig{}
}

type Adapter struct {
	config       *AdapterConfig
	saramaConfig *sarama.Config

	sink          duckv1.Addressable
	reporter      source.StatsReporter
	logger        *zap.SugaredLogger
	keyTypeMapper func([]byte) interface{}
	rateLimiter   *rate.Limiter
	extensions    map[string]string
	dispatcher    *kncloudevents.Dispatcher
}

var (
	_           adapter.MessageAdapter                   = (*Adapter)(nil)
	_           consumer.KafkaConsumerHandler            = (*Adapter)(nil)
	_           consumer.SaramaConsumerLifecycleListener = (*Adapter)(nil)
	_           adapter.MessageAdapterConstructor        = NewAdapter
	retryConfig                                          = defaultRetryConfig()
)

func NewAdapter(ctx context.Context, processed adapter.EnvConfigAccessor, sink duckv1.Addressable, reporter source.StatsReporter) adapter.MessageAdapter {
	logger := logging.FromContext(ctx)
	config := processed.(*AdapterConfig)

	oidcTokenProvider := auth.NewOIDCTokenProvider(ctx)

	return &Adapter{
		config:        config,
		sink:          sink,
		reporter:      reporter,
		logger:        logger,
		keyTypeMapper: getKeyTypeMapper(config.KeyType),
		dispatcher:    kncloudevents.NewDispatcher(oidcTokenProvider),
	}
}
func (a *Adapter) GetConsumerGroup() string {
	return a.config.ConsumerGroup
}

func (a *Adapter) Start(ctx context.Context) (err error) {
	a.logger.Infow("Starting with config: ",
		zap.String("Topics", strings.Join(a.config.Topics, ",")),
		zap.String("ConsumerGroup", a.config.ConsumerGroup),
		zap.String("SinkURI", a.config.Sink),
		zap.String("Name", a.config.Name),
		zap.String("Namespace", a.config.Namespace),
	)

	// Preprocess ceOverrides
	if a.config.CEOverrides != "" {
		ceOverrides, err := a.config.GetCloudEventOverrides()
		if err != nil {
			return err
		}
		if len(ceOverrides.Extensions) > 0 {
			a.extensions = ceOverrides.Extensions
		}
	}

	// init consumer group
	addrs, config, err := client.NewConfigWithEnv(context.Background(), &a.config.KafkaEnvConfig)
	if err != nil {
		return fmt.Errorf("failed to create the config: %w", err)
	}
	a.saramaConfig = config

	options := []consumer.SaramaConsumerHandlerOption{consumer.WithSaramaConsumerLifecycleListener(a)}
	consumerGroupFactory := consumer.NewConsumerGroupFactory(addrs, config, &consumer.NoopConsumerGroupOffsetsChecker{}, func(ref types.NamespacedName) {})
	group, err := consumerGroupFactory.StartConsumerGroup(
		ctx,
		a.config.ConsumerGroup,
		a.config.Topics,
		a,
		types.NamespacedName{Namespace: a.config.Namespace, Name: a.config.Name},
		options...,
	)
	if err != nil {
		return fmt.Errorf("failed to start consumer group: %w", err)
	}
	defer func() {
		err := group.Close()
		if err != nil {
			a.logger.Errorw("Failed to close consumer group", zap.Error(err))
		}
	}()

	// Track errors
	go func() {
		for err := range group.Errors() {
			a.logger.Errorw("Error while consuming messages", zap.Error(err))
		}
	}()

	<-ctx.Done()
	a.logger.Info("Shutting down...")
	return nil
}

func (a *Adapter) SetReady(int32, bool) {}

func (a *Adapter) Handle(ctx context.Context, msg *sarama.ConsumerMessage) (bool, error) {
	if a.rateLimiter != nil {
		a.rateLimiter.Wait(ctx)
	}

	message := protocolkafka.NewMessageFromConsumerMessage(msg)
	ctx, span := tracing.StartTraceFromMessage(a.logger, ctx, message, "kafka-source-"+msg.Topic)
	defer span.End()

	event, err := a.ConsumerMessageToCloudEvent(ctx, msg)
	if err != nil {
		return false, fmt.Errorf("failed to get cloud event from consumer message: %w", err)
	}

	dispatchInfo, err := a.dispatcher.SendEvent(ctx, *event, a.sink,
		kncloudevents.WithRetryConfig(retryConfig),
		kncloudevents.WithTransformers(extensionAsTransformer(a.extensions)))
	if err != nil {
		a.logger.Debug("Error while sending the message", zap.Error(err))
		return false, err // Error while sending, don't commit offset
	}

	if dispatchInfo.ResponseCode/100 != 2 {
		a.logger.Debug("Unexpected status code", zap.Int("status code", dispatchInfo.ResponseCode))
		return false, fmt.Errorf("%d %s", dispatchInfo.ResponseCode, http.StatusText(dispatchInfo.ResponseCode))
	}

	reportArgs := &source.ReportArgs{
		Namespace:     a.config.Namespace,
		Name:          a.config.Name,
		ResourceGroup: resourceGroup,
	}

	_ = a.reporter.ReportEventCount(reportArgs, dispatchInfo.ResponseCode)
	return true, nil
}

// SetRateLimiter sets the global consumer rate limiter
func (a *Adapter) SetRateLimits(r rate.Limit, b int) {
	a.rateLimiter = rate.NewLimiter(r, b)
}

func (a *Adapter) Setup(sess sarama.ConsumerGroupSession) {
}

func (a *Adapter) Cleanup(sess sarama.ConsumerGroupSession) {
}

// Default retry configuration, 5 retries, exponential backoff with 50ms delay
func defaultRetryConfig() *kncloudevents.RetryConfig {
	return &kncloudevents.RetryConfig{
		CheckRetry: kncloudevents.SelectiveRetry,
		RetryMax:   5,
		Backoff: func(attemptNum int, resp *http.Response) time.Duration {
			return 50 * time.Millisecond * time.Duration(math.Exp2(float64(attemptNum)))
		},
	}
}
