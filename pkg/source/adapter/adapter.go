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
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"strings"
	"time"

	"golang.org/x/time/rate"
	"k8s.io/apimachinery/pkg/types"
	ctrl "knative.dev/control-protocol/pkg"
	ctrlnetwork "knative.dev/control-protocol/pkg/network"

	"github.com/Shopify/sarama"
	"go.opencensus.io/trace"
	"go.uber.org/zap"

	"knative.dev/eventing/pkg/metrics/source"
	"knative.dev/pkg/logging"

	"knative.dev/eventing/pkg/adapter/v2"
	"knative.dev/eventing/pkg/kncloudevents"

	"knative.dev/eventing-kafka/pkg/common/consumer"
	"knative.dev/eventing-kafka/pkg/source/client"
	kafkasourcecontrol "knative.dev/eventing-kafka/pkg/source/control"
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
	config        *AdapterConfig
	controlServer *ctrlnetwork.ControlServer
	saramaConfig  *sarama.Config

	httpMessageSender *kncloudevents.HTTPMessageSender
	reporter          source.StatsReporter
	logger            *zap.SugaredLogger
	keyTypeMapper     func([]byte) interface{}
	rateLimiter       *rate.Limiter
}

var (
	_           adapter.MessageAdapter                   = (*Adapter)(nil)
	_           consumer.KafkaConsumerHandler            = (*Adapter)(nil)
	_           consumer.SaramaConsumerLifecycleListener = (*Adapter)(nil)
	_           adapter.MessageAdapterConstructor        = NewAdapter
	retryConfig                                          = defaultRetryConfig()
)

func NewAdapter(ctx context.Context, processed adapter.EnvConfigAccessor, httpMessageSender *kncloudevents.HTTPMessageSender, reporter source.StatsReporter) adapter.MessageAdapter {
	logger := logging.FromContext(ctx)
	config := processed.(*AdapterConfig)

	return &Adapter{
		config:            config,
		httpMessageSender: httpMessageSender,
		reporter:          reporter,
		logger:            logger,
		keyTypeMapper:     getKeyTypeMapper(config.KeyType),
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

	// Init control service
	if !a.config.DisableControlServer {
		a.controlServer, err = ctrlnetwork.StartInsecureControlServer(ctx)
		if err != nil {
			return err
		}
		a.controlServer.MessageHandler(a)
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

	ctx, span := trace.StartSpan(ctx, "kafka-source")
	defer span.End()

	req, err := a.httpMessageSender.NewCloudEventRequest(ctx)
	if err != nil {
		return false, err
	}

	err = a.ConsumerMessageToHttpRequest(ctx, msg, req)
	if err != nil {
		a.logger.Debug("failed to create request", zap.Error(err))
		return true, err
	}

	res, err := a.httpMessageSender.SendWithRetries(req, retryConfig)

	if err != nil {
		a.logger.Debug("Error while sending the message", zap.Error(err))
		return false, err // Error while sending, don't commit offset
	}
	// Always try to read and close body so the connection can be reused afterwards
	if res.Body != nil {
		io.Copy(ioutil.Discard, res.Body)
		res.Body.Close()
	}

	if res.StatusCode/100 != 2 {
		a.logger.Debug("Unexpected status code", zap.Int("status code", res.StatusCode))
		return false, fmt.Errorf("%d %s", res.StatusCode, http.StatusText(res.StatusCode))
	}

	reportArgs := &source.ReportArgs{
		Namespace:     a.config.Namespace,
		Name:          a.config.Name,
		ResourceGroup: resourceGroup,
	}

	_ = a.reporter.ReportEventCount(reportArgs, res.StatusCode)
	return true, nil
}

// SetRateLimiter sets the global consumer rate limiter
func (a *Adapter) SetRateLimits(r rate.Limit, b int) {
	a.rateLimiter = rate.NewLimiter(r, b)
}

func (a *Adapter) HandleServiceMessage(ctx context.Context, message ctrl.ServiceMessage) {
	// In this first PR, there is only the RA sending messages to control plane,
	// there is no message the control plane should send to the RA
	a.logger.Info("Received unexpected control message")
	message.Ack()
}

func (a *Adapter) Setup(sess sarama.ConsumerGroupSession) {
	if a.controlServer != nil {
		if err := a.controlServer.SendAndWaitForAck(kafkasourcecontrol.NotifySetupClaimsOpCode, kafkasourcecontrol.Claims(sess.Claims())); err != nil {
			a.logger.Warnf("Cannot send the claims update: %v", err)
		}
	}

}

func (a *Adapter) Cleanup(sess sarama.ConsumerGroupSession) {
	if a.controlServer != nil {
		if err := a.controlServer.SendAndWaitForAck(kafkasourcecontrol.NotifyCleanupClaimsOpCode, kafkasourcecontrol.Claims(sess.Claims())); err != nil {
			a.logger.Warnf("Cannot send the claims update: %v", err)
		}
	}
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
