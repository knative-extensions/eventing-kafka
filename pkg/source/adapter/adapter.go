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
	"net/http"
	"reflect"
	"strings"

	"golang.org/x/time/rate"
	ctrlservice "knative.dev/control-protocol/pkg/service"

	ctrlnetwork "knative.dev/control-protocol/pkg/network"

	"github.com/Shopify/sarama"
	"go.opencensus.io/trace"
	"go.uber.org/zap"

	"knative.dev/pkg/logging"
	pkgsource "knative.dev/pkg/source"

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

	Topics        []string `envconfig:"KAFKA_TOPICS" required:"false"`
	ConsumerGroup string   `envconfig:"KAFKA_CONSUMER_GROUP" required:"false"`
	Name          string   `envconfig:"NAME" required:"true"`
	KeyType       string   `envconfig:"KEY_TYPE" required:"false"`

	// Turn off the control server.
	DisableControlProtocol bool
}

func NewEnvConfig() adapter.EnvConfigAccessor {
	return &AdapterConfig{}
}

type Adapter struct {
	config        *AdapterConfig
	controlServer *ctrlnetwork.ControlServer

	httpMessageSender *kncloudevents.HTTPMessageSender
	reporter          pkgsource.StatsReporter
	logger            *zap.SugaredLogger
	keyTypeMapper     func([]byte) interface{}
	rateLimiter       *rate.Limiter

	// These are used only in case we're running in the DisableControlServer = false (aka for st kafka source)
	newContractCh         chan ctrlservice.AsyncCommandMessage
	actualContract        *kafkasourcecontrol.KafkaSourceContract
	stopConsumer          context.CancelFunc
	consumerStoppedSignal context.Context
}

var _ adapter.MessageAdapter = (*Adapter)(nil)
var _ consumer.KafkaConsumerHandler = (*Adapter)(nil)
var _ consumer.SaramaConsumerLifecycleListener = (*Adapter)(nil)
var _ adapter.MessageAdapterConstructor = NewAdapter

func NewAdapter(ctx context.Context, processed adapter.EnvConfigAccessor, httpMessageSender *kncloudevents.HTTPMessageSender, reporter pkgsource.StatsReporter) adapter.MessageAdapter {
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

	if a.config.DisableControlProtocol {
		// Behave the old way!
		return a.mtSourceStart(ctx)
	}

	a.newContractCh = make(chan ctrlservice.AsyncCommandMessage)

	// Register the control protocol server and the message handler
	a.controlServer, err = ctrlnetwork.StartInsecureControlServer(ctx)
	if err != nil {
		return err
	}
	a.controlServer.MessageHandler(ctrlservice.MessageRouter{
		kafkasourcecontrol.SetContractCommand: ctrlservice.NewAsyncCommandHandler(
			a.controlServer,
			&kafkasourcecontrol.KafkaSourceContract{},
			kafkasourcecontrol.NotifyContractUpdated,
			func(ctx context.Context, message ctrlservice.AsyncCommandMessage) {
				a.newContractCh <- message
			},
		),
	})

	go func() {
		for {
			select {
			case newContract := <-a.newContractCh:
				a.handleSetContract(ctx, newContract)
			case <-ctx.Done():
				a.stopConsumerGroup()
				return
			}
		}
	}()

	// At this point, we do nothing, waiting for the first contract to come
	// Environment variables are ignored and all the configuration comes from the control plane through the control protocol

	// This goroutine remains forever blocked until the adapter is closed
	<-ctx.Done()
	a.logger.Info("Shutting down...")
	return nil
}

func (a *Adapter) mtSourceStart(ctx context.Context) (err error) {
	// init consumer group
	addrs, config, err := client.NewConfigWithEnv(context.Background(), &a.config.KafkaEnvConfig)
	if err != nil {
		return fmt.Errorf("failed to create the config: %w", err)
	}

	consumerGroupFactory := consumer.NewConsumerGroupFactory(addrs, config)
	group, err := consumerGroupFactory.StartConsumerGroup(
		a.config.ConsumerGroup,
		a.config.Topics,
		a.logger,
		a,
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

	res, err := a.httpMessageSender.Send(req)

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

	reportArgs := &pkgsource.ReportArgs{
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

func (a *Adapter) handleSetContract(ctx context.Context, msg ctrlservice.AsyncCommandMessage) {
	newContract := msg.ParsedCommand().(*kafkasourcecontrol.KafkaSourceContract)
	a.logger.Infof("Setting the new contract, generation: %d", newContract.Generation)

	if reflect.DeepEqual(&newContract, a.actualContract) {
		// Nothing to do here
		msg.NotifySuccess()
		return
	}

	a.stopConsumerGroup()

	a.actualContract = newContract

	consumerContext, stopConsumer := context.WithCancel(ctx)
	a.stopConsumer = stopConsumer
	var startupError error
	a.consumerStoppedSignal, startupError = a.startConsumerGroup(consumerContext, consumer.WithSaramaConsumerLifecycleListener(a))

	if startupError == nil {
		msg.NotifySuccess()
	} else {
		msg.NotifyFailed(startupError)
	}
}

func (a *Adapter) stopConsumerGroup() {
	if a.stopConsumer != nil {
		a.stopConsumer()
	}
	if a.consumerStoppedSignal != nil {
		<-a.consumerStoppedSignal.Done()
	}
}

func (a *Adapter) startConsumerGroup(ctx context.Context, consumerGroupOptions ...consumer.SaramaConsumerHandlerOption) (context.Context, error) {
	// I'm overwriting these to avoid creating a too big change
	// A proper solution is to pass the whole config through the control protocol
	// But, this is still not possible, because some parts of the config still comes from the env,
	// hence this can't be trivially removed. It will be, gradually
	// TODO(slinkydeveloper) make client.NewConfig flexible to accept KafkaSourceContract too
	a.config.KafkaEnvConfig.BootstrapServers = a.actualContract.BootstrapServers
	a.keyTypeMapper = getKeyTypeMapper(a.actualContract.KeyType) // Maybe this one needs a lock
	a.config.ConsumerGroup = a.actualContract.ConsumerGroup
	a.config.Topics = a.actualContract.Topics

	addrs, config, err := client.NewConfigWithEnv(context.Background(), &a.config.KafkaEnvConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create the config: %w", err)
	}

	consumerGroupFactory := consumer.NewConsumerGroupFactory(addrs, config)
	group, err := consumerGroupFactory.StartConsumerGroup(
		a.config.ConsumerGroup,
		a.config.Topics,
		a.logger,
		a,
		consumerGroupOptions...,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to start consumer group: %w", err)
	}

	closedSignal, cancel := context.WithCancel(context.Background())

	// Goroutine to stop the thing
	go func() {
		for {
			select {
			case <-ctx.Done():
				err := group.Close()
				if err != nil {
					a.logger.Errorw("Failed to close consumer group", zap.Error(err))
				}
				cancel()
				return
			case err := <-group.Errors():
				a.logger.Errorw("Error while consuming messages", zap.Error(err))
			}
		}
	}()
	return closedSignal, nil
}

func (a *Adapter) Setup(sess sarama.ConsumerGroupSession) {
	if err := a.controlServer.SendAndWaitForAck(kafkasourcecontrol.NotifySetupClaimsOpCode, kafkasourcecontrol.Claims(sess.Claims())); err != nil {
		a.logger.Warnf("Cannot send the claims update: %v", err)
	}
}

func (a *Adapter) Cleanup(sess sarama.ConsumerGroupSession) {
	if err := a.controlServer.SendAndWaitForAck(kafkasourcecontrol.NotifyCleanupClaimsOpCode, kafkasourcecontrol.Claims(sess.Claims())); err != nil {
		a.logger.Warnf("Cannot send the claims update: %v", err)
	}
}
