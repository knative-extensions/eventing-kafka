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
	"strings"

	ctrlservice "knative.dev/control-protocol/pkg/service"

	ctrlnetwork "knative.dev/control-protocol/pkg/network"

	"github.com/Shopify/sarama"
	"go.uber.org/zap"

	pkgsource "knative.dev/pkg/source"

	"knative.dev/eventing/pkg/adapter/v2"
	"knative.dev/eventing/pkg/kncloudevents"

	"knative.dev/eventing-kafka/pkg/common/consumer"
	kafkasourcecontrol "knative.dev/eventing-kafka/pkg/source/control"
)

type ControlledAdapter struct {
	*Adapter

	controlServerOptions []ctrlnetwork.ControlServerOption
	controlServer        *ctrlnetwork.ControlServer

	newContractCh         chan ctrlservice.AsyncCommandMessage
	actualContract        *kafkasourcecontrol.KafkaSourceContract
	stopConsumer          context.CancelFunc
	consumerStoppedSignal context.Context
}

var _ adapter.MessageAdapter = (*ControlledAdapter)(nil)
var _ consumer.KafkaConsumerHandler = (*ControlledAdapter)(nil)
var _ consumer.SaramaConsumerLifecycleListener = (*ControlledAdapter)(nil)
var _ adapter.MessageAdapterConstructor = NewControlledAdapter

func NewControlledAdapter(ctx context.Context, processed adapter.EnvConfigAccessor, httpMessageSender *kncloudevents.HTTPMessageSender, reporter pkgsource.StatsReporter) adapter.MessageAdapter {
	return &ControlledAdapter{
		Adapter: NewAdapter(ctx, processed, httpMessageSender, reporter).(*Adapter),
	}
}

func (a *ControlledAdapter) Start(ctx context.Context) (err error) {
	a.logger.Infow("Starting with config: ",
		zap.String("Topics", strings.Join(a.config.Topics, ",")),
		zap.String("ConsumerGroup", a.config.ConsumerGroup),
		zap.String("SinkURI", a.config.Sink),
		zap.String("Name", a.config.Name),
		zap.String("Namespace", a.config.Namespace),
	)

	a.newContractCh = make(chan ctrlservice.AsyncCommandMessage)

	// Register the control protocol server and the message handler
	a.controlServer, err = ctrlnetwork.StartInsecureControlServer(ctx, a.controlServerOptions...)
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
				return
			}
		}
	}()

	// At this point, we do nothing, waiting for the first contract to come
	// Environment variables are ignored and all the configuration comes from the control plane through the control protocol

	// This goroutine remains forever blocked until the adapter is closed
	<-ctx.Done()
	a.logger.Info("Shutting down...")

	a.stopConsumerGroup()
	<-a.controlServer.ClosedCh() // The stop signal is the same as ctx
	a.logger.Info("Shut down completed")

	return nil
}

func (a *ControlledAdapter) handleSetContract(ctx context.Context, msg ctrlservice.AsyncCommandMessage) {
	newContract := msg.ParsedCommand().(*kafkasourcecontrol.KafkaSourceContract)
	a.logger.Infof("Received contract, generation: %d", newContract.Generation)

	if a.actualContract != nil && newContract.Generation == a.actualContract.Generation {
		a.logger.Info("The received contract has the same generation of the previous one, ignoring it")

		// Nothing to do here
		msg.NotifySuccess()
		return
	}

	// Stop consumer group
	a.stopConsumerGroup()

	// I'm overwriting these to avoid creating a too big change
	// A proper solution is to pass the whole config through the control protocol
	// But, this is still not possible, because some parts of the config still comes from the env,
	// hence this can't be trivially removed. It will be, gradually
	// TODO(slinkydeveloper) make client.NewConfig flexible to accept KafkaSourceContract too
	a.actualContract = newContract
	a.config.KafkaEnvConfig.BootstrapServers = a.actualContract.BootstrapServers
	a.keyTypeMapper = getKeyTypeMapper(a.actualContract.KeyType) // Maybe this one needs a lock
	a.config.ConsumerGroup = a.actualContract.ConsumerGroup
	a.config.Topics = a.actualContract.Topics

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

func (a *ControlledAdapter) stopConsumerGroup() {
	if a.stopConsumer != nil {
		a.stopConsumer()
	}
	if a.consumerStoppedSignal != nil {
		<-a.consumerStoppedSignal.Done()
	}
}

func (a *ControlledAdapter) Setup(sess sarama.ConsumerGroupSession) {
	if err := a.controlServer.SendAndWaitForAck(kafkasourcecontrol.NotifySetupClaimsOpCode, kafkasourcecontrol.Claims(sess.Claims())); err != nil {
		a.logger.Warnf("Cannot send the claims update: %v", err)
	}
}

func (a *ControlledAdapter) Cleanup(sess sarama.ConsumerGroupSession) {
	if err := a.controlServer.SendAndWaitForAck(kafkasourcecontrol.NotifyCleanupClaimsOpCode, kafkasourcecontrol.Claims(sess.Claims())); err != nil {
		a.logger.Warnf("Cannot send the claims update: %v", err)
	}
}
