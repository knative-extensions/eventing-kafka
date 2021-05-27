/*
Copyright 2021 The Knative Authors

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

package consumer

import (
	"context"
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	"go.uber.org/zap"
)

type KafkaConsumerHandler interface {
	// When this function returns true, the consumer group offset is marked as consumed.
	// The returned error is enqueued in errors channel.
	Handle(context context.Context, message *sarama.ConsumerMessage) (bool, error)
	SetReady(partition int32, ready bool)
	GetConsumerGroup() string
}

// ConsumerHandler implements sarama.ConsumerGroupHandler and provides some glue code to simplify message handling
// You must implement KafkaConsumerHandler and create a new SaramaConsumerHandler with it
type SaramaConsumerHandler struct {
	// The user message handler
	handler KafkaConsumerHandler

	logger  *zap.SugaredLogger
	timeout time.Duration
	// Errors channel
	errors chan error
}

func NewConsumerHandler(logger *zap.SugaredLogger, handler KafkaConsumerHandler, errorsCh chan error) SaramaConsumerHandler {
	return SaramaConsumerHandler{
		logger:  logger,
		handler: handler,
		errors:  errorsCh,
		timeout: 60 * time.Second,
	}
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *SaramaConsumerHandler) Setup(sarama.ConsumerGroupSession) error {
	consumer.logger.Info("setting up handler")
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *SaramaConsumerHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	consumer.logger.Infow("Cleanup handler")
	for t, ps := range session.Claims() {
		for _, p := range ps {
			consumer.logger.Debugw("Cleanup handler: Setting partition readiness to false", zap.String("topic", t),
				zap.Int32("partition", p))
			consumer.handler.SetReady(p, false)
		}
	}
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *SaramaConsumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	consumer.logger.Infow(fmt.Sprintf("Starting partition consumer, topic: %s, partition: %d, initialOffset: %d", claim.Topic(), claim.Partition(), claim.InitialOffset()), zap.String("ConsumeGroup", consumer.handler.GetConsumerGroup()))
	consumer.handler.SetReady(claim.Partition(), true)
	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29
	c := make(chan bool)

	for message := range claim.Messages() {

		if ce := consumer.logger.Desugar().Check(zap.DebugLevel, "debugging"); ce != nil {
			consumer.logger.Debugw("Message claimed", zap.String("topic", message.Topic), zap.Binary("value", message.Value))
		}
		// Preemptively interrupt processing messages if the session is closed.
		// Processing all messages from the buffered channel can take a long time,
		// potentially exceeding the session timeout, leading to duplicate events.
		if session.Context().Err() != nil {
			consumer.logger.Infof("Session closed for %s/%d. Exiting ConsumeClaim ", claim.Topic(), claim.Partition())
			break
		}
		// We need to control when to cancel Handle calls so give it a downstream context
		hctx, cancel := context.WithCancel(context.Background())
		// Start Handle routine
		go func() {
			mustMark, err := consumer.handler.Handle(hctx, message)

			if err != nil {
				consumer.logger.Errorw("Failure while handling a message", zap.String("topic", message.Topic), zap.Int32("partition", message.Partition), zap.Int64("offset", message.Offset), zap.Error(err))
				consumer.errors <- err
				consumer.handler.SetReady(claim.Partition(), false)
			}
			c <- mustMark
		}()

		var mustMark bool
		select {
		case mustMark = <-c:
			// Handle returned gracefully, call cancel to free the context resources.
			cancel()
		case <-session.Context().Done():
			// Consumer session canceled, wait for in-flight request to finish before we hit a rebalance timeout
			select {
			case <-time.After(consumer.timeout):
				// Handle still didn't return, cancel the in-flight request
				cancel()
				// Unblock the Handle goroutine
				mustMark = <-c
			case mustMark = <-c:
				// Handle returned gracefully, call cancel to free the context resources.
				cancel()
			}
		}

		if mustMark {
			session.MarkMessage(message, "") // Mark kafka message as processed
			if ce := consumer.logger.Desugar().Check(zap.DebugLevel, "debugging"); ce != nil {
				consumer.logger.Debugw("Message marked", zap.String("topic", message.Topic), zap.Binary("value", message.Value))
			}
		}

	}

	consumer.logger.Infof("Stopping partition consumer, topic: %s, partition: %d", claim.Topic(), claim.Partition())
	return nil
}

var _ sarama.ConsumerGroupHandler = (*SaramaConsumerHandler)(nil)
