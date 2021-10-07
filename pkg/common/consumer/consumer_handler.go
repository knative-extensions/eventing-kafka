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

	kafkasarama "knative.dev/eventing-kafka/pkg/common/kafka/sarama"
)

type KafkaConsumerHandler interface {
	// When this function returns true, the consumer group offset is marked as consumed.
	// The returned error is enqueued in errors channel.
	Handle(context context.Context, message *sarama.ConsumerMessage) (bool, error)
	SetReady(partition int32, ready bool)
	GetConsumerGroup() string
}

type SaramaConsumerLifecycleListener interface {
	// Setup is invoked when the consumer is joining the session
	Setup(sess sarama.ConsumerGroupSession)

	// Cleanup is invoked when the consumer is leaving the session
	Cleanup(sess sarama.ConsumerGroupSession)
}

type noopSaramaConsumerLifecycleListener struct{}

func (n noopSaramaConsumerLifecycleListener) Setup(sess sarama.ConsumerGroupSession) {}

func (n noopSaramaConsumerLifecycleListener) Cleanup(sess sarama.ConsumerGroupSession) {}

func WithSaramaConsumerLifecycleListener(listener SaramaConsumerLifecycleListener) SaramaConsumerHandlerOption {
	return func(handler *SaramaConsumerHandler) {
		handler.lifecycleListener = listener
	}
}

// WithTimeout configures the request timeout. Default is set to 60s.
func WithTimeout(timeout time.Duration) SaramaConsumerHandlerOption {
	return func(handler *SaramaConsumerHandler) {
		handler.timeout = timeout
	}
}

// ConsumerHandler implements sarama.ConsumerGroupHandler and provides some glue code to simplify message handling
// You must implement KafkaConsumerHandler and create a new SaramaConsumerHandler with it
type SaramaConsumerHandler struct {
	// The user message handler
	handler KafkaConsumerHandler

	// Request to sink timeout
	timeout time.Duration

	lifecycleListener SaramaConsumerLifecycleListener

	logger *zap.SugaredLogger

	// Errors channel
	errors chan error
}

type SaramaConsumerHandlerOption func(*SaramaConsumerHandler)

func NewConsumerHandler(logger *zap.SugaredLogger, handler KafkaConsumerHandler, errorsCh chan error, options ...SaramaConsumerHandlerOption) SaramaConsumerHandler {
	sch := SaramaConsumerHandler{
		handler:           handler,
		lifecycleListener: noopSaramaConsumerLifecycleListener{},
		timeout:           60 * time.Second, // default rebalance timeout
		logger:            logger,
		errors:            errorsCh,
	}

	for _, f := range options {
		f(&sch)
	}

	return sch
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *SaramaConsumerHandler) Setup(session sarama.ConsumerGroupSession) error {
	consumer.logger.Info("setting up handler", zap.Any("claims", session.Claims()))
	consumer.lifecycleListener.Setup(session)
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
	consumer.lifecycleListener.Cleanup(session)
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *SaramaConsumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	consumer.logger.Infow(fmt.Sprintf("Starting partition consumer, topic: %s, partition: %d, initialOffset: %d", claim.Topic(), claim.Partition(), claim.InitialOffset()), zap.String("ConsumeGroup", consumer.handler.GetConsumerGroup()))
	consumer.handler.SetReady(claim.Partition(), true)
	c := make(chan bool)

	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29
	for message := range claim.Messages() {

		// Debug Log Kafka ConsumerMessage
		if consumer.logger.Desugar().Core().Enabled(zap.DebugLevel) {
			// Checked Logging Level First To Avoid Calling StringifyHeaderPtrs In Production
			consumer.logger.Debugw("Consuming Kafka Message",
				zap.Any("Headers", kafkasarama.StringifyHeaderPtrs(message.Headers)), // Log human-readable strings, not base64
				zap.ByteString("Key", message.Key),
				zap.ByteString("Value", message.Value),
				zap.String("Topic", message.Topic),
				zap.Int32("Partition", message.Partition),
				zap.Int64("Offset", message.Offset))
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

		// Start Handle goroutine
		go func() {
			mustMark, err := consumer.handler.Handle(hctx, message)

			if err != nil {
				consumer.logger.Infow("Failure while handling a message", zap.String("topic", message.Topic), zap.Int32("partition", message.Partition), zap.Int64("offset", message.Offset), zap.Error(err))
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
			if consumer.logger.Desugar().Core().Enabled(zap.DebugLevel) {
				consumer.logger.Debugw("Message marked", zap.String("topic", message.Topic), zap.Binary("value", message.Value))
			}
		}
	}

	consumer.logger.Infof("Stopping partition consumer, topic: %s, partition: %d", claim.Topic(), claim.Partition())
	return nil
}

var _ sarama.ConsumerGroupHandler = (*SaramaConsumerHandler)(nil)
