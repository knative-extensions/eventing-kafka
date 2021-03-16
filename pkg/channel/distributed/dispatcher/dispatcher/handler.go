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

package dispatcher

import (
	"context"
	"errors"
	"net/url"
	"strings"

	"github.com/Shopify/sarama"
	kafkasaramaprotocol "github.com/cloudevents/sdk-go/protocol/kafka_sarama/v2"
	"github.com/cloudevents/sdk-go/v2/binding"
	"go.uber.org/zap"
	kafkasarama "knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/sarama"
	"knative.dev/eventing-kafka/pkg/common/tracing"
	eventingduck "knative.dev/eventing/pkg/apis/duck/v1"
	"knative.dev/eventing/pkg/channel"
	"knative.dev/eventing/pkg/kncloudevents"
)

// Verify The Handler Implements The Sarama ConsumerGroupHandler
var _ sarama.ConsumerGroupHandler = &Handler{}

// Define A Sarama ConsumerGroupHandler DispatcherImpl
type Handler struct {
	Logger            *zap.Logger
	Subscriber        *eventingduck.SubscriberSpec
	MessageDispatcher channel.MessageDispatcher
}

// Create A New Handler
func NewHandler(logger *zap.Logger, subscriber *eventingduck.SubscriberSpec) *Handler {
	return &Handler{
		Logger:            logger,
		Subscriber:        subscriber,
		MessageDispatcher: newMessageDispatcherWrapper(logger),
	}
}

// Wrapper Function To Facilitate Testing With A Mock Knative MessageDispatcher
var newMessageDispatcherWrapper = func(logger *zap.Logger) channel.MessageDispatcher {
	return channel.NewMessageDispatcher(logger)
}

// ConsumerGroupHandler Lifecycle Method (Runs before any ConsumeClaims)
func (h *Handler) Setup(_ sarama.ConsumerGroupSession) error {
	return nil // Nothing To Do As Of Yet
}

// ConsumerGroupHandler Lifecycle Method (Runs after all ConsumeClaims stop but before final offset commit)
func (h *Handler) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil // Nothing To Do As Of Yet
}

// ConsumerGroupHandler Lifecycle Method (Main processing loop, must finish when claim.Messages() channel closes.)
func (h *Handler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	// Extract The Destination URL From The Subscriber
	var destinationURL *url.URL
	if !h.Subscriber.SubscriberURI.IsEmpty() {
		destinationURL = h.Subscriber.SubscriberURI.URL()
	}

	// Extract The Reply URL From The Subscriber
	var replyURL *url.URL
	if !h.Subscriber.ReplyURI.IsEmpty() {
		replyURL = h.Subscriber.ReplyURI.URL()
	}

	// Validate The Subscriber's Delivery (Optional)
	var deadLetterURL *url.URL
	retryConfig := kncloudevents.NoRetries()
	if h.Subscriber.Delivery != nil {

		// Extract The DeadLetterSink From The Subscriber.Delivery
		if h.Subscriber.Delivery.DeadLetterSink != nil &&
			h.Subscriber.Delivery.DeadLetterSink.URI != nil &&
			!h.Subscriber.Delivery.DeadLetterSink.URI.IsEmpty() {
			deadLetterURL = h.Subscriber.Delivery.DeadLetterSink.URI.URL()
		}

		// Extract The RetryConfig From The Subscriber.Delivery (Defaults To NoRetries)
		var err error
		retryConfig, err = kncloudevents.RetryConfigFromDeliverySpec(*h.Subscriber.Delivery)
		if err != nil {
			h.Logger.Error("Failed To Parse RetryConfig From DeliverySpec - No Retries Will Occur", zap.Error(err))
		} else {
			h.Logger.Info("Successfully Parsed RetryConfig From DeliverySpec", zap.Int("RetryMax", retryConfig.RetryMax))
			retryConfig.CheckRetry = kncloudevents.SelectiveRetry // Specify Custom CheckRetry Function
		}
	}

	// Pull Any Available Messages From The ConsumerGroupClaim (Until The Channel Closes)
	for message := range claim.Messages() {

		// Consume The Message (Ignore any errors other than "context canceled" - Will have already been retried
		// and we're moving on so as not to block further Topic processing.)
		err := h.consumeMessage(session.Context(), message, destinationURL, replyURL, deadLetterURL, &retryConfig)

		// If the system is shutting down, it's possible to get a "unable to complete request to [destination]: context canceled" error
		// This is different than other types of failure in that it is the dispatcher's "fault" and so marking the message as delivered
		// would be a violation of the "at least once" guarantee (because the next dispatcher that starts would see it as marked and not
		// try to deliver it again).
		if err == nil || !strings.Contains(err.Error(), context.Canceled.Error()) {
			// Mark The Message As Having Been Consumed (Does Not Imply Successful Delivery - Only Full Retry Attempts Made)
			session.MarkMessage(message, "")
		}

	}

	// Return Success
	return nil
}

// Consume A Single Message
func (h *Handler) consumeMessage(context context.Context, consumerMessage *sarama.ConsumerMessage, destinationURL *url.URL, replyURL *url.URL, deadLetterURL *url.URL, retryConfig *kncloudevents.RetryConfig) error {

	// Debug Log Kafka ConsumerMessage
	if h.Logger.Core().Enabled(zap.DebugLevel) {
		// Checked Logging Level First To Avoid Calling StringifyHeaderPtrs In Production
		h.Logger.Debug("Consuming Kafka Message",
			zap.Any("Headers", kafkasarama.StringifyHeaderPtrs(consumerMessage.Headers)), // Log human-readable strings, not base64
			zap.ByteString("Key", consumerMessage.Key),
			zap.ByteString("Value", consumerMessage.Value),
			zap.String("Topic", consumerMessage.Topic),
			zap.Int32("Partition", consumerMessage.Partition),
			zap.Int64("Offset", consumerMessage.Offset))
	}

	// Convert The Sarama ConsumerMessage Into A CloudEvents Message
	message := kafkasaramaprotocol.NewMessageFromConsumerMessage(consumerMessage)
	if message.ReadEncoding() == binding.EncodingUnknown {
		h.Logger.Warn("Received A Message With Unknown Encoding - Skipping")
		return errors.New("received a message with unknown encoding - skipping")
	}

	ctx, span := tracing.StartTraceFromMessage(h.Logger.Sugar(), context, message, consumerMessage.Topic)
	defer span.End()

	// Dispatch The Message With Configured Retries & Return Any Errors
	_, dispatchError := h.MessageDispatcher.DispatchMessageWithRetries(ctx, message, nil, destinationURL, replyURL, deadLetterURL, retryConfig)
	return dispatchError
}
