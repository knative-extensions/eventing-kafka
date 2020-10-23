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
	"github.com/Shopify/sarama"
	kafkasaramaprotocol "github.com/cloudevents/sdk-go/protocol/kafka_sarama/v2"
	"github.com/cloudevents/sdk-go/v2/binding"
	"go.opencensus.io/trace"
	"go.uber.org/zap"
	"knative.dev/eventing-kafka/pkg/channel/distributed/common/util"
	eventingduck "knative.dev/eventing/pkg/apis/duck/v1"
	"knative.dev/eventing/pkg/channel"
	"knative.dev/eventing/pkg/kncloudevents"
	"net/http"
	"net/url"
)

// Verify The Handler Implements The Sarama ConsumerGroupHandler
var _ sarama.ConsumerGroupHandler = &Handler{}

// Define A Sarama ConsumerGroupHandler Implementation
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
			retryConfig.CheckRetry = h.checkRetry // Specify Custom CheckRetry Function
		}
	}

	// Pull Any Available Messages From The ConsumerGroupClaim (Until The Channel Closes)
	for message := range claim.Messages() {

		// Consume The Message (Ignore Errors - Will have already been retried and we're moving on so as not to block further Topic processing.)
		_ = h.consumeMessage(session.Context(), message, destinationURL, replyURL, deadLetterURL, &retryConfig)

		// Mark The Message As Having Been Consumed (Does Not Imply Successful Delivery - Only Full Retry Attempts Made)
		session.MarkMessage(message, "")
	}

	// Return Success
	return nil
}

// Consume A Single Message
func (h *Handler) consumeMessage(context context.Context, consumerMessage *sarama.ConsumerMessage, destinationURL *url.URL, replyURL *url.URL, deadLetterURL *url.URL, retryConfig *kncloudevents.RetryConfig) error {

	// Debug Log Kafka ConsumerMessage
	h.Logger.Debug("Consuming Kafka Message",
		zap.Any("Headers", consumerMessage.Headers),
		zap.ByteString("Key", consumerMessage.Key),
		zap.ByteString("Value", consumerMessage.Value),
		zap.String("Topic", consumerMessage.Topic),
		zap.Int32("Partition", consumerMessage.Partition),
		zap.Int64("Offset", consumerMessage.Offset))

	// Convert The Sarama ConsumerMessage Into A CloudEvents Message
	message := kafkasaramaprotocol.NewMessageFromConsumerMessage(consumerMessage)
	if message.ReadEncoding() == binding.EncodingUnknown {
		h.Logger.Warn("Received A Message With Unknown Encoding - Skipping")
		return errors.New("received a message with unknown encoding - skipping")
	}

	ctx, span := startTraceFromMessage(h.Logger, context, message, consumerMessage.Topic)
	defer span.End()

	// Dispatch The Message With Configured Retries & Return Any Errors
	_, dispatchError := h.MessageDispatcher.DispatchMessageWithRetries(ctx, message, nil, destinationURL, replyURL, deadLetterURL, retryConfig)
	return dispatchError
}

func startTraceFromMessage(logger *zap.Logger, inCtx context.Context, message *kafkasaramaprotocol.Message, topic string) (context.Context, *trace.Span) {
	sc, ok := util.ParseSpanContext(message.Headers)
	if !ok {
		logger.Warn("Cannot parse the spancontext, creating a new span")
		return trace.StartSpan(inCtx, "kafkachannel-"+topic)
	}

	return trace.StartSpanWithRemoteParent(inCtx, "kafkachannel-"+topic, sc)
}

//
// Custom Implementation Of RetryConfig.CheckRetry To Determine Whether To Retry Based On Response
//
// Note - Returning true indicates a retry should occur.  Returning an error will result in that
//        error being returned instead of any errors from the Request.
//
func (h *Handler) checkRetry(_ context.Context, response *http.Response, err error) (bool, error) {

	// Retry Any Nil HTTP Response
	if response == nil {
		h.Logger.Info("Unable To Check Retry State On Nil Response - Retrying")
		return true, nil
	}

	// Retry Any Errors
	if err != nil {
		h.Logger.Warn("Received Response Error - Retrying", zap.Error(err))
		return true, nil
	}

	// Extract The StatusCode From The Response & Add To Logger
	statusCode := response.StatusCode
	logger := h.Logger.With(zap.Int("StatusCode", statusCode))

	//
	// Note - Normally we would NOT want to retry 400 responses, BUT the knative-eventing
	//        filter handler (due to CloudEvents SDK V1 usage) is swallowing the actual
	//        status codes from the subscriber and returning 400s instead.  Once this has,
	//        been resolved we can remove 400 from the list of codes to retry.
	//
	if statusCode >= 500 || statusCode == 400 || statusCode == 404 || statusCode == 429 || statusCode == 409 {
		logger.Warn("Failed To Send Message To Subscriber Service - Retrying")
		return true, nil
	} else if statusCode >= 300 && statusCode <= 399 {
		logger.Warn("Failed To Send Message To Subscriber Service - Not Retrying")
		return false, nil
	} else if statusCode == -1 {
		logger.Warn("No StatusCode Detected In Error - Retrying")
		return true, nil
	}

	// Do Not Retry 1XX, 2XX, & Most 4XX StatusCode Responses
	return false, nil
}
