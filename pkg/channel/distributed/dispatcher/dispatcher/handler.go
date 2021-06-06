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
	"go.uber.org/zap/zapcore"
	kafkasarama "knative.dev/eventing-kafka/pkg/common/kafka/sarama"
	"knative.dev/eventing-kafka/pkg/common/tracing"
	eventingduck "knative.dev/eventing/pkg/apis/duck/v1"
	"knative.dev/eventing/pkg/channel"
	"knative.dev/eventing/pkg/kncloudevents"
)

// Verify The Handler Implements The Sarama ConsumerGroupHandler
var _ sarama.ConsumerGroupHandler = &Handler{}

// Handler Defines A Sarama ConsumerGroupHandler Implementation
type Handler struct {
	Logger            *zap.Logger
	Subscriber        *eventingduck.SubscriberSpec
	MessageDispatcher channel.MessageDispatcher
}

// NewHandler Creates A New Handler
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

// Setup Is The Initial ConsumerGroupHandler Lifecycle Method (Runs before any ConsumeClaims)
func (h *Handler) Setup(_ sarama.ConsumerGroupSession) error {
	return nil // Nothing To Do As Of Yet
}

// Cleanup Is The Final ConsumerGroupHandler Lifecycle Method (Runs after all ConsumeClaims stop but before final offset commit)
func (h *Handler) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil // Nothing To Do As Of Yet
}

// ConsumeClaim Is The Main ConsumerGroupHandler Lifecycle Method (Main processing loop, must finish when claim.Messages() channel closes.)
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

// consumeMessage Consumes A Single Message
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
	info, dispatchError := h.MessageDispatcher.DispatchMessageWithRetries(ctx, message, nil, destinationURL, replyURL, deadLetterURL, retryConfig)
	h.Logger.Debug("Received Response", zap.Any("ExecutionInfo", executionInfoWrapper{info}))

	return dispatchError
}

// executionInfoWrapper wraps a DispatchExecutionInfo struct so that zap.Any can lazily marshal it
type executionInfoWrapper struct {
	*channel.DispatchExecutionInfo
}

// MarshalLogObject implements the zapcore.ObjectMarshaler interface on the executionInfoWrapper
func (w executionInfoWrapper) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddDuration("Time", w.Time)
	enc.AddInt("ResponseCode", w.ResponseCode)
	if len(w.ResponseBody) > 500 {
		enc.AddString("Body", string(w.ResponseBody[:500])+"...")
	} else {
		enc.AddString("Body", string(w.ResponseBody))
	}
	return nil
}

///*
//Copyright 2020 The Knative Authors
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.
//*/
//
//package dispatcher
//
//import (
//	"context"
//	"errors"
//	"fmt"
//	"net/url"
//	"strings"
//
//	"github.com/Shopify/sarama"
//	kafkasaramaprotocol "github.com/cloudevents/sdk-go/protocol/kafka_sarama/v2"
//	"github.com/cloudevents/sdk-go/v2/binding"
//	"go.uber.org/zap"
//	"go.uber.org/zap/zapcore"
//	eventingduck "knative.dev/eventing/pkg/apis/duck/v1"
//	"knative.dev/eventing/pkg/channel"
//	"knative.dev/eventing/pkg/kncloudevents"
//
//	commonconsumer "knative.dev/eventing-kafka/pkg/common/consumer"
//	kafkasarama "knative.dev/eventing-kafka/pkg/common/kafka/sarama"
//	"knative.dev/eventing-kafka/pkg/common/tracing"
//)
//
//// Verify The Handler Implements The Common KafkaConsumerHandler
//var _ commonconsumer.KafkaConsumerHandler = &Handler{}
//
//// Handler Struct implementing the KafkaConsumerHandler Interface
//type Handler struct {
//	Logger            *zap.Logger
//	GroupId           string
//	Subscriber        *eventingduck.SubscriberSpec
//	MessageDispatcher channel.MessageDispatcher
//	destinationURL    *url.URL
//	replyURL          *url.URL
//	deadLetterURL     *url.URL
//	retryConfig       kncloudevents.RetryConfig
//}
//
//// NewHandler creates a new Handler instance.
//func NewHandler(logger *zap.Logger, groupId string, subscriber *eventingduck.SubscriberSpec) *Handler {
//
//	// Create The New Handler Instance
//	handler := &Handler{
//		Logger:            logger,
//		GroupId:           groupId,
//		Subscriber:        subscriber,
//		MessageDispatcher: newMessageDispatcherWrapper(logger),
//	}
//
//	// Extract The Destination URL From The Subscriber
//	if !subscriber.SubscriberURI.IsEmpty() {
//		handler.destinationURL = subscriber.SubscriberURI.URL()
//	}
//
//	// Extract The Reply URL From The Subscriber
//	if !subscriber.ReplyURI.IsEmpty() {
//		handler.replyURL = subscriber.ReplyURI.URL()
//	}
//
//	// Validate The Subscriber's Delivery (Optional - Defaults To No Retries Or DLQ)
//	handler.retryConfig = kncloudevents.NoRetries()
//	if subscriber.Delivery != nil {
//
//		// Extract The DeadLetterSink From The Subscriber.Delivery
//		if subscriber.Delivery.DeadLetterSink != nil &&
//			subscriber.Delivery.DeadLetterSink.URI != nil &&
//			!subscriber.Delivery.DeadLetterSink.URI.IsEmpty() {
//			handler.deadLetterURL = subscriber.Delivery.DeadLetterSink.URI.URL()
//		}
//
//		// Extract The RetryConfig From The Subscriber.Delivery
//		var err error
//		handler.retryConfig, err = kncloudevents.RetryConfigFromDeliverySpec(*subscriber.Delivery)
//		if err != nil {
//			logger.Error("Failed To Parse RetryConfig From DeliverySpec - No Retries Will Occur", zap.Error(err))
//		} else {
//			logger.Info("Successfully Parsed RetryConfig From DeliverySpec", zap.Int("RetryMax", handler.retryConfig.RetryMax))
//			handler.retryConfig.CheckRetry = kncloudevents.SelectiveRetry // Specify Custom CheckRetry Function
//		}
//	}
//
//	// Return The Configured Handler
//	fmt.Printf("EDV: ~NewHandler()\n")
//	return handler
//}
//
//// Wrapper Function To Facilitate Testing With A Mock Knative MessageDispatcher
//var newMessageDispatcherWrapper = func(logger *zap.Logger) channel.MessageDispatcher {
//	return channel.NewMessageDispatcher(logger)
//}
//
//// Handle is responsible for processing the individual ConsumerMessages.  The
//// first return bool indicates whether to MarkOffset in the ConsumerGroup and
//// the second error value will be sent to the ConsumerGroups error channel as
//// well as the SetReady() function.
//func (h *Handler) Handle(ctx context.Context, consumerMessage *sarama.ConsumerMessage) (bool, error) {
//
//	fmt.Printf("EDV: Handle()\n")
//	// Debug Log Kafka ConsumerMessage (Verify Debug Level For Efficiency!)
//	if h.Logger.Core().Enabled(zap.DebugLevel) {
//
//		// Checked Logging Level First To Avoid Calling StringifyHeaderPtrs() In Production
//		h.Logger.Debug("Consuming Kafka Message",
//			zap.Any("Headers", kafkasarama.StringifyHeaderPtrs(consumerMessage.Headers)), // Log human-readable strings, not base64
//			zap.ByteString("Key", consumerMessage.Key),
//			zap.ByteString("Value", consumerMessage.Value),
//			zap.String("Topic", consumerMessage.Topic),
//			zap.Int32("Partition", consumerMessage.Partition),
//			zap.Int64("Offset", consumerMessage.Offset))
//	}
//
//	// Convert The Sarama ConsumerMessage Into A CloudEvents Message
//	message := kafkasaramaprotocol.NewMessageFromConsumerMessage(consumerMessage)
//	if message.ReadEncoding() == binding.EncodingUnknown {
//		h.Logger.Warn("Received A Message With Unknown Encoding - Skipping")
//		return true, errors.New("received a message with unknown encoding - skipping") // Mark As Handled Since Retry Won't Fix Anything : )
//	}
//
//	// Start Tracing
//	ctx, span := tracing.StartTraceFromMessage(h.Logger.Sugar(), ctx, message, consumerMessage.Topic)
//	defer span.End()
//
//	// Dispatch The Message With Configured Retries, DLQ, etc
//	info, err := h.MessageDispatcher.DispatchMessageWithRetries(ctx, message, nil, h.destinationURL, h.replyURL, h.deadLetterURL, &h.retryConfig)
//	h.Logger.Debug("Received Response", zap.Any("ExecutionInfo", executionInfoWrapper{info}))
//
//	//
//	// Determine Whether To Mark The Message As Processed
//	// (Does Not Imply Successful Delivery - Only Full Retry Attempts Made)
//	//
//	// In general all messages are marked as Processed, even if the result of
//	// processing was not a successful delivery. This is to prevent blocking
//	// the Dispatcher forever on invalid messages, or faulty subscribers, etc.
//	// If, however, the Dispatcher is shutting down it is possible to get an
//	// "unable to complete request to [destination]: context canceled" error
//	// for which the message might not have been given a fair chance and needs
//	// to be attempted again upon subsequent restart.
//	//
//	// This is different from the Consolidated KafkaChannel implementation
//	// which only returns true if message was delivered successfully.
//	//
//	markMessage := true
//	if err != nil && strings.Contains(err.Error(), context.Canceled.Error()) {
//		markMessage = false
//	}
//
//	// Return The Results Of Handling The ConsumerMessage
//	return markMessage, err // TODO - Not certain we will be able to return errors if they cause status to change!
//}
//
//// SetReady is used by the "Prober" implementation for tracking ConsumerGroup
//// status which we are not using at the moment, and is believed to be
//// undergoing refactor / replacement in favor of using the control-protocol
////  and is thus not supported here yet ; )
//func (h *Handler) SetReady(partition int32, ready bool) {
//	h.Logger.Info("No-Op SetReady Handler", zap.Int32("Partition", partition), zap.Bool("Ready", ready))
//}
//
//// GetConsumerGroup returns the ConsumerGroup ID of the Handler
//func (h *Handler) GetConsumerGroup() string {
//	return h.GroupId
//}
//
//// executionInfoWrapper wraps a DispatchExecutionInfo struct so that zap.Any can lazily marshal it
//type executionInfoWrapper struct {
//	*channel.DispatchExecutionInfo
//}
//
//// MarshalLogObject implements the zapcore.ObjectMarshaler interface on the executionInfoWrapper
//func (w executionInfoWrapper) MarshalLogObject(enc zapcore.ObjectEncoder) error {
//	enc.AddDuration("Time", w.Time)
//	enc.AddInt("ResponseCode", w.ResponseCode)
//	if len(w.ResponseBody) > 500 {
//		enc.AddString("Body", string(w.ResponseBody[:500])+"...")
//	} else {
//		enc.AddString("Body", string(w.ResponseBody))
//	}
//	return nil
//}
