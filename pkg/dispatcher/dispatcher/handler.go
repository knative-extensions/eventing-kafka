package dispatcher

import (
	"context"
	"errors"
	"github.com/Shopify/sarama"
	kafkasaramaprotocol "github.com/cloudevents/sdk-go/protocol/kafka_sarama/v2"
	"github.com/cloudevents/sdk-go/v2/binding"
	"github.com/slok/goresilience/retry"
	"go.uber.org/zap"
	eventingduck "knative.dev/eventing/pkg/apis/duck/v1alpha1"
	"knative.dev/eventing/pkg/channel"
	"math"
	"net/url"
	"regexp"
	"strconv"
	"time"
)

// 3 Digit Word Boundary HTTP Status Code Regular Expression
var HttpStatusCodeRegExp = regexp.MustCompile("(^|\\s)([12345]\\d{2})(\\s|$)")

// Verify The Handler Implements The Sarama ConsumerGroupHandler
var _ sarama.ConsumerGroupHandler = &Handler{}

// Define A Sarama ConsumerGroupHandler Implementation
type Handler struct {
	Logger               *zap.Logger
	Subscriber           *eventingduck.SubscriberSpec
	MessageDispatcher    channel.MessageDispatcher
	ExponentialBackoff   bool
	InitialRetryInterval int64
	MaxRetryTime         int64
}

// Create A New Handler
func NewHandler(logger *zap.Logger, subscriber *eventingduck.SubscriberSpec, exponentialBackoff bool, initialRetryInterval int64, maxRetryTime int64) *Handler {
	return &Handler{
		Logger:               logger,
		Subscriber:           subscriber,
		MessageDispatcher:    newMessageDispatcherWrapper(logger),
		ExponentialBackoff:   exponentialBackoff,
		InitialRetryInterval: initialRetryInterval,
		MaxRetryTime:         maxRetryTime,
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

// ConsumerGroupHandler Lifecycle Method (Main processing loop, must finish when Messages() channel closes.)
func (h *Handler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	// TODO - use pkg/resolver/URIResolver to support object reference
	// Extract The Relevant Knative Subscription Event URLs
	var destinationURL *url.URL
	if !h.Subscriber.SubscriberURI.IsEmpty() {
		destinationURL = h.Subscriber.SubscriberURI.URL()
	}
	var replyURL *url.URL
	if !h.Subscriber.ReplyURI.IsEmpty() {
		replyURL = h.Subscriber.ReplyURI.URL()
	}
	var deadLetterURL *url.URL
	if h.Subscriber.Delivery != nil && h.Subscriber.Delivery.DeadLetterSink != nil && !h.Subscriber.Delivery.DeadLetterSink.URI.IsEmpty() {
		deadLetterURL = h.Subscriber.Delivery.DeadLetterSink.URI.URL()
	}

	// Pull Any Available Messages From The ConsumerGroupClaim
	for message := range claim.Messages() {

		// Consume The Message (Ignore Errors - Will have already been retried and we're moving on so as not to block further Topic processing.)
		_ = h.consumeMessage(message, destinationURL, replyURL, deadLetterURL)

		// Mark The Message As Having Been Consumed (Does Not Imply Successful Delivery - Only Full Retry Attempts Made)
		session.MarkMessage(message, "")
	}

	// Return Success
	return nil
}

// Consume A Single Message
func (h *Handler) consumeMessage(consumerMessage *sarama.ConsumerMessage, destinationURL *url.URL, replyURL *url.URL, deadLetterURL *url.URL) error {

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

	// Create A New Retry Runner With Configured Backoff Behavior
	retryRunner := retry.New(
		retry.Config{
			DisableBackoff: !h.ExponentialBackoff, // Note Negation: Env Var Is Whether To Enable & Retry Config Is Whether To Disable ; )
			Times:          h.calculateNumberOfRetries(),
			WaitBase:       time.Millisecond * time.Duration(h.InitialRetryInterval),
		},
	)

	// Attempt To Dispatch The CloudEvent Message Via Knative Message Dispatcher With Retry Wrapper
	err := retryRunner.Run(context.Background(), func(ctx context.Context) error {
		return h.sendMessage(ctx, message, destinationURL, replyURL)
	})

	// Retries Failed
	if err != nil {

		// If The DeadLetterSink Is Configured
		if deadLetterURL != nil {

			// Then Make One Final Attempt With It & Return Result
			h.Logger.Info("Making One Final Attempt With DeadLetterSink Configured")
			err := h.MessageDispatcher.DispatchMessage(context.Background(), message, nil, destinationURL, replyURL, deadLetterURL)
			if err != nil {
				h.Logger.Error("Failed To Send Final Attempt With DeadLetterSink", zap.Error(err))
			}
			return err

		} else {

			// Otherwise Return Original Retry Failure (DeadLetter Sink Not Configured)
			h.Logger.Error("Failed To Send After Configured Number Of Retries - DeadLetterSink Not Configured", zap.Error(err))
			return err
		}
	}

	// Return Success
	return nil
}

// Message Sending Functionality For Use Within Retry Runner (With Custom StatusCode Handling)
func (h *Handler) sendMessage(ctx context.Context, message *kafkasaramaprotocol.Message, destinationURL *url.URL, replyURL *url.URL) error {

	// Dispatch The Message
	err := h.MessageDispatcher.DispatchMessage(ctx, message, nil, destinationURL, replyURL, nil)
	if err == nil {

		// No Error - Return Success
		h.Logger.Debug("Successfully Sent Cloud Event To Subscription!")
		return nil

	} else {

		// An Error Occurred - Parse The StatusCode & Add To Logger
		statusCode := h.parseHttpStatusCodeFromError(err)
		logger := h.Logger.With(zap.Error(err), zap.Int("StatusCode", statusCode))

		//
		// Note - Normally we would NOT want to retry 400 responses, BUT the knative-eventing
		//        filter handler (due to CloudEvents SDK V1 usage) is swallowing the actual
		//        status codes from the subscriber and returning 400s instead.  Once this has,
		//        been resolved we can remove 400 from the list of codes to retry.
		//
		if statusCode >= 500 || statusCode == 400 || statusCode == 404 || statusCode == 429 {
			logger.Warn("Failed To Send Message To Subscriber Service, Retrying")
			return errors.New("server returned a bad response code")
		} else if statusCode >= 300 {
			logger.Warn("Failed To Send Message To Subscriber Service, Not Retrying")
		} else if statusCode == -1 {
			logger.Warn("No StatusCode Detected In Error, Retrying")
			return errors.New("no response code detected in error, retrying")
		}

		// Do Not Retry 1XX, 2XX, & Most 4XX StatusCode Responses
		return nil
	}
}

// Calculate Approximate Retry Count That Can Be Done ~MaxRetryTime Based On Exponential Backoff
func (h *Handler) calculateNumberOfRetries() int {
	if h.ExponentialBackoff {
		return int(math.Round(math.Log2(float64(h.MaxRetryTime) / float64(h.InitialRetryInterval))))
	} else {
		return int(h.MaxRetryTime / h.InitialRetryInterval)
	}
}

//
// Parse The HTTP Status Code From The Specified Error - HACK ALERT!
//
// This is necessary due to the Knative Eventing Channel's MessageDispatcher implementation
// NOT returning the HTTP Status Code other than as part of the error string, combined with
// the DispatchMessage() functionality not supporting retry.  We do, though, want to use their
// implementation in order to align with standards and take advantage of tracing logic.
// Therefore, since we are providing our own wrapping retry logic, which depends on the
// resulting HTTP Status Code, we're forced to parse it from the error text here.  It is
// hoped that we can provide a better solution directly in the knative channel implementation
// in the near future.
//
func (h *Handler) parseHttpStatusCodeFromError(err error) int {

	// Default Value Indicates The HTTP Status Code Was Not Found In Error
	statusCode := -1

	// Validate The Error
	if err != nil && len(err.Error()) >= 3 {

		// Get The Error String Value
		errString := err.Error()

		// Match/Group Any 3 Digit Status Code SubString From The Error String
		statusCodeStringSubMatch := HttpStatusCodeRegExp.FindAllStringSubmatch(errString, -1)

		// If A Match Was Found
		if len(statusCodeStringSubMatch) >= 1 {

			// Log Warning If Multiple Potential HTTP StatusCodes Detected
			if len(statusCodeStringSubMatch) > 1 {
				h.Logger.Warn("Encountered Multiple Possible HTTP Status Codes In Error String - Using First One")
			}

			// Take The First Potential HTTP StatusCode (For lack anything more sophisticated ;)
			statusCodeString := statusCodeStringSubMatch[0][2]

			// And Convert To An Int
			code, conversionErr := strconv.Atoi(statusCodeString)
			if conversionErr != nil {
				// Conversion Error - Should Be Impossible Due To RegExp Match - But Log A Warning Just To Be Safe ; )
				h.Logger.Warn("Failed To Convert Parsed HTTP Status Code String To Int", zap.String("HTTP Status Code String", statusCodeString), zap.Error(conversionErr))
			} else {
				statusCode = code
			}
		}
	}

	// Return The HTTP Status Code
	return statusCode
}
