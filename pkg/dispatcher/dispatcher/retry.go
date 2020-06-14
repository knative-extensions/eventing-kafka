package dispatcher

import (
	"context"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/binding"
	"github.com/pkg/errors"
	"github.com/slok/goresilience/retry"
	"go.uber.org/zap"
	"math"
	"net/url"
	"regexp"
	"strconv"
	"time"
)

// 3 Digit Word Boundary HTTP Status Code Regular Expression
var HttpStatusCodeRegExp = regexp.MustCompile("(^|\\s)([12345]\\d{2})(\\s|$)")

// Dispatch A Cloud Event To The Specified Destination URL
func (d *Dispatcher) Dispatch(event *cloudevents.Event, subscription Subscription) error {

	// Configure The Logger
	logger := d.Logger.With(zap.String("Subscriber URI", subscription.SubscriberURI.String()))
	if d.Logger.Core().Enabled(zap.DebugLevel) {
		logger = d.Logger.With(zap.String("Event", event.String()))
	}

	timesOfRetries := d.calculateNumberOfRetries()

	// Create A New Retry Runner With Configured Backoff Behavior
	retryRunner := NewMiddleware(
		retry.Config{
			DisableBackoff: !d.ExponentialBackoff, // Note Negation: Env Var Is Whether To Enable & Retry Config Is Whether To Disable ; )
			Times:          timesOfRetries,
			WaitBase:       time.Millisecond * time.Duration(d.InitialRetryInterval),
		},
	)(nil)

	//
	// Convert The CloudEvent To A binding/Message
	//
	// TODO - It is potentially inefficient to take the KafkaMessage which we've already turned into a Cloud Event, only
	//        to re-convert it into a binding/Message.  The current implementation is based on CloudEvent Events, however,
	//        and until a "protocol" implementation for Confluent Kafka exists this is the simplest path forward.  Once
	//        such a protocol implementation exists, it would be more efficient to convert directly from the KafkaMessage
	//        to a binding/Message.
	//
	message := binding.ToMessage(event)

	// Extract The Relevant Knative Subscription Event URLs
	var destinationURL *url.URL
	if !subscription.SubscriberURI.IsEmpty() {
		destinationURL = subscription.SubscriberURI.URL()
	}
	var replyURL *url.URL
	if !subscription.ReplyURI.IsEmpty() {
		replyURL = subscription.ReplyURI.URL()
	}
	var deadLetterURL *url.URL
	if subscription.Delivery != nil && subscription.Delivery.DeadLetterSink != nil && !subscription.Delivery.DeadLetterSink.URI.IsEmpty() {
		// TODO - Currently ignoring dead-letter configuration due to wrapping retry implementation - would send one deadletter for every retry :(
		deadLetterURL = subscription.Delivery.DeadLetterSink.URI.URL()
		// logger.Warn("Subscription Delivery DeadLetterSink Not Currently Supported!")
	}

	// Attempt To Dispatch The CloudEvent Message Via Knative Message Dispatcher With Retry Wrapper
	err := retryRunner.Run(context.Background(), func(ctx context.Context) error {
		retryTime := ctx.Value("retries").(Retries).times

		var err error
		if retryTime == timesOfRetries {
			err = d.messageDispatcher.DispatchMessage(ctx, message, nil, destinationURL, replyURL, deadLetterURL)
		} else {
			err = d.messageDispatcher.DispatchMessage(ctx, message, nil, destinationURL, replyURL, nil)
		}

		return d.logResponse(err)
	})

	// Retries failed
	if err != nil {
		logger.Error("Failed to send after configured number of retries", zap.Error(err))
		return err
	}

	// Return Success
	return nil
}

// Utility Function For Logging The Response From A Dispatch Request
func (d *Dispatcher) logResponse(err error) error {

	if err == nil {
		d.Logger.Debug("Successfully Sent Cloud Event To Subscription")
		return nil
	} else {
		statusCode := d.parseHttpStatusCodeFromError(err)
		logger := d.Logger.With(zap.Error(err), zap.Int("StatusCode", statusCode))

		//
		// Note - Normally we would NOT want to retry 400 responses, BUT the knative-eventing
		//        filter handler (due to CloudEvents SDK V1 usage) is swallowing the actual
		//        status codes from the subscriber and returning 400s instead.  Once this has,
		//        been resolved we can remove 400 from the list of codes to retry.
		//
		if statusCode >= 500 || statusCode == 400 || statusCode == 404 || statusCode == 429 {
			logger.Warn("Failed to send message to subscriber service, retrying")
			return errors.New("Server returned a bad response code")
		} else if statusCode >= 300 {
			logger.Warn("Failed to send message to subscriber service, not retrying")
		} else if statusCode == -1 {
			logger.Warn("No response code detected in error, retrying")
			return errors.New("No response code detected in error, retrying")
		}
		return nil
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
func (d *Dispatcher) parseHttpStatusCodeFromError(err error) int {

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
				d.Logger.Warn("Encountered Multiple Possible HTTP Status Codes In Error String - Using First One")
			}

			// Take The First Potential HTTP StatusCode (For lack anything more sophisticated ;)
			statusCodeString := statusCodeStringSubMatch[0][2]

			// And Convert To An Int
			code, conversionErr := strconv.Atoi(statusCodeString)
			if conversionErr != nil {
				// Conversion Error - Should Be Impossible Due To RegExp Match - But Log A Warning Just To Be Safe ; )
				d.Logger.Warn("Failed To Convert Parsed HTTP Status Code String To Int", zap.String("HTTP Status Code String", statusCodeString), zap.Error(conversionErr))
			} else {
				statusCode = code
			}
		}
	}

	// Return The HTTP Status Code
	return statusCode
}

// Determine the approximate number of retries that will take around maxRetryTime,
// depending on whether exponential backoff is enabled
func (d *Dispatcher) calculateNumberOfRetries() int {
	if d.ExponentialBackoff {
		return int(math.Round(math.Log2(float64(d.MaxRetryTime) / float64(d.InitialRetryInterval))))
	} else {
		return int(d.MaxRetryTime / d.InitialRetryInterval)
	}
}
