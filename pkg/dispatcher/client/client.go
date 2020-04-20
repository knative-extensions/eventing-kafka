package client

import (
	"context"
	cloudevents "github.com/cloudevents/sdk-go/v1"
	cloudeventhttp "github.com/cloudevents/sdk-go/v1/cloudevents/transport/http"
	"github.com/pkg/errors"
	"github.com/slok/goresilience/retry"
	"go.uber.org/zap"
	"knative.dev/eventing/pkg/kncloudevents"
	"knative.dev/pkg/tracing"
	"math"
	"net/http"
	"strconv"
	"time"
)

// Create a shared go http client with a timeout
var httpClient = &http.Client{
	Timeout: 30 * time.Second,
}

// Client represents anything that can dispatch an event
// to a downstream service
type RetriableClient interface {
	Dispatch(message cloudevents.Event, uri string) error
}

// retriableCloudEventClient is a client implementation that interprets
// kafka messages as cloud events and utilizes the cloud event library
// and supports retries with exponential backoff
type retriableCloudEventClient struct {
	logger               *zap.Logger
	exponentialBackoff   bool
	initialRetryInterval int64
	maxRetryTime         int64
	cloudEventClient     cloudevents.Client
}

var _ RetriableClient = &retriableCloudEventClient{}

func NewRetriableCloudEventClient(logger *zap.Logger, exponentialBackoff bool, initialRetryInterval int64, maxRetryTime int64) retriableCloudEventClient {

	//
	// TODO - Previously we we're adding cloud events middleware for tracing.  This implementation is still
	//        based on CloudEvents V1, despite being based on knative eventing release-0.14 which has moved
	//        on to CloudEvents V2.  This will all need to be re-done once we migrate to CloudEvents V2 and
	//        re-evaluate their support for event tracing.
	//

	tOpts := []cloudeventhttp.Option{
		cloudevents.WithBinaryEncoding(),
		cloudevents.WithMiddleware(tracing.HTTPSpanMiddleware),
	}

	// Make an http transport for the CloudEvents client.
	transport, err := cloudevents.NewHTTPTransport(tOpts...)
	if err != nil {
		panic("Failed To Create Transport, " + err.Error())
	}
	transport.Client = httpClient

	ceClient, err := kncloudevents.NewDefaultHTTPClient(transport)
	if err != nil {
		panic("Unable To Create KnativeCloudEvent Client: " + err.Error())
	}

	return retriableCloudEventClient{
		logger:               logger,
		exponentialBackoff:   exponentialBackoff,
		initialRetryInterval: initialRetryInterval,
		maxRetryTime:         maxRetryTime,
		cloudEventClient:     ceClient,
	}
}

func (rcec retriableCloudEventClient) Dispatch(event cloudevents.Event, uri string) error {

	// Configure The Logger
	var logger *zap.Logger
	if rcec.logger.Core().Enabled(zap.DebugLevel) {
		logger = rcec.logger.With(zap.String("Event", event.String()), zap.String("uri", uri))
	} else {
		logger = rcec.logger.With(zap.String("uri", uri))
	}

	// Build the runner for retry capabilities
	runner := retry.New(retry.Config{DisableBackoff: rcec.exponentialBackoff, Times: rcec.calculateNumberOfRetries(), WaitBase: time.Millisecond * time.Duration(rcec.initialRetryInterval)})

	// Build the sending context for the event
	sendingCtx := cloudevents.ContextWithTarget(context.Background(), uri)

	//
	// TODO - Previously we were manually adding tracing information to the HTTP conext from the event.
	//        It is hoped that when we convert this implementation to CloudEvents V2 that this will be
	//        handled for us.  The AddSpanFromTraceparentAttribute() function was removed from knative
	//        after the release-0.13 branch.
	//
	//sendingCtx, err := knativeeventingtracing.AddSpanFromTraceparentAttribute(sendingCtx, uri, event)
	//if err != nil {
	//	logger.Error("Unable to connect outgoing span", zap.Error(err))
	//}

	err := runner.Run(sendingCtx, func(ctx context.Context) error {
		responseContext, _, err := rcec.cloudEventClient.Send(sendingCtx, event)
		transportContext := cloudevents.HTTPTransportContextFrom(responseContext)
		return logResponse(logger, transportContext.StatusCode, err)
	})

	// Retries failed
	if err != nil {
		logger.Error("Failed to send after configured number of retries", zap.Error(err))
		return err
	}
	return nil
}

func logResponse(logger *zap.Logger, statusCode int, err error) error {
	if statusCode >= 500 || statusCode == 404 || statusCode == 429 {
		logger.Warn("Failed to send message to subscriber service, retrying", zap.Int("statusCode", statusCode))
		return errors.New("Server returned a bad response code: " + strconv.Itoa(statusCode))
	} else if statusCode > 299 {
		logger.Warn("Failed to send message to subscriber service, not retrying", zap.Int("statusCode", statusCode))
	} else if statusCode == 0 {
		return errors.Wrap(err, "Validation Error")
	} else {
		logger.Debug("Successfully sent message to subscriber service", zap.Int("statusCode", statusCode))
	}
	return nil
}

// Convert defined max retry time to the approximate number
// of retries, taking into account the exponential backoff algorithm
func (rcec retriableCloudEventClient) calculateNumberOfRetries() int {
	return int(math.Round(math.Log2(float64(rcec.maxRetryTime)/float64(rcec.initialRetryInterval))) + 1)
}
