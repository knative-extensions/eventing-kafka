package dispatcher

import (
	"fmt"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	eventingduck "knative.dev/eventing/pkg/apis/duck/v1alpha1"
	"knative.dev/pkg/apis"
	logtesting "knative.dev/pkg/logging/testing"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHttpClient_Dispatch(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		description       string
		expectedCallCount int
		expectedSuccess   bool
		handler           func(w http.ResponseWriter, r *http.Request, callCount int)
	}{
		{
			"Basic successful Request",
			1,
			true,
			func(w http.ResponseWriter, r *http.Request, callCount int) {
				w.WriteHeader(http.StatusCreated)
			},
		},
		{
			"Test first 2 calls fail, 3rd succeeds",
			3,
			true,
			func(w http.ResponseWriter, r *http.Request, callCount int) {
				if callCount < 3 {
					w.WriteHeader(http.StatusBadGateway)
				} else {
					w.WriteHeader(http.StatusCreated)
				}
			},
		},
		{
			"Test all retries fail",
			11,
			false,
			func(w http.ResponseWriter, r *http.Request, callCount int) {
				w.WriteHeader(http.StatusNotFound)
			},
		},
		{
			"Test don't retry on 401",
			1,
			true,
			func(w http.ResponseWriter, r *http.Request, callCount int) {
				w.WriteHeader(http.StatusUnauthorized)
			},
		},
		{
			// NOTE: We had to retry on 400 to workaround a knative-eventing bug
			// where the filter service does not pass-through the correct status code

			"Test do retry on 400",
			2,
			true,
			func(w http.ResponseWriter, r *http.Request, callCount int) {
				if callCount == 1 {
					w.WriteHeader(http.StatusBadRequest)
				} else {
					w.WriteHeader(http.StatusCreated)
				}
			},
		},
		{
			"Test do retry on 429",
			2,
			true,
			func(w http.ResponseWriter, r *http.Request, callCount int) {
				if callCount == 1 {
					w.WriteHeader(http.StatusTooManyRequests)
				} else {
					w.WriteHeader(http.StatusCreated)
				}
			},
		},
		{
			"Test do retry on 404",
			2,
			true,
			func(w http.ResponseWriter, r *http.Request, callCount int) {
				if callCount == 1 {
					w.WriteHeader(http.StatusNotFound)
				} else {
					w.WriteHeader(http.StatusCreated)
				}
			},
		},
	}

	for _, tc := range testCases {
		tc := tc // capture range variable
		t.Run(tc.description, func(t *testing.T) {
			t.Parallel()

			dispatcher, server, mux := setup(t)
			defer teardown(server)

			callCount := 0
			mux.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {
				callCount++
				tc.handler(writer, request, callCount)
			})

			testCloudEvent := cloudevents.NewEvent(cloudevents.VersionV03)
			testCloudEvent.SetID("ABC-123")
			testCloudEvent.SetType("com.cloudevents.readme.sent")
			testCloudEvent.SetSource("http://localhost:8080/")
			err := testCloudEvent.SetData("application/json", map[string]string{"test": "value"})
			assert.Nil(t, err)

			subscriberURI, _ := apis.ParseURL(server.URL)

			err = dispatcher.Dispatch(&testCloudEvent, Subscription{SubscriberSpec: eventingduck.SubscriberSpec{SubscriberURI: subscriberURI}})

			if tc.expectedSuccess && err != nil {
				t.Error("Message failed to dispatch:", err)
			} else if !tc.expectedSuccess && err == nil {
				t.Error("Message should have failed to dispatch")
			}

			if callCount != tc.expectedCallCount {
				t.Errorf("Expected to call server %d time, was actually %d times", tc.expectedCallCount, callCount)
			}
		})
	}
}

func setup(t *testing.T) (*Dispatcher, *httptest.Server, *http.ServeMux) {
	// test server
	mux := http.NewServeMux()
	server := httptest.NewServer(mux)

	// Create A New Dispatcher
	dispatcherConfig := DispatcherConfig{
		Logger:                      logtesting.TestLogger(t).Desugar(),
		Brokers:                     testBrokers,
		Topic:                       testTopic,
		Offset:                      testOffset,
		PollTimeoutMillis:           testPollTimeoutMillis,
		OffsetCommitCount:           testOffsetCommitCount,
		OffsetCommitDuration:        testOffsetCommitDuration,
		OffsetCommitDurationMinimum: testOffsetCommitDurationMin,
		Username:                    testUsername,
		Password:                    testPassword,
		ChannelKey:                  testChannelKey,
		ExponentialBackoff:          false,
		InitialRetryInterval:        1000,
		MaxRetryTime:                10000,
	}
	dispatcher := NewDispatcher(dispatcherConfig)

	return dispatcher, server, mux
}

func teardown(server *httptest.Server) {
	server.Close()
}

func TestHttpClient_calculateNumberOfRetries(t *testing.T) {

	type fields struct {
		uri                  string
		exponentialBackoff   bool
		initialRetryInterval int64
		maxNumberRetries     int
		maxRetryTime         int64
		logger               *zap.Logger
	}
	tests := []struct {
		fields fields
		want   int
	}{
		{fields{maxRetryTime: 10000, initialRetryInterval: 1000, exponentialBackoff: true}, 3},
		{fields{maxRetryTime: 10000, initialRetryInterval: 5000, exponentialBackoff: true}, 1},
		{fields{maxRetryTime: 17000, initialRetryInterval: 1000, exponentialBackoff: true}, 4},
		{fields{maxRetryTime: 60000, initialRetryInterval: 5000, exponentialBackoff: true}, 4},
		{fields{maxRetryTime: 300000, initialRetryInterval: 500, exponentialBackoff: true}, 9},
		{fields{maxRetryTime: 300000, initialRetryInterval: 500, exponentialBackoff: false}, 600},
		{fields{maxRetryTime: 10000, initialRetryInterval: 5000, exponentialBackoff: false}, 2},
	}

	// Create A Test Logger
	logger := logtesting.TestLogger(t).Desugar()

	// Loop Over All The Tests
	for _, test := range tests {
		t.Run(fmt.Sprintf("%d max retry, initial interval %d, exponential backoff %t", test.fields.maxRetryTime, test.fields.initialRetryInterval, test.fields.exponentialBackoff), func(t *testing.T) {

			// Create A New Dispatcher
			dispatcherConfig := DispatcherConfig{
				Logger:                      logger,
				Brokers:                     testBrokers,
				Topic:                       testTopic,
				Offset:                      testOffset,
				PollTimeoutMillis:           testPollTimeoutMillis,
				OffsetCommitCount:           testOffsetCommitCount,
				OffsetCommitDuration:        testOffsetCommitDuration,
				OffsetCommitDurationMinimum: testOffsetCommitDurationMin,
				Username:                    testUsername,
				Password:                    testPassword,
				ChannelKey:                  testChannelKey,
				ExponentialBackoff:          test.fields.exponentialBackoff,
				InitialRetryInterval:        test.fields.initialRetryInterval,
				MaxRetryTime:                test.fields.maxRetryTime,
			}
			testDispatcher := NewDispatcher(dispatcherConfig)

			// Perform The Test
			if got := testDispatcher.calculateNumberOfRetries(); got != test.want {
				t.Errorf("httpClient.calculateNumberOfRetries() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestLogResponse(t *testing.T) {

	// Test Data
	noStatusCodeError := errors.New("No response code detected in error, retrying")
	badResponseError := errors.New("Server returned a bad response code")

	// Define TestCase Type
	type testCase struct {
		errIn  error
		errOut error
	}

	// Create The Set Of TestCases
	tests := []testCase{

		{errIn: nil, errOut: nil},

		{errIn: errors.New(""), errOut: noStatusCodeError},
		{errIn: errors.New("no status code"), errOut: noStatusCodeError},

		{errIn: errors.New("100"), errOut: nil},
		{errIn: errors.New("200"), errOut: nil},

		{errIn: errors.New("300"), errOut: nil},
		{errIn: errors.New("301"), errOut: nil},
		{errIn: errors.New("399"), errOut: nil},

		{errIn: errors.New("400"), errOut: badResponseError},
		{errIn: errors.New("401"), errOut: nil},
		{errIn: errors.New("499"), errOut: nil},

		{errIn: errors.New("404"), errOut: badResponseError},
		{errIn: errors.New("429"), errOut: badResponseError},

		{errIn: errors.New("500"), errOut: badResponseError},
		{errIn: errors.New("503"), errOut: badResponseError},
		{errIn: errors.New("599"), errOut: badResponseError},
	}

	// Create A Test Logger
	logger := logtesting.TestLogger(t).Desugar()

	// Create A New Dispatcher
	dispatcherConfig := DispatcherConfig{
		Logger:                      logger,
		Brokers:                     testBrokers,
		Topic:                       testTopic,
		Offset:                      testOffset,
		PollTimeoutMillis:           testPollTimeoutMillis,
		OffsetCommitCount:           testOffsetCommitCount,
		OffsetCommitDuration:        testOffsetCommitDuration,
		OffsetCommitDurationMinimum: testOffsetCommitDurationMin,
		Username:                    testUsername,
		Password:                    testPassword,
		ChannelKey:                  testChannelKey,
		ExponentialBackoff:          testExponentialBackoff,
		InitialRetryInterval:        testInitialRetryInterval,
		MaxRetryTime:                testMaxRetryTime,
	}
	testDispatcher := NewDispatcher(dispatcherConfig)

	// Loop Over The TestCases
	for _, test := range tests {

		// Perform The Specific TestCase
		actualErrOut := testDispatcher.logResponse(test.errIn)

		// Verify Results
		if test.errOut == nil {
			assert.Nil(t, actualErrOut)
		} else {
			assert.NotNil(t, actualErrOut)
			assert.Equal(t, test.errOut.Error(), actualErrOut.Error())
		}
	}
}

func TestParseHttpStatusCodeFromError(t *testing.T) {

	// Define TestCase Type
	type testCase struct {
		error error
		code  int
	}

	// Create The Set Of TestCases
	tests := []testCase{
		{error: nil, code: -1},
		{error: errors.New(""), code: -1},
		{error: errors.New("no status codes to see here"), code: -1},
		{error: errors.New("status code without leading200 word boundary"), code: -1},
		{error: errors.New("status code without both200word boundary"), code: -1},
		{error: errors.New("status code without 200trailing word boundary"), code: -1},
		{error: errors.New("200 status code at start"), code: 200},
		{error: errors.New("status code 200 in middle"), code: 200},
		{error: errors.New("status code at end 200"), code: 200},
		{error: errors.New("multiple 200 status codes 300 selects first"), code: 200},
		{error: errors.New("unable to complete request to http://sample-event-proxy-300-stage.svc.cluster.local/: unexpected HTTP response, expected 2xx, got 500"), code: 500},
	}

	// Create A Test Logger
	logger := logtesting.TestLogger(t).Desugar()

	// Create A New Dispatcher
	dispatcherConfig := DispatcherConfig{
		Logger:                      logger,
		Brokers:                     testBrokers,
		Topic:                       testTopic,
		Offset:                      testOffset,
		PollTimeoutMillis:           testPollTimeoutMillis,
		OffsetCommitCount:           testOffsetCommitCount,
		OffsetCommitDuration:        testOffsetCommitDuration,
		OffsetCommitDurationMinimum: testOffsetCommitDurationMin,
		Username:                    testUsername,
		Password:                    testPassword,
		ChannelKey:                  testChannelKey,
		ExponentialBackoff:          testExponentialBackoff,
		InitialRetryInterval:        testInitialRetryInterval,
		MaxRetryTime:                testMaxRetryTime,
	}
	testDispatcher := NewDispatcher(dispatcherConfig)

	// Loop Over The TestCases
	for _, test := range tests {

		// Perform The Specific TestCase
		actualStatusCode := testDispatcher.parseHttpStatusCodeFromError(test.error)

		// Verify Results
		assert.Equal(t, test.code, actualStatusCode)
	}
}
