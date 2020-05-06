package client

import (
	"errors"
	"fmt"
	cloudevents "github.com/cloudevents/sdk-go/v1"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
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
			5,
			false,
			func(w http.ResponseWriter, r *http.Request, callCount int) {
				w.WriteHeader(http.StatusNotFound)
			},
		},
		{
			"Test don't retry on 400",
			1,
			true,
			func(w http.ResponseWriter, r *http.Request, callCount int) {
				w.WriteHeader(http.StatusBadRequest)
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

			client, server, mux := setup(t)
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
			testCloudEvent.SetDataContentType("application/json")
			err := testCloudEvent.SetData(map[string]string{"test": "value"})
			assert.Nil(t, err)

			err = client.Dispatch(testCloudEvent, server.URL)

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

func setup(t *testing.T) (*retriableCloudEventClient, *httptest.Server, *http.ServeMux) {
	// test server
	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	client := NewRetriableCloudEventClient(logtesting.TestLogger(t).Desugar(), true, 1000, 10000)

	return &client, server, mux
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
		{fields{maxRetryTime: 10000, initialRetryInterval: 1000}, 4},
		{fields{maxRetryTime: 10000, initialRetryInterval: 5000}, 2},
		{fields{maxRetryTime: 17000, initialRetryInterval: 1000}, 5},
		{fields{maxRetryTime: 60000, initialRetryInterval: 5000}, 5},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%d max retry, initial interval %d", tt.fields.maxRetryTime, tt.fields.initialRetryInterval), func(t *testing.T) {
			hc := retriableCloudEventClient{
				exponentialBackoff:   tt.fields.exponentialBackoff,
				initialRetryInterval: tt.fields.initialRetryInterval,
				maxRetryTime:         tt.fields.maxRetryTime,
			}
			if got := hc.calculateNumberOfRetries(); got != tt.want {
				t.Errorf("httpClient.calculateNumberOfRetries() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_logResponse(t *testing.T) {

	logger := logtesting.TestLogger(t).Desugar()

	type args struct {
		logger     *zap.Logger
		statusCode int
		err        error
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "200",
			args: args{
				logger:     logger,
				statusCode: 200,
				err:        nil,
			},
			wantErr: false,
		},
		{
			name: "429",
			args: args{
				logger:     logger,
				statusCode: 429,
				err:        nil,
			},
			wantErr: true,
		},
		{
			name: "503",
			args: args{
				logger:     logger,
				statusCode: 503,
				err:        nil,
			},
			wantErr: true,
		},
		{
			name: "Validation Error",
			args: args{
				logger:     logger,
				statusCode: 0,
				err:        errors.New("validation error"),
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := logResponse(tt.args.logger, tt.args.statusCode, tt.args.err); (err != nil) != tt.wantErr {
				t.Errorf("logResponse() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
