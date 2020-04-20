package health

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"io"
	logtesting "knative.dev/pkg/logging/testing"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

// Test Constants
const (
	testHttpPort  = "8089"
	testHttpHost  = "localhost"
	livenessPath  = "/healthz"
	readinessPath = "/healthy"
)

// Test Struct That Implements The HealthInterface Functions
type testStatus struct {
	server *Server
}

// Mock Function That Returns The Mock Status "Alive" Flag
func (ts *testStatus) Alive() bool {
	return ts.server.Alive()
}

// Mock Function That Returns True For Readiness Flag
func (ts *testStatus) Ready() bool {
	return true
}

// Mock Status For Starting Health Server
var mockStatus testStatus

func getTestHealthServer() *Server {
	health := NewHealthServer(testHttpPort, &mockStatus)
	mockStatus.server = health
	return health
}

// Test The NewHealthServer() Functionality
func TestNewHealthServer(t *testing.T) {

	// Create A Health Server
	health := getTestHealthServer()

	// Validate The EventProxy
	assert.NotNil(t, health)
	assert.NotNil(t, health.server)
	assert.Equal(t, ":"+testHttpPort, health.server.Addr)
	assert.Equal(t, false, health.alive)
}

// Test Flag Set And Reset Functions
func TestFlagWrites(t *testing.T) {

	// Create A New Health Server
	health := getTestHealthServer()

	// Test Liveness Flag
	health.SetAlive(false)
	assert.Equal(t, false, health.Alive())
	health.SetAlive(true)
	assert.Equal(t, true, health.Alive())

	// Verify that Readiness is true by default
	assert.Equal(t, true, health.status.Ready())
}

// Test Unsupported Events Requests
func TestUnsupportedEventsRequests(t *testing.T) {

	// Create A New Health Server
	health := getTestHealthServer()

	// Test All Unsupported Events Requests
	performUnsupportedMethodRequestTest(t, http.MethodConnect, livenessPath, health.handleLiveness)
	performUnsupportedMethodRequestTest(t, http.MethodDelete, livenessPath, health.handleLiveness)
	performUnsupportedMethodRequestTest(t, http.MethodPost, livenessPath, health.handleLiveness)
	performUnsupportedMethodRequestTest(t, http.MethodOptions, livenessPath, health.handleLiveness)
	performUnsupportedMethodRequestTest(t, http.MethodPatch, livenessPath, health.handleLiveness)
	performUnsupportedMethodRequestTest(t, http.MethodPut, livenessPath, health.handleLiveness)
	performUnsupportedMethodRequestTest(t, http.MethodTrace, livenessPath, health.handleLiveness)

	performUnsupportedMethodRequestTest(t, http.MethodConnect, readinessPath, health.handleReadiness)
	performUnsupportedMethodRequestTest(t, http.MethodDelete, readinessPath, health.handleReadiness)
	performUnsupportedMethodRequestTest(t, http.MethodPost, readinessPath, health.handleReadiness)
	performUnsupportedMethodRequestTest(t, http.MethodOptions, readinessPath, health.handleReadiness)
	performUnsupportedMethodRequestTest(t, http.MethodPatch, readinessPath, health.handleReadiness)
	performUnsupportedMethodRequestTest(t, http.MethodPut, readinessPath, health.handleReadiness)
	performUnsupportedMethodRequestTest(t, http.MethodTrace, readinessPath, health.handleReadiness)
}

// Test The Health Server Via The HTTP Handlers
func TestHealthHandler(t *testing.T) {

	// Create A New Health Server
	health := getTestHealthServer()

	// Verify that initially the statuses are not live / ready
	getEventToHandler(t, health.handleLiveness, livenessPath, http.StatusInternalServerError)
	getEventToHandler(t, health.handleReadiness, readinessPath, http.StatusOK)

	// Verify that the liveness status follows the health.Alive flag
	health.SetAlive(true)
	getEventToHandler(t, health.handleLiveness, livenessPath, http.StatusOK)

	// Verify that the shutdown process sets liveness to false
	health.Shutdown()
	getEventToHandler(t, health.handleLiveness, livenessPath, http.StatusInternalServerError)
}

// Test The Health Server Via Live HTTP Calls
func TestHealthServer(t *testing.T) {

	logger := logtesting.TestLogger(t).Desugar()

	health := getTestHealthServer()
	health.Start(logger)

	livenessUri, err := url.Parse(fmt.Sprintf("http://%s:%s%s", testHttpHost, testHttpPort, livenessPath))
	assert.Nil(t, err)
	readinessUri, err := url.Parse(fmt.Sprintf("http://%s:%s%s", testHttpHost, testHttpPort, readinessPath))
	assert.Nil(t, err)
	waitServerReady(readinessUri.String(), 3*time.Second)

	// Test basic functionality - advanced logical tests are in TestHealthHandler
	getEventToServer(t, livenessUri, http.StatusInternalServerError)
	getEventToServer(t, readinessUri, http.StatusOK)
	health.SetAlive(true)
	getEventToServer(t, livenessUri, http.StatusOK)

	health.Stop(logger)

	// Pause to let async go process finish logging :(
	// Appears to be race condition between test finishing and logging in health.Stop() above
	time.Sleep(1 * time.Second)
}

//
// Private Utility Functions
//

// Waits Until A GET Request Succeeds (Or Times Out)
func waitServerReady(uri string, timeout time.Duration) {
	// Create An HTTP Client And Send The Request Until Success Or Timeout
	client := http.DefaultClient
	for start := time.Now(); time.Since(start) < timeout; {
		_, err := client.Get(uri) // Don't care what the response actually is, only if there was an error getting it
		if err == nil {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// Sends A Simple GET Event To A URL Expecting A Specific Response Code
func getEventToServer(t *testing.T, uri *url.URL, expectedStatus int) {

	// Create An HTTP Client And Send The Request
	client := http.DefaultClient
	resp, err := client.Get(uri.String())

	// Verify The Client Response Is As Expected
	assert.NotNil(t, resp)
	assert.Nil(t, err)
	assert.Equal(t, expectedStatus, resp.StatusCode)
}

// Sends A Request To An HTTP Response Recorder Directly Expecting A Specific Response Code
func getEventToHandler(t *testing.T, handlerFunc http.HandlerFunc, path string, expectedStatus int) {
	// Create A Test HTTP GET Request For requested path
	request := createNewRequest(t, "GET", path, nil)

	// Create An HTTP ResponseRecorder & Handler For Request
	responseRecorder := httptest.NewRecorder()
	handler := handlerFunc

	// Call The HTTP Request Handler Function For Path
	handler.ServeHTTP(responseRecorder, request)

	// Verify The StatusMethodNotAllowed Response Code Is Returned
	statusCode := responseRecorder.Code
	assert.Equal(t, expectedStatus, statusCode)

}

// Create A Test HTTP Request For The Specified Method / Path
func createNewRequest(t *testing.T, method string, path string, body io.Reader) *http.Request {
	request, err := http.NewRequest(method, path, body)
	assert.Nil(t, err)
	return request
}

// Perform An Unsupported Method Request Test To Specified Path/Handler
func performUnsupportedMethodRequestTest(t *testing.T, method string, path string, handlerFunc http.HandlerFunc) {

	// Create A Test HTTP POST Request For Metrics Path
	request := createNewRequest(t, method, path, nil)

	// Create An HTTP ResponseRecorder & Handler For Request
	responseRecorder := httptest.NewRecorder()
	handler := handlerFunc

	// Call The HTTP Request Handler Function For Path
	handler.ServeHTTP(responseRecorder, request)

	// Verify The StatusMethodNotAllowed Response Code Is Returned
	statusCode := responseRecorder.Code
	assert.Equal(t, http.StatusMethodNotAllowed, statusCode)
}
