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

package health

import (
	"github.com/stretchr/testify/assert"
	"io"
	logtesting "knative.dev/pkg/logging/testing"
	"net/http"
	"net/http/httptest"
	"testing"
)

// Test Constants
const (
	livenessPath  = "/healthz"
	readinessPath = "/healthy"
)

// Test Struct That Implements The HealthInterface Functions
type testStatus struct {
	server  *Server
	IsReady bool
}

// Mock Function That Returns The Mock Status "Alive" Flag
func (ts *testStatus) Alive() bool {
	return ts.server.Alive()
}

// Mock Function That Returns True For Readiness Flag
func (ts *testStatus) Ready() bool {
	return ts.IsReady
}

// Mock Status For Starting Health Server
var mockStatus testStatus

func getTestHealthServer() *Server {
	mockStatus.IsReady = true
	health := NewHealthServer("0", &mockStatus)
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
	assert.Equal(t, ":"+health.HttpPort, health.server.Addr)
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
	performUnsupportedMethodRequestTest(t, http.MethodConnect, livenessPath, health.HandleLiveness)
	performUnsupportedMethodRequestTest(t, http.MethodDelete, livenessPath, health.HandleLiveness)
	performUnsupportedMethodRequestTest(t, http.MethodPost, livenessPath, health.HandleLiveness)
	performUnsupportedMethodRequestTest(t, http.MethodOptions, livenessPath, health.HandleLiveness)
	performUnsupportedMethodRequestTest(t, http.MethodPatch, livenessPath, health.HandleLiveness)
	performUnsupportedMethodRequestTest(t, http.MethodPut, livenessPath, health.HandleLiveness)
	performUnsupportedMethodRequestTest(t, http.MethodTrace, livenessPath, health.HandleLiveness)

	performUnsupportedMethodRequestTest(t, http.MethodConnect, readinessPath, health.HandleReadiness)
	performUnsupportedMethodRequestTest(t, http.MethodDelete, readinessPath, health.HandleReadiness)
	performUnsupportedMethodRequestTest(t, http.MethodPost, readinessPath, health.HandleReadiness)
	performUnsupportedMethodRequestTest(t, http.MethodOptions, readinessPath, health.HandleReadiness)
	performUnsupportedMethodRequestTest(t, http.MethodPatch, readinessPath, health.HandleReadiness)
	performUnsupportedMethodRequestTest(t, http.MethodPut, readinessPath, health.HandleReadiness)
	performUnsupportedMethodRequestTest(t, http.MethodTrace, readinessPath, health.HandleReadiness)
}

// Test The Health Server Via The HTTP Handlers
func TestHealthHandler(t *testing.T) {

	// Create A New Health Server
	health := getTestHealthServer()

	// Verify that initially the statuses are not live / ready
	getEventToHandler(t, health.HandleLiveness, livenessPath, http.StatusInternalServerError)
	getEventToHandler(t, health.HandleReadiness, readinessPath, http.StatusOK)

	// Verify that the liveness status follows the health.Alive flag
	health.SetAlive(true)
	getEventToHandler(t, health.HandleLiveness, livenessPath, http.StatusOK)

	// Verify that the shutdown process sets liveness to false
	health.Shutdown()
	getEventToHandler(t, health.HandleLiveness, livenessPath, http.StatusInternalServerError)

	// Verify that the readiness status follows the health.Ready flag
	mockStatus.IsReady = false
	getEventToHandler(t, health.HandleReadiness, readinessPath, http.StatusInternalServerError)

}

// Test The Health Server With Startup Errors
func TestHealthServer(t *testing.T) {

	logger := logtesting.TestLogger(t).Desugar()

	health := getTestHealthServer()
	health.HttpPort = "--"
	err := health.Start(logger)
	assert.NotNil(t, err)
	health.Stop(logger)
}

//
// Private Utility Functions
//

// Sends A Request To An HTTP Response Recorder Directly Expecting A Specific Response Code
func getEventToHandler(t *testing.T, handlerFunc http.HandlerFunc, path string, expectedStatus int) {

	// Create A Test HTTP GET Request For requested path
	request := createNewRequest(t, http.MethodGet, path, nil)

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
