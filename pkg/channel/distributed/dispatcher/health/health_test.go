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
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	testHttpPort  = "0"
	readinessPath = "/healthy"
)

// Test The NewDispatcherHealthServer() Functionality
func TestNewDispatcherHealthServer(t *testing.T) {

	// Create A Health Server
	health := NewDispatcherHealthServer(testHttpPort)

	// Validate The EventProxy
	assert.NotNil(t, health)
	assert.Equal(t, false, health.Alive())
	assert.Equal(t, false, health.dispatcherReady)
}

// Test Flag Set And Reset Functions
func TestReadinessFlagWrites(t *testing.T) {

	// Create A New Health Server
	chs := NewDispatcherHealthServer(testHttpPort)

	// Test Readiness Flags
	chs.SetDispatcherReady(false)
	assert.Equal(t, false, chs.DispatcherReady())
	chs.SetDispatcherReady(true)
	assert.Equal(t, true, chs.DispatcherReady())
}

// Test The Dispatcher Health Server Via The HTTP Handlers
func TestDispatcherHealthHandler(t *testing.T) {

	// Create A New Health Server
	chs := NewDispatcherHealthServer(testHttpPort)

	// Verify that initially the readiness status is false
	getEventToHandler(t, chs.HandleReadiness, readinessPath, http.StatusInternalServerError)

	// Verify that the readiness status required setting all of the readiness flags
	chs.SetDispatcherReady(true)
	getEventToHandler(t, chs.HandleReadiness, readinessPath, http.StatusOK)

	// Verify that the shutdown process sets all statuses to not live / not ready
	chs.SetDispatcherReady(true)
	getEventToHandler(t, chs.HandleReadiness, readinessPath, http.StatusOK)

	chs.Shutdown()
	getEventToHandler(t, chs.HandleReadiness, readinessPath, http.StatusInternalServerError)
}

//
// Private Utility Functions
//

// Create A Test HTTP Request For The Specified Method / Path
func createNewRequest(t *testing.T, method string, path string, body io.Reader) *http.Request {
	request, err := http.NewRequest(method, path, body)
	assert.Nil(t, err)
	return request
}

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
