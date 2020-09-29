package health

import (
	"github.com/stretchr/testify/assert"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

const (
	testHttpPort  = "0"
	readinessPath = "/healthy"
)

// Test The NewChannelHealthServer() Functionality
func TestNewChannelHealthServer(t *testing.T) {

	// Create A Health Server
	health := NewChannelHealthServer(testHttpPort)

	// Validate The EventProxy
	assert.NotNil(t, health)
	assert.Equal(t, false, health.Alive())
	assert.Equal(t, false, health.channelReady)
	assert.Equal(t, false, health.producerReady)
}

// Test Flag Set And Reset Functions
func TestReadinessFlagWrites(t *testing.T) {

	// Create A New Health Server
	chs := NewChannelHealthServer(testHttpPort)

	// Test Readiness Flags
	chs.SetProducerReady(false)
	assert.Equal(t, false, chs.ProducerReady())
	chs.SetProducerReady(true)
	assert.Equal(t, true, chs.ProducerReady())
	chs.SetChannelReady(false)
	assert.Equal(t, false, chs.ChannelReady())
	chs.SetChannelReady(true)
	assert.Equal(t, true, chs.ChannelReady())

}

// Test The Channel Health Server Via The HTTP Handlers
func TestChannelHealthHandler(t *testing.T) {

	// Create A New Health Server
	chs := NewChannelHealthServer(testHttpPort)

	// Verify that initially the readiness status is false
	getEventToHandler(t, chs.HandleReadiness, readinessPath, http.StatusInternalServerError)

	// Verify that the readiness status requires setting all of the readiness flags
	chs.SetChannelReady(true)
	getEventToHandler(t, chs.HandleReadiness, readinessPath, http.StatusInternalServerError)
	chs.SetProducerReady(true)
	getEventToHandler(t, chs.HandleReadiness, readinessPath, http.StatusOK)
	chs.SetChannelReady(false)
	getEventToHandler(t, chs.HandleReadiness, readinessPath, http.StatusInternalServerError)

	// Verify that the shutdown process sets the readiness status to false
	chs.SetProducerReady(true)
	chs.SetChannelReady(true)
	getEventToHandler(t, chs.HandleReadiness, readinessPath, http.StatusOK)

	chs.Shutdown()
	getEventToHandler(t, chs.HandleReadiness, readinessPath, http.StatusInternalServerError)

	getEventToHandler(t, chs.HandleReadiness, readinessPath, http.StatusInternalServerError)
	chs.SetChannelReady(true)
	chs.SetProducerReady(true)
	getEventToHandler(t, chs.HandleReadiness, readinessPath, http.StatusOK)
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
