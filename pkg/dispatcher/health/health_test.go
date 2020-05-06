package health

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	logtesting "knative.dev/pkg/logging/testing"
	"net/http"
	"net/url"
	"testing"
	"time"
)

const (
	testHttpPort  = "8089"
	testHttpHost  = "localhost"
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

// Test The Dispatcher Health Server Via Live HTTP Calls
func TestDispatcherHealthServer(t *testing.T) {

	logger := logtesting.TestLogger(t).Desugar()

	chs := NewDispatcherHealthServer(testHttpPort)
	chs.Start(logger)

	readinessUri, err := url.Parse(fmt.Sprintf("http://%s:%s%s", testHttpHost, testHttpPort, readinessPath))
	assert.Nil(t, err)
	waitServerReady(readinessUri.String(), 3*time.Second)

	// Verify that initially the readiness status is false
	getEventToServer(t, readinessUri, http.StatusInternalServerError)

	// Verify that the readiness status required setting all of the readiness flags
	chs.SetDispatcherReady(true)
	getEventToServer(t, readinessUri, http.StatusOK)

	// Verify that the shutdown process sets all statuses to not live / not ready
	chs.SetDispatcherReady(true)
	getEventToServer(t, readinessUri, http.StatusOK)

	chs.Shutdown()
	getEventToServer(t, readinessUri, http.StatusInternalServerError)

	chs.Stop(logger)

	// Pause to let async go process finish logging :(
	// Appears to be race condition between test finishing and logging in Stop() above
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
