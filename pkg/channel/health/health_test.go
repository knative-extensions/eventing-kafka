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

// Test The Channel Health Server Via Live HTTP Calls
func TestChannelHealthServer(t *testing.T) {

	logger := logtesting.TestLogger(t).Desugar()

	chs := NewChannelHealthServer(testHttpPort)
	chs.Start(logger)

	readinessUri, err := url.Parse(fmt.Sprintf("http://%s:%s%s", testHttpHost, testHttpPort, readinessPath))
	assert.Nil(t, err)
	waitServerReady(readinessUri.String(), 3*time.Second)

	// Verify that initially the readiness status is false
	getEventToServer(t, readinessUri, http.StatusInternalServerError)

	// Verify that the readiness status requires setting all of the readiness flags
	chs.SetChannelReady(true)
	getEventToServer(t, readinessUri, http.StatusInternalServerError)
	chs.SetProducerReady(true)
	getEventToServer(t, readinessUri, http.StatusOK)
	chs.SetChannelReady(false)
	getEventToServer(t, readinessUri, http.StatusInternalServerError)

	// Verify that the shutdown process sets the readiness status to false
	chs.SetProducerReady(true)
	chs.SetChannelReady(true)
	getEventToServer(t, readinessUri, http.StatusOK)

	chs.Shutdown()
	getEventToServer(t, readinessUri, http.StatusInternalServerError)

	getEventToServer(t, readinessUri, http.StatusInternalServerError)
	chs.SetChannelReady(true)
	chs.SetProducerReady(true)
	getEventToServer(t, readinessUri, http.StatusOK)

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
