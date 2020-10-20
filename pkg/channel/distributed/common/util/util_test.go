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

package util

import (
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	logtesting "knative.dev/pkg/logging/testing"
)

// Test The WaitForSignal Functionality
func TestWaitForSignal(t *testing.T) {

	// Logger Reference
	logger := logtesting.TestLogger(t).Desugar()

	// Create A Completion Channel
	doneChan := make(chan struct{})

	// Start Async Go Routing
	go func() {

		// Perform The Test - Wait For SIGINT
		logger.Info("Waiting For Signal")
		WaitForSignal(logger, syscall.SIGINT)

		// Close The Completion Channel
		close(doneChan)
	}()

	// Wait A Short Bit To Let Async Function Start
	logger.Info("Sleeping")
	time.Sleep(500 * time.Millisecond)

	// Issue The Signal
	logger.Info("Sending Signal")
	err := syscall.Kill(syscall.Getpid(), syscall.SIGINT)
	assert.Nil(t, err)

	// Block Waiting For Completion
	<-doneChan

	// Done!  (If The Test Completes (Doesn't Hang) Then It Was Successful)
	logger.Info("Done!")
}
