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
	"os"
	"os/signal"

	"github.com/Shopify/sarama"

	"go.uber.org/zap"
)

// Block Waiting For Any Of The Specified Signals
func WaitForSignal(logger *zap.Logger, signals ...os.Signal) {

	// Register For Signal Notification On Channel
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, signals...)

	// Block Waiting For Signal Channel Notification
	sig := <-signalChan

	// Log Signal Receipt
	logger.Info("Received Signal", zap.String("Signal", sig.String()))
}

// Utility function to convert []byte headers to string ones for logging purposes
func StringifyHeaders(headers []sarama.RecordHeader) map[string][]string {
	stringHeaders := make(map[string][]string)
	for _, header := range headers {
		key := string(header.Key)
		stringHeaders[key] = append(stringHeaders[key], string(header.Value))
	}
	return stringHeaders
}

// Pointer-version of the StringifyHeaders function
func StringifyHeaderPtrs(headers []*sarama.RecordHeader) map[string][]string {
	stringHeaders := make(map[string][]string)
	for _, header := range headers {
		key := string(header.Key)
		stringHeaders[key] = append(stringHeaders[key], string(header.Value))
	}
	return stringHeaders
}
