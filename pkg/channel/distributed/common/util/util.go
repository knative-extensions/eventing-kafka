package util

import (
	"os"
	"os/signal"

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
