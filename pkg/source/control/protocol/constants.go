package protocol

import "time"

const (
	clientInitialDialRetry  = 5
	clientReconnectionRetry = 10
	clientDialRetryInterval = 200 * time.Millisecond

	maximumSupportedVersion uint16 = 0
	outboundMessageVersion         = maximumSupportedVersion

	keepAlive = 30 * time.Second

	AckOpCode uint8 = 0

	controlServiceSendTimeout = 15 * time.Second
)
