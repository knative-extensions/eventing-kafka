package network

import "time"

const (
	clientInitialDialRetry  = 5
	clientReconnectionRetry = 10
	clientDialRetryInterval = 200 * time.Millisecond

	keepAlive = 30 * time.Second
)
