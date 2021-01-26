package control

// Connection handles the low level stuff, reading and writing to the wire
type Connection interface {
	OutboundMessages() chan<- *OutboundMessage
	InboundMessages() <-chan *InboundMessage
	// On this channel we get only very bad, usually fatal, errors (like cannot re-establish the connection after several attempts)
	Errors() <-chan error
}
