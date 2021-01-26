package control

import (
	"io"

	"github.com/google/uuid"
)

const (
	maximumSupportedVersion uint16 = 0
	outboundMessageVersion         = maximumSupportedVersion
)

type InboundMessage struct {
	MessageHeader
	Payload []byte
}

func (msg *InboundMessage) ReadFrom(r io.Reader) (count int64, err error) {
	var b [24]byte
	var n int
	n, err = io.ReadAtLeast(io.LimitReader(r, 24), b[0:24], 24)
	count = count + int64(n)
	if err != nil {
		return count, err
	}

	msg.MessageHeader = messageHeaderFromBytes(b)
	if msg.Length() != 0 {
		// We need to read the payload
		msg.Payload = make([]byte, msg.Length())
		n, err = io.ReadAtLeast(io.LimitReader(r, int64(msg.Length())), msg.Payload, int(msg.Length()))
		count = count + int64(n)
	}
	return count, err
}

type OutboundMessage struct {
	MessageHeader
	payload []byte
}

func (msg *OutboundMessage) WriteTo(w io.Writer) (count int64, err error) {
	n, err := msg.MessageHeader.WriteTo(w)
	count = count + n
	if err != nil {
		return count, err
	}

	if msg.payload != nil {
		var n1 int
		n1, err = w.Write(msg.payload)
		count = count + int64(n1)
	}
	return count, err
}

func NewOutboundMessage(opcode uint8, payload []byte) OutboundMessage {
	return OutboundMessage{
		MessageHeader: MessageHeader{
			version: outboundMessageVersion,
			flags:   0,
			opcode:  opcode,
			uuid:    uuid.New(),
			length:  uint32(len(payload)),
		},
		payload: payload,
	}
}

func NewOutboundMessageWithUUID(uuid [16]byte, opcode uint8, payload []byte) OutboundMessage {
	return OutboundMessage{
		MessageHeader: MessageHeader{
			version: outboundMessageVersion,
			flags:   0,
			opcode:  opcode,
			uuid:    uuid,
			length:  uint32(len(payload)),
		},
		payload: payload,
	}
}
