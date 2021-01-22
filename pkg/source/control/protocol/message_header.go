package protocol

import (
	"encoding/binary"
	"io"

	"github.com/google/uuid"
)

type MessageFlag uint8

type MessageHeader struct {
	version uint16
	flags   uint8
	opcode  uint8
	uuid    [16]byte
	// In bytes
	length uint32
}

func (m MessageHeader) Version() uint16 {
	return m.version
}

func (m MessageHeader) Check(flag MessageFlag) bool {
	return (m.flags & uint8(flag)) == uint8(flag)
}

func (m MessageHeader) OpCode() uint8 {
	return m.opcode
}

func (m MessageHeader) UUID() uuid.UUID {
	return m.uuid
}

func (m MessageHeader) Length() uint32 {
	return m.length
}

func (m MessageHeader) WriteTo(w io.Writer) (int64, error) {
	var b [4]byte
	var n int64
	binary.BigEndian.PutUint16(b[0:2], m.version)
	b[2] = m.flags
	b[3] = m.opcode
	n1, err := w.Write(b[0:4])
	n = n + int64(n1)
	if err != nil {
		return n, err
	}

	n1, err = w.Write(m.uuid[0:16])
	n = n + int64(n1)
	if err != nil {
		return n, err
	}

	binary.BigEndian.PutUint32(b[0:4], m.length)
	n1, err = w.Write(b[0:4])
	n = n + int64(n1)
	return n, err
}

func messageHeaderFromBytes(b [24]byte) MessageHeader {
	m := MessageHeader{}
	m.version = binary.BigEndian.Uint16(b[0:2])
	m.flags = b[2]
	m.opcode = b[3]
	for i := 0; i < 16; i++ {
		m.uuid[i] = b[4+i]
	}
	m.length = binary.BigEndian.Uint32(b[20:24])
	return m
}
