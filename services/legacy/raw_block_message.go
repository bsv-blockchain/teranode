package legacy

import (
	"io"

	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
)

// RawBlockMessage implements wire.Message for streaming raw block bytes
// without the overhead of deserializing into wire.MsgBlock structure.
//
// This is significantly more efficient for large blocks (e.g., 4GB+) because:
// - No CPU time spent deserializing millions of transactions into Go structs
// - No CPU time spent re-serializing those structs back to bytes
// - Reduced memory overhead (no Go struct allocation per transaction)
//
// The raw bytes are read from the source and written directly to the wire,
// bypassing the deserialize/serialize roundtrip that would otherwise require
// allocating memory for each transaction struct.
type RawBlockMessage struct {
	data []byte
}

// NewRawBlockMessage creates a RawBlockMessage by reading all bytes from the reader.
// The data must be in wire-format block encoding (header + txcount + transactions).
func NewRawBlockMessage(reader io.Reader) (*RawBlockMessage, error) {
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, errors.NewProcessingError("failed to read block data", err)
	}

	return &RawBlockMessage{data: data}, nil
}

// Command returns the protocol command string for the message.
// Implements wire.Message interface.
func (m *RawBlockMessage) Command() string {
	return wire.CmdBlock
}

// BsvEncode writes the raw block bytes to the writer.
// Implements wire.Message interface.
func (m *RawBlockMessage) BsvEncode(w io.Writer, _ uint32, _ wire.MessageEncoding) error {
	_, err := w.Write(m.data)
	return err
}

// Bsvdecode is not supported for RawBlockMessage.
// Implements wire.Message interface.
func (m *RawBlockMessage) Bsvdecode(_ io.Reader, _ uint32, _ wire.MessageEncoding) error {
	return errors.NewProcessingError("RawBlockMessage does not support decoding")
}

// MaxPayloadLength returns the length of the raw block data.
// Implements wire.Message interface.
func (m *RawBlockMessage) MaxPayloadLength(_ uint32) uint64 {
	return uint64(len(m.data))
}
