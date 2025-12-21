package legacy

import (
	"bytes"
	"io"
	"testing"

	"github.com/bsv-blockchain/go-wire"
	"github.com/stretchr/testify/require"
)

func TestRawBlockMessage_Command(t *testing.T) {
	msg := &RawBlockMessage{data: []byte{}}
	require.Equal(t, wire.CmdBlock, msg.Command())
}

func TestRawBlockMessage_MaxPayloadLength(t *testing.T) {
	data := make([]byte, 1000)
	msg := &RawBlockMessage{data: data}
	require.Equal(t, uint64(1000), msg.MaxPayloadLength(0))
}

func TestRawBlockMessage_BsvEncode(t *testing.T) {
	testData := []byte{0x01, 0x02, 0x03, 0x04, 0x05}
	msg := &RawBlockMessage{data: testData}

	var buf bytes.Buffer
	err := msg.BsvEncode(&buf, 0, wire.BaseEncoding)
	require.NoError(t, err)
	require.Equal(t, testData, buf.Bytes())
}

func TestRawBlockMessage_Bsvdecode(t *testing.T) {
	msg := &RawBlockMessage{}
	err := msg.Bsvdecode(nil, 0, wire.BaseEncoding)
	require.Error(t, err)
	require.Contains(t, err.Error(), "does not support decoding")
}

func TestNewRawBlockMessage(t *testing.T) {
	testData := []byte{0x01, 0x02, 0x03, 0x04, 0x05}
	reader := bytes.NewReader(testData)

	msg, err := NewRawBlockMessage(reader)
	require.NoError(t, err)
	require.Equal(t, testData, msg.data)
}

func TestNewRawBlockMessage_ReadError(t *testing.T) {
	// Create a reader that will return an error
	reader := &failingReader{}

	msg, err := NewRawBlockMessage(reader)
	require.Error(t, err)
	require.Nil(t, msg)
}

// failingReader is a test helper that always fails on Read
type failingReader struct{}

func (r *failingReader) Read(_ []byte) (int, error) {
	return 0, io.ErrUnexpectedEOF
}

func TestRawBlockMessage_LargeData(t *testing.T) {
	// Test with a larger block-like structure (1MB)
	data := make([]byte, 1024*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	reader := bytes.NewReader(data)
	msg, err := NewRawBlockMessage(reader)
	require.NoError(t, err)
	require.Equal(t, uint64(1024*1024), msg.MaxPayloadLength(0))

	// Verify encode produces the same data
	var buf bytes.Buffer
	err = msg.BsvEncode(&buf, 0, wire.BaseEncoding)
	require.NoError(t, err)
	require.Equal(t, data, buf.Bytes())
}
