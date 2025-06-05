package wire

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMsgProtoconf(t *testing.T) {
	t.Run("default payload length", func(t *testing.T) {
		msg := NewMsgProtoconf(0, true)

		assert.Equal(t, "protoconf", msg.Command())
		assert.Equal(t, uint64(2), msg.NumberOfFields)
		assert.Equal(t, uint64(1024*1024), msg.MaxPayloadLength(ProtocolVersion))
		assert.Equal(t, uint32(2*1024*1024), msg.MaxRecvPayloadLength)
		assert.Len(t, msg.StreamPolicies, 2)
		assert.Equal(t, BlockPriorityStreamPolicy, msg.StreamPolicies[0])
		assert.Equal(t, DefaultStreamPolicy, msg.StreamPolicies[1])
	})

	t.Run("custom payload length", func(t *testing.T) {
		customLength := uint32(5 * 1024 * 1024) // 5MB

		msg := NewMsgProtoconf(customLength, true)

		assert.Equal(t, customLength, msg.MaxRecvPayloadLength)
		assert.Len(t, msg.StreamPolicies, 2)
	})

	t.Run("without block priority", func(t *testing.T) {
		msg := NewMsgProtoconf(0, false)

		assert.Len(t, msg.StreamPolicies, 1)
		assert.Equal(t, DefaultStreamPolicy, msg.StreamPolicies[0])
	})

	t.Run("encoding and decoding without block priority", func(t *testing.T) {
		msg := NewMsgProtoconf(0, false)

		var b bytes.Buffer
		bw := bufio.NewWriter(&b)

		err := msg.BsvEncode(bw, ProtocolVersion, BaseEncoding)
		require.NoError(t, err)
		err = bw.Flush()
		require.NoError(t, err)

		assert.Equal(t, "02000020000744656661756c74", hex.EncodeToString(b.Bytes()))

		msg2 := &MsgProtoconf{}
		r := bytes.NewReader(b.Bytes())
		err = msg2.Bsvdecode(r, ProtocolVersion, BaseEncoding)
		require.NoError(t, err)

		assert.Equal(t, msg.MaxRecvPayloadLength, msg2.MaxRecvPayloadLength)
		assert.Equal(t, msg.StreamPolicies, msg2.StreamPolicies)
	})

	t.Run("encoding and decoding with block priority", func(t *testing.T) {
		msg := NewMsgProtoconf(0, true)

		var b bytes.Buffer
		bw := bufio.NewWriter(&b)

		err := msg.BsvEncode(bw, ProtocolVersion, BaseEncoding)
		require.NoError(t, err)
		err = bw.Flush()
		require.NoError(t, err)

		assert.Equal(t, "020000200015426c6f636b5072696f726974792c44656661756c74", hex.EncodeToString(b.Bytes()))

		msg2 := &MsgProtoconf{}
		r := bytes.NewReader(b.Bytes())
		err = msg2.Bsvdecode(r, ProtocolVersion, BaseEncoding)
		require.NoError(t, err)

		assert.Equal(t, msg.MaxRecvPayloadLength, msg2.MaxRecvPayloadLength)
		assert.Equal(t, msg.StreamPolicies, msg2.StreamPolicies)
	})
}
