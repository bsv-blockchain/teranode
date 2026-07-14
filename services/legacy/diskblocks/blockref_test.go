package diskblocks

import (
	"bytes"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

// encodeVarInt encodes using Bitcoin Core's base-128 ReadVarInt scheme (mirror of readVarInt).
func encodeVarInt(n uint64) []byte {
	tmp := make([]byte, 0, 10)
	for {
		b := byte(n & 0x7f)
		tmp = append(tmp, b)
		if n <= 0x7f {
			break
		}
		n = (n >> 7) - 1
	}
	// emit in reverse, setting the continuation bit on all but the last
	out := make([]byte, 0, len(tmp))
	for i := len(tmp) - 1; i >= 0; i-- {
		b := tmp[i]
		if i != 0 {
			b |= 0x80
		}
		out = append(out, b)
	}
	return out
}

func TestReadVarIntRoundTrip(t *testing.T) {
	for _, v := range []uint64{0, 1, 127, 128, 255, 16384, 1_000_000} {
		enc := encodeVarInt(v)
		got, n := readVarInt(enc)
		require.Equal(t, v, got)
		require.Equal(t, len(enc), n)
	}
}

func TestParseBlockIndexRecord(t *testing.T) {
	// 80-byte header: version(4) | prevhash(32) | merkle(32) | time(4) | bits(4) | nonce(4)
	header := make([]byte, 80)
	header[0] = 0x01 // version = 1
	prev := bytes.Repeat([]byte{0xAB}, 32)
	copy(header[4:36], prev)

	status := BlockValidTransactions | BlockHaveData // has data, validated to TRANSACTIONS
	var buf bytes.Buffer
	buf.Write(encodeVarInt(1))              // version (discarded)
	buf.Write(encodeVarInt(150))            // height
	buf.Write(encodeVarInt(uint64(status))) // status
	buf.Write(encodeVarInt(3))              // txcount
	buf.Write(encodeVarInt(7))              // nFile
	buf.Write(encodeVarInt(8192))           // nDataPos
	buf.Write(header)                       // 80-byte header

	ref, err := parseBlockIndexRecord(buf.Bytes())
	require.NoError(t, err)
	require.Equal(t, uint32(150), ref.Height)
	require.Equal(t, uint32(7), ref.NFile)
	require.Equal(t, uint32(8192), ref.NDataPos)
	require.True(t, ref.HaveData)
	require.Equal(t, uint64(3), ref.TxCount)

	wantPrev, _ := chainhash.NewHash(prev)
	require.Equal(t, wantPrev.String(), ref.PrevHash.String())
}

func TestParseBlockIndexRecordRejectsNoData(t *testing.T) {
	var buf bytes.Buffer
	buf.Write(encodeVarInt(1))                      // version
	buf.Write(encodeVarInt(10))                     // height
	buf.Write(encodeVarInt(uint64(BlockValidTree))) // status: only TREE, no data
	buf.Write(encodeVarInt(1))                      // txcount
	buf.Write(make([]byte, 80))                     // header
	_, err := parseBlockIndexRecord(buf.Bytes())
	require.Error(t, err)
}

func TestParseBlockIndexRecordWithUndo(t *testing.T) {
	header := make([]byte, 80)
	header[0] = 0x01
	prev := bytes.Repeat([]byte{0xCD}, 32)
	copy(header[4:36], prev)

	status := BlockValidTransactions | BlockHaveData | BlockHaveUndo
	var buf bytes.Buffer
	buf.Write(encodeVarInt(1))              // version (discarded)
	buf.Write(encodeVarInt(900))            // height
	buf.Write(encodeVarInt(uint64(status))) // status
	buf.Write(encodeVarInt(5))              // txcount
	buf.Write(encodeVarInt(12))             // nFile
	buf.Write(encodeVarInt(40000))          // nDataPos
	buf.Write(encodeVarInt(777))            // nUndoPos (read and discarded)
	buf.Write(header)                       // 80-byte header

	ref, err := parseBlockIndexRecord(buf.Bytes())
	require.NoError(t, err)
	require.Equal(t, uint32(900), ref.Height)
	require.Equal(t, uint32(12), ref.NFile)
	require.Equal(t, uint32(40000), ref.NDataPos)
	require.Equal(t, uint64(5), ref.TxCount)
	require.True(t, ref.HaveData)
}
