// Package utxopersister provides functionality for managing UTXO (Unspent Transaction Output) persistence.
package utxopersister

import (
	"os"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBits(t *testing.T) {
	var unsigned uint32 = 12345

	encodedValue1 := unsigned << 1
	bytes1 := append([]byte{}, byte(encodedValue1), byte(encodedValue1>>8), byte(encodedValue1>>16), byte(encodedValue1>>24))

	encodedValue2 := (unsigned << 1) | 1
	bytes2 := append([]byte{}, byte(encodedValue2), byte(encodedValue2>>8), byte(encodedValue2>>16), byte(encodedValue2>>24))

	decodedValue1 := uint32(bytes1[0]) | uint32(bytes1[1])<<8 | uint32(bytes1[2])<<16 | uint32(bytes1[3])<<24
	assert.Equal(t, uint32(12345), decodedValue1>>1)
	assert.False(t, (decodedValue1&1) == 1)

	decodedValue2 := uint32(bytes2[0]) | uint32(bytes2[1])<<8 | uint32(bytes2[2])<<16 | uint32(bytes2[3])<<24
	assert.Equal(t, uint32(12345), decodedValue2>>1)
	assert.True(t, (decodedValue2&1) == 1)
}

func TestBytesNormalTX(t *testing.T) {
	hash := chainhash.HashH([]byte{0x00, 0x01, 0x02, 0x03, 0x04})

	uw := &UTXOWrapper{
		TxID:     hash,
		Height:   12345,
		Coinbase: false,
		UTXOs: []*UTXO{
			{
				Index:  12345,
				Value:  1234567890,
				Script: []byte{0x00, 0x01, 0x02, 0x03, 0x04},
			},
		},
	}

	b := uw.Bytes()
	// t.Logf("b: %x", b)

	assert.Len(t, b, 32+4+4+4+8+4+5)

	uw2, err := NewUTXOWrapperFromBytes(b)
	assert.NoError(t, err)

	assert.Equal(t, uw.TxID, uw2.TxID)
	assert.Equal(t, uw.Height, uw2.Height)
	assert.Equal(t, uw.Coinbase, uw2.Coinbase)
	assert.Equal(t, uw.UTXOs[0].Index, uw2.UTXOs[0].Index)
	assert.Equal(t, uw.UTXOs[0].Value, uw2.UTXOs[0].Value)
	assert.Equal(t, uw.UTXOs[0].Script, uw2.UTXOs[0].Script)
}

func TestBytesCoinbaseTX(t *testing.T) {
	hash := chainhash.HashH([]byte{0x00, 0x01, 0x02, 0x03, 0x04})

	uw := &UTXOWrapper{
		TxID:     hash,
		Height:   12345,
		Coinbase: true,
		UTXOs: []*UTXO{
			{
				Index:  12345,
				Value:  1234567890,
				Script: []byte{0x00, 0x01, 0x02, 0x03, 0x04},
			},
		},
	}

	b := uw.Bytes()
	// t.Logf("b: %x", b)

	assert.Len(t, b, 32+4+4+4+8+4+5)

	u2, err := NewUTXOWrapperFromBytes(b)
	assert.NoError(t, err)

	assert.Equal(t, uw.TxID, u2.TxID)
	assert.Equal(t, uw.Height, u2.Height)
	assert.Equal(t, uw.Coinbase, u2.Coinbase)
	assert.Equal(t, uw.UTXOs[0].Index, u2.UTXOs[0].Index)
	assert.Equal(t, uw.UTXOs[0].Value, u2.UTXOs[0].Value)
	assert.Equal(t, uw.UTXOs[0].Script, u2.UTXOs[0].Script)
}

func TestTxWithOnlyOutputs(t *testing.T) {
	txBytes, err := os.ReadFile("testdata/4827ad32852fd9ca3979a8e507e38c6e557e58bc453184ef19cc7a5f86e7d59b.outputs")
	require.NoError(t, err)

	uw, err := NewUTXOWrapperFromBytes(txBytes)
	require.NoError(t, err)

	assert.Equal(t, 476, len(uw.UTXOs))

	utxos, err := PadUTXOsWithNil(uw.UTXOs)
	require.NoError(t, err)
	assert.NotNil(t, utxos)
	assert.Equal(t, 1000, len(utxos))

	// check the length of utxos that are not nil
	count := 0

	for _, u := range utxos {
		if u != nil {
			count++
		}
	}

	assert.Equal(t, 476, count)
}

// A decoded script must be backed by an exactly-sized slice: the seeder decodes
// hundreds of millions of ~25-byte scripts, and an over-allocated backing array
// (the old bytes.Buffer growth) costs ~1.5 KB of garbage per UTXO and retains
// ~1 KB per script.
func TestNewUTXOWrapperFromBytes_ScriptIsExactlySized(t *testing.T) {
	script := []byte{0x76, 0xa9, 0x14, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 0x88, 0xac}

	in := &UTXOWrapper{
		TxID:   chainhash.HashH([]byte("exact-script")),
		Height: 7,
		UTXOs:  []*UTXO{{Index: 3, Value: 42, Script: script}},
	}

	out, err := NewUTXOWrapperFromBytes(in.Bytes())
	require.NoError(t, err)
	require.Len(t, out.UTXOs, 1)

	require.Equal(t, script, out.UTXOs[0].Script)
	require.Equal(t, len(script), cap(out.UTXOs[0].Script))
}

// A script cut short inside the exact-size read must fail, not be returned
// zero-padded: the record's claimed length is trusted up to the bound only
// for the allocation, never for the content.
func TestNewUTXOWrapperFromBytes_TruncatedSmallScriptFails(t *testing.T) {
	in := &UTXOWrapper{
		TxID:   chainhash.HashH([]byte("truncated-script")),
		Height: 7,
		UTXOs:  []*UTXO{{Index: 0, Value: 42, Script: make([]byte, 25)}},
	}

	b := in.Bytes()

	_, err := NewUTXOWrapperFromBytes(b[:len(b)-10]) // drop the last 10 script bytes
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to read utxo script (25 bytes)")
}

// Scripts on either side of maxExactScriptAlloc take different read paths and
// must decode to the same bytes.
func TestNewUTXOWrapperFromBytes_ScriptAtExactAllocBoundary(t *testing.T) {
	for _, size := range []int{maxExactScriptAlloc, maxExactScriptAlloc + 1} {
		script := make([]byte, size)
		for i := range script {
			script[i] = byte(i)
		}

		in := &UTXOWrapper{
			TxID:   chainhash.HashH([]byte("boundary-script")),
			Height: 7,
			UTXOs:  []*UTXO{{Index: 0, Value: 42, Script: script}},
		}

		out, err := NewUTXOWrapperFromBytes(in.Bytes())
		require.NoError(t, err, "size %d", size)
		require.Len(t, out.UTXOs, 1)
		require.Equal(t, script, out.UTXOs[0].Script, "size %d", size)
	}
}

// An output index no valid transaction can have must be rejected, not padded:
// 0xFFFFFFFF wrapped maxIndex+1 to an empty slice and panicked on the first
// placement, and an index just below it asked for a ~32 GiB slice.
func TestPadUTXOsWithNil_RejectsImpossibleIndex(t *testing.T) {
	for _, index := range []uint32{MaxOutputIndex + 1, 0xFFFFFFFE, 0xFFFFFFFF} {
		_, err := PadUTXOsWithNil([]*UTXO{{Index: 0}, {Index: index}})
		require.Error(t, err, "index %d", index)
	}

	padded, err := PadUTXOsWithNil([]*UTXO{{Index: 2}})
	require.NoError(t, err)
	require.Len(t, padded, 3)
}
