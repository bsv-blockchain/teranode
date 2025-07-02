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

	utxos := PadUTXOsWithNil(uw.UTXOs)
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
