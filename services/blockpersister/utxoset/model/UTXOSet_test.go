package model

import (
	"bufio"
	"bytes"
	"testing"

	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewUTXOSetFromReader(t *testing.T) {
	// Create a valid byte slice
	hash := chainhash.HashH([]byte{0x00, 0x01, 0x02, 0x03, 0x04})

	// Create a new UTXOMap
	us1 := NewUTXOSet(&hash, 22)

	b := []byte{0x00, 0x01, 0x02, 0x03, 0x04}

	// Add some UTXOs
	for i := uint32(0); i < 5; i++ {
		us1.Add(hash, i, 1000+1, 10+1, []byte{b[i]})
	}

	buf := new(bytes.Buffer)
	w := bufio.NewWriter(buf)

	err := us1.Write(w)
	require.NoError(t, err)

	// Flush the buffer
	err = w.Flush()
	require.NoError(t, err)

	// Read the UTXOMap from the buffer
	r := bufio.NewReader(buf)
	us2, err := NewUTXOSetFromReader(r)
	require.NoError(t, err)

	// Check the UTXOMap is the same
	assert.Equal(t, us1.blockHash, us2.blockHash)
	assert.Equal(t, us1.blockHeight, us2.blockHeight)
	assert.Equal(t, us1.m.Length(), us2.m.Length())

	// Check the UTXOs are the same
	for i := 0; i < 5; i++ {
		utxo1, ok := us1.Get(hash, uint32(i))
		require.True(t, ok)

		utxo2, ok := us2.Get(hash, uint32(i))
		require.True(t, ok)

		require.Equal(t, utxo1, utxo2)
	}

	us2.Add(hash, 1, 1000, 10, []byte{b[0]})
	require.NotEqual(t, us1, us2)
}
