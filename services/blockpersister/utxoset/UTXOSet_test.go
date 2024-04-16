package utxoset_test

import (
	"bufio"
	"bytes"
	"testing"

	"github.com/bitcoin-sv/ubsv/services/blockpersister/utxoset"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

func TestNewUTXOMapFromReader(t *testing.T) {
	// Create a valid byte slice
	hash := chainhash.HashH([]byte{0x00, 0x01, 0x02, 0x03, 0x04})

	// Create a new UTXOMap
	um1 := utxoset.NewUTXOMap(&hash)

	b := []byte{0x00, 0x01, 0x02, 0x03, 0x04}

	// Add some UTXOs
	for i := uint32(0); i < 5; i++ {
		um1.Add(hash, i, 1000+1, 10+1, []byte{b[i]})
	}

	buf := new(bytes.Buffer)
	w := bufio.NewWriter(buf)

	err := um1.Write(w)
	require.NoError(t, err)

	// Flush the buffer
	err = w.Flush()
	require.NoError(t, err)

	// Read the UTXOMap from the buffer
	r := bufio.NewReader(buf)
	um2, err := utxoset.NewUTXOMapFromReader(r)
	require.NoError(t, err)

	// Check the UTXOMap is the same
	require.Equal(t, um1, um2)

	// Check the UTXOs are the same
	for i := 0; i < 5; i++ {
		utxo1 := um1.Get(hash, uint32(i))
		utxo2 := um2.Get(hash, uint32(i))

		require.Equal(t, utxo1, utxo2)
	}

	um2.Add(hash, 1, 1000, 10, []byte{b[0]})
	require.NotEqual(t, um1, um2)
}
