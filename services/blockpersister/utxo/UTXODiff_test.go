package utxo

import (
	"context"
	"io"
	"testing"

	"github.com/bitcoin-sv/ubsv/stores/blob/memory"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewUTXODiff(t *testing.T) {
	// Create a random 32 byte slice
	hash := chainhash.HashH([]byte{0x00, 0x01, 0x02, 0x03, 0x04})

	store := memory.New()

	ctx := context.Background()

	// Create a new UTXODiff
	ud, err := NewUTXODiff(ctx, ulogger.TestLogger{}, store, &hash)
	require.NoError(t, err)

	script := []byte{0x00, 0x01, 0x02, 0x03, 0x04}

	// Add some UTXOs
	for i := uint32(0); i < 5; i++ {
		err = ud.add(&UTXO{&hash, i, uint64(1000 + i), 10 + i, script})
		require.NoError(t, err)
	}

	// Remove some UTXOs
	for i := uint32(3); i < 8; i++ {
		err = ud.delete(&UTXODeletion{&hash, i})
		require.NoError(t, err)
	}

	err = ud.Close()
	require.NoError(t, err)

	d, err := ud.GetUTXODeletionsSet()
	require.NoError(t, err)

	assert.Equal(t, 5, len(d))

	// Lookup the first utxo
	var b2 [36]byte
	copy(b2[:], hash[:])
	copy(b2[32:], []byte{0x03, 0x00, 0x00, 0x00})

	_, found := d[b2]
	assert.True(t, found)

	r, err := ud.GetUTXOAdditionsReader()
	require.NoError(t, err)

	defer r.Close()

	var i uint32

	for {
		// Read the txID
		utxo, err := NewUTXOFromReader(r)
		if err == io.EOF {
			break
		}

		require.NoError(t, err)
		assert.Equal(t, &hash, utxo.TxID)
		assert.Equal(t, i, utxo.Index)
		assert.Equal(t, uint64(1000+i), utxo.Value)
		assert.Equal(t, uint32(10+i), utxo.SpendingHeight)
		assert.Equal(t, script, utxo.Script)

		i++
	}

	assert.Equal(t, uint32(5), i)
}
