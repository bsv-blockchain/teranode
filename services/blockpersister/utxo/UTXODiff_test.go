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

	for i := uint32(0); i < 5; i++ {
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
	}
}

// func TestNewUTXODiffFromReaderWithProcessTx(t *testing.T) {
// 	hash := chainhash.HashH([]byte{0x00, 0x01, 0x02, 0x03, 0x04})

// 	var err error

// 	tx := bt.NewTx()

// 	numberOfInputs := 0

// 	for i := 0; i < numberOfInputs; i++ {
// 		err := tx.From(hash.String(), uint32(i), "0011", 1024)
// 		require.NoError(t, err)
// 	}

// 	numberOfOutputs := 60

// 	for i := 0; i < numberOfOutputs; i++ {
// 		err = tx.PayToAddress("1MM6xtKRdUAHQ4hZkqwVGf8wnDuYu1dHPA", 100)
// 		require.NoError(t, err)
// 	}

// 	err = tx.AddOpReturnOutput([]byte("hello world"))
// 	require.NoError(t, err)

// 	// Create a new UTXODiff
// 	ud1 := NewUTXODiff(ulogger.TestLogger{}, &hash)

// 	ud1.ProcessTx(tx)

// 	assert.Equal(t, numberOfInputs, ud1.Removed.Length())
// 	assert.Equal(t, numberOfOutputs, ud1.Added.Length())

// 	buf := new(bytes.Buffer)
// 	w := bufio.NewWriter(buf)

// 	err = ud1.Write(w)
// 	require.NoError(t, err)

// 	// Flush the buffer
// 	err = w.Flush()
// 	require.NoError(t, err)

// 	// Read the UTXOMap from the buffer
// 	r := bufio.NewReader(buf)
// 	ud2, err := NewUTXODiffFromReader(ulogger.TestLogger{}, r)
// 	require.NoError(t, err)

// 	// Check the UTXOMap is the same
// 	assert.Equal(t, ud1.BlockHash, ud2.BlockHash)
// 	assert.Equal(t, ud1.Added.Length(), ud2.Added.Length())
// 	assert.Equal(t, ud1.Removed.Length(), ud2.Removed.Length())
// }
