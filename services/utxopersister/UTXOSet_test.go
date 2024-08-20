package utxopersister

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

var (
	hash1 = chainhash.HashH([]byte{0x00, 0x01, 0x02, 0x03, 0x04})
	// hash2 = chainhash.HashH([]byte{0x05, 0x06, 0x07, 0x08, 0x09})
	// ctx   = context.Background()
)

func TestNewUTXOSet(t *testing.T) {
	store := memory.New()
	// store, err := file.New(ulogger.TestLogger{}, "utxo")
	// require.NoError(t, err)

	ctx := context.Background()

	// Create a new UTXODiff
	ud1, err := NewUTXODiff(ctx, ulogger.TestLogger{}, store, &hash1, 0)
	require.NoError(t, err)

	ud1.blockHeight = 10

	err = ud1.ProcessTx(tx)
	require.NoError(t, err)

	for i := uint32(0); i < 5; i++ {
		err = ud1.delete(&UTXODeletion{tx.TxIDChainHash(), i})
		require.NoError(t, err)
	}

	err = ud1.Close()
	require.NoError(t, err)

	checkAdditions(t, ud1)
	checkDeletions(t, ud1)

	// err = ud1.CreateUTXOSet(ctx, nil)
	// require.NoError(t, err)

	// checkUTXOSet(t, ud1, 5)

	// ud2, err := NewUTXODiff(ctx, ulogger.TestLogger{}, store, &hash2, 0)
	// require.NoError(t, err)

	// for i := uint32(9); i < 15; i++ {
	// 	err = ud2.add(&UTXO{&hash1, i, uint64(1000 + i), 10 + i, script, false})
	// 	require.NoError(t, err)
	// }

	// for i := uint32(3); i < 8; i++ {
	// 	err = ud2.delete(&UTXODeletion{&hash1, i})
	// 	require.NoError(t, err)
	// }

	// err = ud2.Close()
	// require.NoError(t, err)

	// checkAdditions(t, ud2, 9, 6)
	// checkDeletions(t, ud2, 3, 5)

	// err = ud2.CreateUTXOSet(ctx, &hash1)
	// require.NoError(t, err)

	// checkUTXOSet(t, ud2, 9)
}

func checkAdditions(t *testing.T, ud *UTXODiff) {
	r, err := ud.GetUTXOAdditionsReader()
	require.NoError(t, err)

	defer r.Close()

	_, _, _, err = GetHeaderFromReader(r)
	require.NoError(t, err)

	for {
		utxoWrapper, err := NewUTXOWrapperFromReader(r)
		if err != nil {
			assert.ErrorIs(t, err, io.EOF)
			break
		}

		require.NoError(t, err)
		assert.Equal(t, tx.TxIDChainHash().String(), utxoWrapper.TxID.String())
		assert.Equal(t, uint32(10), utxoWrapper.Height)
		assert.False(t, utxoWrapper.Coinbase)

		for i, utxo := range utxoWrapper.UTXOs {
			assert.Equal(t, uint32(i), utxo.Index)
			assert.Equal(t, tx.Outputs[i].Satoshis, utxo.Value)
			assert.True(t, tx.Outputs[i].LockingScript.EqualsBytes(utxo.Script))
		}
	}
}

func checkDeletions(t *testing.T, ud *UTXODiff) {
	r, err := ud.GetUTXODeletionsReader()
	require.NoError(t, err)

	defer r.Close()

	_, _, _, err = GetHeaderFromReader(r)
	require.NoError(t, err)

	// Read the deletion caused by the processTX of tx
	_, err = NewUTXODeletionFromReader(r)
	require.NoError(t, err)

	// assert.Equal(t, tx.Inputs[0].PreviousTxID(), utxoDeletion.TxID)

	for i := 0; i < 5; i++ {
		utxoDeletion, err := NewUTXODeletionFromReader(r)
		if err != nil {
			assert.ErrorIs(t, err, io.EOF)
			break
		}

		require.NoError(t, err)
		assert.Equal(t, tx.TxIDChainHash().String(), utxoDeletion.TxID.String())
		assert.Equal(t, uint32(i), utxoDeletion.Index)
	}
}

// func checkUTXOSet(t *testing.T, ud *UTXODiff, count uint32) {
// 	r, err := ud.GetUTXOSetReader()
// 	require.NoError(t, err)

// 	defer r.Close()

// 	var i int

// 	for {
// 		utxo, err := NewUTXOFromReader(r)
// 		if err != nil {
// 			assert.ErrorIs(t, err, io.EOF)
// 			break
// 		}

// 		require.NoError(t, err)
// 		assert.Equal(t, &hash1, utxo.TxID)
// 		// t.Logf("%v", utxo)

// 		i++
// 	}

// 	assert.Equal(t, count, uint32(i))
// }
