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

var (
	script = []byte{0x00, 0x01, 0x02, 0x03, 0x04}

	hash1 = chainhash.HashH([]byte{0x00, 0x01, 0x02, 0x03, 0x04})
	hash2 = chainhash.HashH([]byte{0x05, 0x06, 0x07, 0x08, 0x09})
	ctx   = context.Background()
)

func TestNewUTXOSet(t *testing.T) {
	store := memory.New()
	// store, err := file.New(ulogger.TestLogger{}, "utxo")
	// require.NoError(t, err)

	ud1, err := NewUTXODiff(ctx, ulogger.TestLogger{}, store, &hash1)
	require.NoError(t, err)

	for i := uint32(0); i < 5; i++ {
		err = ud1.add(&UTXO{&hash1, i, uint64(1000 + i), 10 + i, script})
		require.NoError(t, err)
	}

	for i := uint32(3); i < 8; i++ {
		err = ud1.delete(&UTXODeletion{&hash1, i})
		require.NoError(t, err)
	}

	err = ud1.Close()
	require.NoError(t, err)

	checkAdditions(t, ud1, 0, 5)
	checkDeletions(t, ud1, 3, 5)

	ud2, err := NewUTXODiff(ctx, ulogger.TestLogger{}, store, &hash2)
	require.NoError(t, err)

	for i := uint32(9); i < 15; i++ {
		err = ud2.add(&UTXO{&hash1, i, uint64(1000 + i), 10 + i, script})
		require.NoError(t, err)
	}

	for i := uint32(15); i < 18; i++ {
		err = ud2.delete(&UTXODeletion{&hash1, i})
		require.NoError(t, err)
	}

	err = ud2.Close()
	require.NoError(t, err)

	checkAdditions(t, ud2, 9, 6)
	checkDeletions(t, ud2, 15, 3)

	err = ud1.CreateUTXOSet(ctx, nil)
	require.NoError(t, err)

	checkUTXOSet(t, ud1, 3)

	err = ud2.CreateUTXOSet(ctx, &hash1)
	require.NoError(t, err)

	checkUTXOSet(t, ud2, 9)

	r, err := ud2.GetUTXOSetReader()
	require.NoError(t, err)

	var i uint32 = 9

	for {
		utxo, err := NewUTXOFromReader(r)
		if err != nil {
			assert.ErrorIs(t, err, io.EOF)
			break
		}

		require.NoError(t, err)
		assert.Equal(t, &hash1, utxo.TxID)
		assert.Equal(t, script, utxo.Script)
		// t.Log(utxo)

		i++
	}
}

func checkAdditions(t *testing.T, ud *UTXODiff, i uint32, count uint32) {
	r, err := ud.GetUTXOAdditionsReader()
	require.NoError(t, err)

	defer r.Close()

	for {
		utxo, err := NewUTXOFromReader(r)
		if err != nil {
			assert.ErrorIs(t, err, io.EOF)
			break
		}

		require.NoError(t, err)
		assert.Equal(t, &hash1, utxo.TxID)
		assert.Equal(t, i, utxo.Index)
		assert.Equal(t, uint64(1000+i), utxo.Value)
		assert.Equal(t, uint32(10+i), utxo.SpendingHeight)
		assert.Equal(t, script, utxo.Script)

		i++
		count--
	}

	assert.Equal(t, uint32(0), count)
}

func checkDeletions(t *testing.T, ud *UTXODiff, i uint32, count uint32) {
	r, err := ud.GetUTXODeletionsReader()
	require.NoError(t, err)

	defer r.Close()

	for {
		utxoDeletion, err := NewUTXODeletionFromReader(r)
		if err != nil {
			assert.ErrorIs(t, err, io.EOF)
			break
		}

		require.NoError(t, err)
		assert.Equal(t, &hash1, utxoDeletion.TxID)
		assert.Equal(t, uint32(i), utxoDeletion.Index)

		i++
		count--
	}

	assert.Equal(t, uint32(0), count)
}

func checkUTXOSet(t *testing.T, ud *UTXODiff, count uint32) {
	r, err := ud.GetUTXOSetReader()
	require.NoError(t, err)

	defer r.Close()

	var i int

	for {
		utxo, err := NewUTXOFromReader(r)
		if err != nil {
			assert.ErrorIs(t, err, io.EOF)
			break
		}

		require.NoError(t, err)
		assert.Equal(t, &hash1, utxo.TxID)
		// t.Logf("%v", utxo)

		i++
	}

	assert.Equal(t, count, uint32(i))
}
