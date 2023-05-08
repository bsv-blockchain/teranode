package memory

import (
	"context"
	"testing"

	"github.com/TAAL-GmbH/ubsv/services/seeder/store"
	"github.com/libsv/go-bk/bec"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
	"github.com/stretchr/testify/require"
)

func TestMemoryStore(t *testing.T) {
	ctx := context.Background()
	m := NewMemorySeederStore()

	// Create three transactions to put in the store
	tx1 := &store.SpendableTransaction{
		Txid:              &chainhash.Hash{},
		NumberOfOutputs:   1,
		SatoshisPerOutput: 1000,
		PrivateKey:        &bec.PrivateKey{},
	}

	tx2 := &store.SpendableTransaction{
		Txid:              &chainhash.Hash{},
		NumberOfOutputs:   2,
		SatoshisPerOutput: 2000,
		PrivateKey:        &bec.PrivateKey{},
	}

	tx3 := &store.SpendableTransaction{
		Txid:              &chainhash.Hash{},
		NumberOfOutputs:   3,
		SatoshisPerOutput: 3000,
		PrivateKey:        &bec.PrivateKey{},
	}

	// Push the transactions to the store
	err := m.Push(ctx, tx1)
	require.NoError(t, err)

	err = m.Push(ctx, tx2)
	require.NoError(t, err)

	err = m.Push(ctx, tx3)
	require.NoError(t, err)

	// Iterate over the store and verify that the transactions are present
	it := m.Iterator()
	expectedTxs := []*store.SpendableTransaction{tx1, tx2, tx3}

	i := 0

	for {
		tx, err := it.Next()
		if err != nil {
			require.Equal(t, "no more transactions", err.Error())
			break
		}
		require.Equal(t, expectedTxs[i], tx)
		i++
	}
}
