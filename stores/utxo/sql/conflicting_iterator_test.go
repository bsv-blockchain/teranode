package sql

import (
	"context"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSQLGetConflictingTxIterator verifies the new iterator only yields
// conflicting=true rows and is complementary to GetUnminedTxIterator.
func TestSQLGetConflictingTxIterator(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, tx := setup(ctx, t)

	// Create the tx. No conflicting flag yet.
	_, err := store.Create(ctx, tx, 0)
	require.NoError(t, err)

	// Before marking conflicting: iterator should be empty.
	it, err := store.GetConflictingTxIterator()
	require.NoError(t, err)

	batch, err := it.Next(ctx)
	require.NoError(t, err)
	assert.Empty(t, batch)
	require.NoError(t, it.Close())

	// Flip the conflicting flag directly.
	_, err = store.db.ExecContext(ctx,
		`UPDATE transactions SET conflicting = true WHERE hash = $1`,
		tx.TxIDChainHash()[:])
	require.NoError(t, err)

	// Now iterator should return it.
	it2, err := store.GetConflictingTxIterator()
	require.NoError(t, err)

	collected := make([]*utxo.UnminedTransaction, 0)
	for {
		batch, err := it2.Next(ctx)
		require.NoError(t, err)
		if batch == nil {
			break
		}
		collected = append(collected, batch...)
	}
	require.NoError(t, it2.Close())

	require.Len(t, collected, 1)
	assert.Equal(t, tx.TxIDChainHash().String(), collected[0].Hash.String())
}

// TestSQLConflictingTxHashes pins that the indexed lookup tracks the conflicting
// flag through the store's own writers: SetConflicting in both directions and a
// create of an already-conflicting transaction.
func TestSQLConflictingTxHashes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, parent := setup(ctx, t)

	_, err := store.Create(ctx, parent, 0)
	require.NoError(t, err)

	childOf := func(vout uint32) *bt.Tx {
		child := bt.NewTx()
		require.NoError(t, child.From(parent.TxID(), vout, parent.Outputs[vout].LockingScript.String(), parent.Outputs[vout].Satoshis))
		require.NoError(t, child.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000))
		child.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{bscript.OpTRUE})

		return child
	}

	child := childOf(0)
	_, err = store.Create(ctx, child, 0)
	require.NoError(t, err)

	hashes, indexed, err := store.ConflictingTxHashes(ctx)
	require.NoError(t, err)
	require.True(t, indexed)
	require.Empty(t, hashes)

	_, _, err = store.SetConflicting(ctx, []chainhash.Hash{*child.TxIDChainHash()}, true)
	require.NoError(t, err)

	hashes, indexed, err = store.ConflictingTxHashes(ctx)
	require.NoError(t, err)
	require.True(t, indexed)
	require.Equal(t, []chainhash.Hash{*child.TxIDChainHash()}, hashes)

	_, _, err = store.SetConflicting(ctx, []chainhash.Hash{*child.TxIDChainHash()}, false)
	require.NoError(t, err)

	hashes, _, err = store.ConflictingTxHashes(ctx)
	require.NoError(t, err)
	require.Empty(t, hashes)

	created := childOf(1)
	_, err = store.Create(ctx, created, 0, utxo.WithConflicting(true))
	require.NoError(t, err)

	hashes, _, err = store.ConflictingTxHashes(ctx)
	require.NoError(t, err)
	require.Equal(t, []chainhash.Hash{*created.TxIDChainHash()}, hashes)
}

// TestSQLConflictingTxHashes_ReadErrors pins that a failed read is an error, never
// an indexed empty set, which block validation would read as "none conflicting".
func TestSQLConflictingTxHashes_ReadErrors(t *testing.T) {
	query := regexp.QuoteMeta(`SELECT hash FROM transactions WHERE conflicting = true`)
	goodHash := make([]byte, chainhash.HashSize)

	tests := map[string]struct {
		expect func(sqlmock.Sqlmock)
		want   string
	}{
		"query fails": {
			expect: func(m sqlmock.Sqlmock) {
				m.ExpectQuery(query).WillReturnError(errors.NewStorageError("connection reset"))
			},
			want: "query failed",
		},
		"hash of the wrong length": {
			expect: func(m sqlmock.Sqlmock) {
				m.ExpectQuery(query).WillReturnRows(sqlmock.NewRows([]string{"hash"}).AddRow([]byte{1, 2, 3}))
			},
			want: "bad hash",
		},
		"iteration fails": {
			expect: func(m sqlmock.Sqlmock) {
				m.ExpectQuery(query).WillReturnRows(sqlmock.NewRows([]string{"hash"}).AddRow(goodHash).RowError(0, errors.NewStorageError("connection reset")))
			},
			want: "row iteration failed",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			store, mock := CreateMockStore(ulogger.TestLogger{})
			store.conflictingIndexReady.Store(true)
			tc.expect(mock)

			hashes, indexed, err := store.ConflictingTxHashes(context.Background())
			require.ErrorContains(t, err, tc.want)
			require.False(t, indexed)
			require.Nil(t, hashes)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
