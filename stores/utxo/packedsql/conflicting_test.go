package packedsql

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

func TestSetConflictingTrue(t *testing.T) {
	store := newTestStore(t)

	parent := newExtendedTx(t, 1, 600_000)
	_, err := store.Create(context.Background(), parent, 100)
	require.NoError(t, err)

	child := newSpendingTx(t, parent, 0)
	_, _, err = store.SpendAndCreate(context.Background(), child, 101)
	require.NoError(t, err)

	require.NoError(t, store.SetBlockHeight(101))

	spends, spenders, err := store.SetConflicting(context.Background(),
		[]chainhash.Hash{*child.TxIDChainHash()}, true)
	require.NoError(t, err)

	require.Len(t, spends, 1)
	require.Equal(t, *parent.TxIDChainHash(), *spends[0].TxID)
	require.Equal(t, child.TxID(), spends[0].SpendingData.TxID.String())
	require.Empty(t, spenders)

	var flags int16

	var dah *int64
	require.NoError(t, store.pool.QueryRow(context.Background(),
		`SELECT flags, delete_at_height FROM packed_txs WHERE hash = $1`,
		child.TxIDChainHash()[:]).Scan(&flags, &dah))
	require.NotZero(t, flags&flagConflicting)
	require.NotNil(t, dah)

	children, err := store.GetConflictingChildren(context.Background(), *parent.TxIDChainHash())
	require.NoError(t, err)
	require.Contains(t, children, *child.TxIDChainHash())

	_, _, err = store.SetConflicting(context.Background(),
		[]chainhash.Hash{*child.TxIDChainHash()}, false)
	require.NoError(t, err)

	require.NoError(t, store.pool.QueryRow(context.Background(),
		`SELECT flags, delete_at_height FROM packed_txs WHERE hash = $1`,
		child.TxIDChainHash()[:]).Scan(&flags, &dah))
	require.Zero(t, flags&flagConflicting)
	require.Nil(t, dah)
}

func TestGetCounterConflicting(t *testing.T) {
	store := newTestStore(t)

	parent := newExtendedTx(t, 1, 610_000)
	_, err := store.Create(context.Background(), parent, 100)
	require.NoError(t, err)

	childA := newSpendingTx(t, parent, 0)
	_, _, err = store.SpendAndCreate(context.Background(), childA, 101)
	require.NoError(t, err)

	childB := newSpendingTx(t, parent, 0)
	childB.Outputs[0].Satoshis = 999
	_, err = store.Create(context.Background(), childB, 101, utxo.WithConflicting(true))
	require.NoError(t, err)

	_, _, err = store.SetConflicting(context.Background(), []chainhash.Hash{*childB.TxIDChainHash()}, true)
	require.NoError(t, err)

	counter, err := store.GetCounterConflicting(context.Background(), *childB.TxIDChainHash())
	require.NoError(t, err)
	require.Contains(t, counter, *childA.TxIDChainHash())
}

func TestRemoveFromConflictingChildren(t *testing.T) {
	store := newTestStore(t)

	parent := newExtendedTx(t, 1, 620_000)
	_, err := store.Create(context.Background(), parent, 100)
	require.NoError(t, err)

	child := newSpendingTx(t, parent, 0)
	_, _, err = store.SpendAndCreate(context.Background(), child, 101)
	require.NoError(t, err)

	_, _, err = store.SetConflicting(context.Background(), []chainhash.Hash{*child.TxIDChainHash()}, true)
	require.NoError(t, err)

	require.NoError(t, store.RemoveFromConflictingChildren(context.Background(),
		[]utxo.ConflictingChildRemoval{{ParentHash: parent.TxIDChainHash(), ChildHash: child.TxIDChainHash()}}))

	var n int
	require.NoError(t, store.pool.QueryRow(context.Background(),
		`SELECT count(*) FROM conflicting_children WHERE hash = $1`, parent.TxIDChainHash()[:]).Scan(&n))
	require.Zero(t, n)

	require.NoError(t, store.RemoveFromConflictingChildren(context.Background(),
		[]utxo.ConflictingChildRemoval{{ParentHash: parent.TxIDChainHash(), ChildHash: child.TxIDChainHash()}}))
}

func TestSetLockedBothWays(t *testing.T) {
	store := newTestStore(t)

	tx := newExtendedTx(t, 1, 630_000)
	_, err := store.Create(context.Background(), tx, 100)
	require.NoError(t, err)

	require.NoError(t, store.SetLocked(context.Background(), []chainhash.Hash{*tx.TxIDChainHash()}, true))

	var flags int16
	require.NoError(t, store.pool.QueryRow(context.Background(),
		`SELECT flags FROM packed_txs WHERE hash = $1`, tx.TxIDChainHash()[:]).Scan(&flags))
	require.NotZero(t, flags&flagLocked)

	require.NoError(t, store.SetLocked(context.Background(), []chainhash.Hash{*tx.TxIDChainHash()}, false))

	require.NoError(t, store.pool.QueryRow(context.Background(),
		`SELECT flags FROM packed_txs WHERE hash = $1`, tx.TxIDChainHash()[:]).Scan(&flags))
	require.Zero(t, flags&flagLocked)
}

func TestRemoveBlockIDs(t *testing.T) {
	store := newTestStore(t)

	tx := newExtendedTx(t, 1, 640_000)
	_, err := store.Create(context.Background(), tx, 100,
		utxo.WithMinedBlockInfo(
			utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 100, OnLongestChain: true},
			utxo.MinedBlockInfo{BlockID: 2, BlockHeight: 101, OnLongestChain: true}))
	require.NoError(t, err)

	require.NoError(t, store.RemoveBlockIDs(context.Background(),
		[]utxo.BlockIDsRemoval{{TxHash: tx.TxIDChainHash(), BlockIDs: []uint32{1}}}))

	var blockRefs []byte

	var unminedSince *int64
	require.NoError(t, store.pool.QueryRow(context.Background(),
		`SELECT block_refs, unmined_since FROM packed_txs WHERE hash = $1`,
		tx.TxIDChainHash()[:]).Scan(&blockRefs, &unminedSince))

	ids, _, _ := unpackBlockRefs(blockRefs)
	require.Equal(t, []uint32{2}, ids)
	require.Nil(t, unminedSince)

	missing := newExtendedTx(t, 1, 641_000)
	require.NoError(t, store.RemoveBlockIDs(context.Background(),
		[]utxo.BlockIDsRemoval{{TxHash: missing.TxIDChainHash(), BlockIDs: []uint32{1}}}))
}

func TestConflictWALRoundTrip(t *testing.T) {
	store := newTestStore(t)

	parent := newExtendedTx(t, 1, 650_000)

	intent := utxo.ConflictIntent{
		Kind:        utxo.ConflictIntentForward,
		BlockHeight: 123,
		BlockHash:   *parent.TxIDChainHash(),
		TxHashes:    []chainhash.Hash{*newExtendedTx(t, 1, 651_000).TxIDChainHash(), *newExtendedTx(t, 1, 652_000).TxIDChainHash()},
		StartedAt:   time.Now().UnixNano(),
	}

	require.NoError(t, store.BeginConflictIntent(context.Background(), intent))
	require.NoError(t, store.BeginConflictIntent(context.Background(), intent))

	pending, err := store.PendingConflictIntents(context.Background())
	require.NoError(t, err)
	require.Len(t, pending, 1)
	require.Equal(t, intent.Kind, pending[0].Kind)
	require.Equal(t, intent.BlockHeight, pending[0].BlockHeight)
	require.Equal(t, intent.BlockHash, pending[0].BlockHash)
	require.Equal(t, intent.IntentID(), pending[0].IntentID())
	require.Equal(t, intent.StartedAt, pending[0].StartedAt)

	require.NoError(t, store.CompleteConflictIntent(context.Background(), intent.IntentID()))

	pending, err = store.PendingConflictIntents(context.Background())
	require.NoError(t, err)
	require.Empty(t, pending)

	require.NoError(t, store.CompleteConflictIntent(context.Background(), intent.IntentID()))
}
