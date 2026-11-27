package packedsql

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
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

func TestConflictingTxHashes(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	hashes, indexed, err := store.ConflictingTxHashes(ctx)
	require.NoError(t, err)
	require.True(t, indexed)
	require.Empty(t, hashes)

	parent := newExtendedTx(t, 2, 610_000)
	_, err = store.Create(ctx, parent, 100)
	require.NoError(t, err)

	conflicting := newSpendingTx(t, parent, 0)
	_, err = store.Create(ctx, conflicting, 101, utxo.WithConflicting(true))
	require.NoError(t, err)

	clean := newSpendingTx(t, parent, 1)
	_, _, err = store.SpendAndCreate(ctx, clean, 101)
	require.NoError(t, err)

	hashes, indexed, err = store.ConflictingTxHashes(ctx)
	require.NoError(t, err)
	require.True(t, indexed)
	require.ElementsMatch(t, []chainhash.Hash{*conflicting.TxIDChainHash()}, hashes)

	require.NoError(t, store.SetBlockHeight(101))

	_, _, err = store.SetConflicting(ctx, []chainhash.Hash{*clean.TxIDChainHash()}, true)
	require.NoError(t, err)

	_, _, err = store.SetConflicting(ctx, []chainhash.Hash{*conflicting.TxIDChainHash()}, false)
	require.NoError(t, err)

	hashes, indexed, err = store.ConflictingTxHashes(ctx)
	require.NoError(t, err)
	require.True(t, indexed)
	require.ElementsMatch(t, []chainhash.Hash{*clean.TxIDChainHash()}, hashes)
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

func TestConflictLockOwnership(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	parent := newExtendedTx(t, 1, 620_000)
	_, err := store.Create(ctx, parent, 100)
	require.NoError(t, err)

	parents := []chainhash.Hash{*parent.TxIDChainHash()}
	owner := chainhash.HashH([]byte("recovery-operation"))
	other := chainhash.HashH([]byte("other-operation"))

	require.NoError(t, store.SetConflictLocks(ctx, owner, parents, true))
	require.Error(t, store.SetConflictLocks(ctx, other, parents, true), "a lock another operation owns cannot be taken")
	require.Error(t, store.SetConflictLocks(ctx, other, parents, false), "a lock another operation owns cannot be released")
	require.NoError(t, store.ReleaseConflictLocks(ctx, other, parents), "releasing a lock never owned leaves it as it is")

	var flags int16

	require.NoError(t, store.pool.QueryRow(ctx, `SELECT flags FROM packed_txs WHERE hash = $1`, parent.TxIDChainHash()[:]).Scan(&flags))
	require.NotZero(t, flags&flagLocked)

	require.NoError(t, store.SetConflictLocks(ctx, owner, parents, true), "the owner may take its own lock again")

	require.NoError(t, store.SetLocked(ctx, parents, true))
	require.Error(t, store.SetConflictLocks(ctx, owner, parents, false), "an ordinary lock writer replaces the ownership")

	var ownerCol []byte

	require.NoError(t, store.pool.QueryRow(ctx, `SELECT conflict_owner FROM packed_txs WHERE hash = $1`, parent.TxIDChainHash()[:]).Scan(&ownerCol))
	require.Nil(t, ownerCol)

	require.NoError(t, store.SetLocked(ctx, parents, false))
	require.NoError(t, store.SetConflictLocks(ctx, owner, parents, false), "an unlocked parent needs no release")

	require.NoError(t, store.SetConflictLocks(ctx, owner, parents, true))
	require.NoError(t, store.ReleaseConflictLocks(ctx, owner, parents))

	require.NoError(t, store.pool.QueryRow(ctx, `SELECT flags FROM packed_txs WHERE hash = $1`, parent.TxIDChainHash()[:]).Scan(&flags))
	require.Zero(t, flags&flagLocked)

	missing := []chainhash.Hash{chainhash.HashH([]byte("no such parent"))}
	require.Error(t, store.SetConflictLocks(ctx, owner, missing, true))
	require.Error(t, store.SetConflictLocks(ctx, owner, missing, false))
}

func TestConflictRecoverySaveRequiresIntent(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	recovery := &utxo.ConflictRecovery{Version: 1, Phase: utxo.ConflictRecoveryPlanning}
	missing := chainhash.HashH([]byte("no such intent"))

	require.Error(t, store.SaveConflictRecovery(ctx, missing, recovery))

	loaded, err := store.LoadConflictRecovery(ctx, missing)
	require.NoError(t, err)
	require.Nil(t, loaded)

	intent := utxo.ConflictIntent{
		Kind:      utxo.ConflictIntentForward,
		BlockHash: chainhash.HashH([]byte("block")),
		Recovery:  recovery,
	}
	require.NoError(t, store.BeginConflictIntent(ctx, intent))

	saved := &utxo.ConflictRecovery{Version: 1, Phase: utxo.ConflictRecoveryMutating}
	require.NoError(t, store.SaveConflictRecovery(ctx, intent.IntentID(), saved))
	require.NoError(t, store.BeginConflictIntent(ctx, intent), "beginning an existing intent keeps its progress")

	loaded, err = store.LoadConflictRecovery(ctx, intent.IntentID())
	require.NoError(t, err)
	require.Equal(t, saved, loaded)

	pending, err := store.PendingConflictIntents(ctx)
	require.NoError(t, err)
	require.Len(t, pending, 1)
	require.Equal(t, intent.IntentID(), pending[0].ID)
	require.Nil(t, pending[0].Recovery)
}

func TestSpendsMadeBy(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	parent := newExtendedTx(t, 2, 630_000)
	_, err := store.Create(ctx, parent, 100)
	require.NoError(t, err)

	child := newSpendingTx(t, parent, 0, 1)
	_, _, err = store.SpendAndCreate(ctx, child, 101)
	require.NoError(t, err)

	want, err := utxo.GetSpends(child)
	require.NoError(t, err)

	got, err := store.SpendsMadeBy(ctx, *child.TxIDChainHash())
	require.NoError(t, err)
	require.Len(t, got, 2)

	for i, sp := range got {
		require.Equal(t, *want[i].TxID, *sp.TxID)
		require.Equal(t, want[i].Vout, sp.Vout)
		require.Equal(t, *want[i].UTXOHash, *sp.UTXOHash)
		require.Equal(t, *child.TxIDChainHash(), *sp.SpendingData.TxID)
		require.Equal(t, i, sp.SpendingData.Vin)
	}

	require.NoError(t, store.Unspend(ctx, got))

	md, err := store.Get(ctx, parent.TxIDChainHash(), fields.Utxos)
	require.NoError(t, err)

	for _, sd := range md.SpendingDatas {
		require.Nil(t, sd, "the records SpendsMadeBy returns must restore the coins")
	}

	_, err = store.SpendsMadeBy(ctx, chainhash.HashH([]byte("no such tx")))
	require.ErrorIs(t, err, errors.ErrTxNotFound)
}

func TestSpendsMadeByStrippedInputs(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	parent := newExtendedTx(t, 1, 640_000)
	_, err := store.Create(ctx, parent, 100)
	require.NoError(t, err)

	child := newSpendingTx(t, parent, 0)
	_, _, err = store.SpendAndCreate(ctx, child, 101, utxo.WithSkipExtendedInputs(true))
	require.NoError(t, err)

	want, err := utxo.GetSpends(child)
	require.NoError(t, err)

	got, err := store.SpendsMadeBy(ctx, *child.TxIDChainHash())
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, *want[0].UTXOHash, *got[0].UTXOHash, "the coin hash must come from the parent's stored output")
}
