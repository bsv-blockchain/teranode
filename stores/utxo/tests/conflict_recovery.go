package tests

import (
	"context"
	"fmt"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	utxo "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

type failConflictUnlock struct {
	utxo.Store
	fail bool
}

func (s *failConflictUnlock) SetConflictLocks(ctx context.Context, id chainhash.Hash, hashes []chainhash.Hash, value bool) error {
	if !value && s.fail {
		return errors.NewStorageError("injected final unlock failure")
	}
	return s.Store.SetConflictLocks(ctx, id, hashes, value)
}

// ConflictRecoveryDescendants runs the real write path before retrying. In
// particular, it must not manufacture SpendingDatas that Unspend has removed.
func ConflictRecoveryDescendants(t *testing.T, db utxo.Store) {
	ctx := context.Background()
	parent, other := crashParentTx(0x91), crashParentTx(0x92)
	for _, tx := range []*bt.Tx{parent, other} {
		_, _, err := db.SpendAndCreate(ctx, tx, 1, utxo.WithCreateOnly())
		require.NoError(t, err)
	}
	loser := crashSpendTx(t, parent, 80000)
	winner := crashSpendTx(t, parent, 70000)
	_, _, err := db.SpendAndCreate(ctx, loser, 2)
	require.NoError(t, err)
	child := crashSpendTx(t, loser, 60000)
	require.NoError(t, child.From(other.TxIDChainHash().String(), 0, other.Outputs[0].LockingScript.String(), other.Outputs[0].Satoshis))
	child.Inputs[1].UnlockingScript = dummyUnlockingScript
	_, _, err = db.SpendAndCreate(ctx, child, 2)
	require.NoError(t, err)
	_, _, err = db.SpendAndCreate(ctx, winner, 2, utxo.WithCreateOnly(), utxo.WithConflicting(true))
	require.NoError(t, err)
	wrapped := &failConflictUnlock{Store: db, fail: true}
	winners := []chainhash.Hash{*winner.TxIDChainHash()}
	_, _, err = utxo.ProcessConflicting(ctx, wrapped, 3, chainhash.Hash{}, winners, nil, utxo.NoAncestryGuard)
	require.Error(t, err)
	require.Nil(t, crashSpender(t, ctx, db, loser), "the first run must erase the descendant link")
	wrapped.fail = false
	losers, marked, err := utxo.ProcessConflicting(ctx, wrapped, 3, chainhash.Hash{}, winners, nil, utxo.NoAncestryGuard)
	require.NoError(t, err)
	require.True(t, losers.Exists(*child.TxIDChainHash()), "retry must recover the child")
	require.Contains(t, marked, *child.TxIDChainHash())
	require.True(t, losers.Exists(*loser.TxIDChainHash()))
	require.False(t, losers.Exists(*winner.TxIDChainHash()), "a winner must not be returned as a loser")
	require.NotContains(t, marked, *winner.TxIDChainHash(), "a winner must not be returned as marked")
	require.False(t, crashLocked(t, ctx, db, other.TxIDChainHash()))
}

// failConflictPhase wraps a real store. crash=true suppresses compensation to
// model process loss; the next call uses the underlying store directly.
type failConflictPhase struct {
	utxo.Store
	stage string
	crash bool
	fired bool
}

func (s *failConflictPhase) fail(stage string) error {
	if s.stage == stage && !s.fired {
		s.fired = true
		return errors.NewStorageError("injected conflict failure at %s", stage)
	}
	return nil
}
func (s *failConflictPhase) SaveConflictRecovery(ctx context.Context, id chainhash.Hash, r *utxo.ConflictRecovery) error {
	if s.crash && s.fired && r.Phase == utxo.ConflictRecoveryRollingBack {
		return errors.NewStorageError("process stopped before compensation")
	}
	if r.Phase == utxo.ConflictRecoveryApplied {
		if err := s.fail("publish_applied"); err != nil {
			return err
		}
	}
	return s.Store.SaveConflictRecovery(ctx, id, r)
}
func (s *failConflictPhase) SetConflicting(ctx context.Context, hashes []chainhash.Hash, value bool) ([]*utxo.Spend, []chainhash.Hash, error) {
	spends, children, err := s.Store.SetConflicting(ctx, hashes, value)
	if err != nil {
		return spends, children, err
	}
	stage := "clear_winners"
	if value {
		stage = "mark"
	}
	return spends, children, s.fail(stage)
}
func (s *failConflictPhase) Unspend(ctx context.Context, spends []*utxo.Spend, locked ...bool) error {
	if err := s.Store.Unspend(ctx, spends, locked...); err != nil {
		return err
	}
	return s.fail("unspend")
}
func (s *failConflictPhase) RestoreConflictSpends(ctx context.Context, spends []*utxo.Spend, height uint32) error {
	if s.stage == "partial_spend" && !s.fired && len(spends) > 0 {
		if err := s.Store.RestoreConflictSpends(ctx, spends[:1], height); err != nil {
			return err
		}
		return s.fail("partial_spend")
	}
	return s.Store.RestoreConflictSpends(ctx, spends, height)
}

func ConflictRecoveryFailures(t *testing.T, db utxo.Store) {
	ctx := context.Background()
	for index, stage := range []string{"mark", "unspend", "partial_spend", "clear_winners", "publish_applied"} {
		for _, crash := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/crash=%t", stage, crash), func(t *testing.T) {
				seed := []byte{0xa0, 0xa2, 0xa4, 0xa6, 0xa8}[index]
				if crash {
					seed++
				}
				parent := crashParentTx(seed)
				_, _, err := db.SpendAndCreate(ctx, parent, 1, utxo.WithCreateOnly())
				require.NoError(t, err)
				loser := crashSpendTx(t, parent, 80000)
				_, _, err = db.SpendAndCreate(ctx, loser, 2)
				require.NoError(t, err)
				child := crashSpendTx(t, loser, 60000)
				_, _, err = db.SpendAndCreate(ctx, child, 2)
				require.NoError(t, err)
				// This pre-existing flag belongs to an earlier operation and must survive rollback.
				_, _, err = db.SetConflicting(ctx, []chainhash.Hash{*child.TxIDChainHash()}, true)
				require.NoError(t, err)
				winner := crashSpendTx(t, parent, 70000)
				_, _, err = db.SpendAndCreate(ctx, winner, 2, utxo.WithCreateOnly(), utxo.WithConflicting(true))
				require.NoError(t, err)
				winners := []chainhash.Hash{*winner.TxIDChainHash()}
				wrapper := &failConflictPhase{Store: db, stage: stage, crash: crash}
				_, _, err = utxo.ProcessConflicting(ctx, wrapper, 3, chainhash.Hash{}, winners, nil, utxo.NoAncestryGuard)
				require.Error(t, err)
				require.True(t, wrapper.fired)
				intent := utxo.ConflictIntent{Kind: utxo.ConflictIntentForward, BlockHeight: 3, TxHashes: winners}
				if crash {
					// Exercise the public recovery entrypoint separately so original state
					// can be checked before a subsequent forward attempt changes it again.
					require.NoError(t, utxo.RollbackConflictIntent(ctx, db, intent))
				}
				require.True(t, crashConflicting(t, ctx, db, winner.TxIDChainHash()), "pre-flagged winner must remain flagged")
				require.True(t, crashConflicting(t, ctx, db, child.TxIDChainHash()), "pre-flagged descendant must remain flagged")
				require.False(t, crashConflicting(t, ctx, db, loser.TxIDChainHash()))
				require.Equal(t, loser.TxIDChainHash(), crashSpender(t, ctx, db, parent))
				require.Equal(t, child.TxIDChainHash(), crashSpender(t, ctx, db, loser))
				require.False(t, crashLocked(t, ctx, db, parent.TxIDChainHash()))
				r, err := db.LoadConflictRecovery(ctx, intent.IntentID())
				require.NoError(t, err)
				require.Equal(t, utxo.ConflictRecoveryRolledBack, r.Phase)
				losers, marked, err := utxo.ProcessConflicting(ctx, db, 3, chainhash.Hash{}, winners, nil, utxo.NoAncestryGuard, utxo.WithDeferredConflictCompletion())
				require.NoError(t, err)
				require.True(t, losers.Exists(*child.TxIDChainHash()))
				require.Contains(t, marked, *child.TxIDChainHash())
				require.False(t, losers.Exists(*winner.TxIDChainHash()), "a winner must not be returned as a loser")
				require.NotContains(t, marked, *winner.TxIDChainHash(), "a winner must not be returned as marked")
				require.Equal(t, winner.TxIDChainHash(), crashSpender(t, ctx, db, parent))
				require.True(t, crashConflicting(t, ctx, db, loser.TxIDChainHash()))
				// Simulate a caller failure, then retry using the same saved output set.
				again, markedAgain, err := utxo.ProcessConflicting(ctx, db, 3, chainhash.Hash{}, winners, nil, utxo.NoAncestryGuard, utxo.WithDeferredConflictCompletion())
				require.NoError(t, err)
				require.ElementsMatch(t, losers.Keys(), again.Keys())
				require.ElementsMatch(t, marked, markedAgain)
				require.NoError(t, db.CompleteConflictIntent(ctx, intent.IntentID()))
			})
		}
	}
}

// ConflictRecoveryPersistence crosses Aerospike's chunk boundary and verifies
// that an idempotent Begin cannot replace an existing checklist.
func ConflictRecoveryPersistence(t *testing.T, db utxo.Store) {
	ctx := t.Context()
	intent := utxo.ConflictIntent{Kind: utxo.ConflictIntentForward, BlockHash: chainhash.HashH([]byte("large-checklist")), Recovery: &utxo.ConflictRecovery{Version: 1, Phase: utxo.ConflictRecoveryPlanning}}
	require.NoError(t, db.BeginConflictIntent(ctx, intent))
	r := &utxo.ConflictRecovery{Version: 1, Phase: utxo.ConflictRecoveryPrepared}
	for i := 0; i < 2000; i++ {
		h := chainhash.HashH([]byte(fmt.Sprintf("checklist-%d", i)))
		r.Transactions = append(r.Transactions, utxo.ConflictTransaction{Hash: h, Conflicting: i%2 == 0})
		r.Losers = append(r.Losers, h)
	}
	encoded, err := utxo.EncodeConflictRecovery(r)
	require.NoError(t, err)
	require.Greater(t, len(encoded), 64*1024)
	require.NoError(t, db.SaveConflictRecovery(ctx, intent.IntentID(), r))
	require.NoError(t, db.BeginConflictIntent(ctx, intent))
	actual, err := db.LoadConflictRecovery(ctx, intent.IntentID())
	require.NoError(t, err)
	require.Equal(t, r, actual)
	r.Phase = utxo.ConflictRecoveryMutating
	require.NoError(t, db.SaveConflictRecovery(ctx, intent.IntentID(), r))
	actual, err = db.LoadConflictRecovery(ctx, intent.IntentID())
	require.NoError(t, err)
	require.Equal(t, r, actual)
	// Caller acknowledgement cannot delete unfinished work or another block's WAL.
	require.NoError(t, utxo.CompleteConflictIntentsForBlock(ctx, db, intent.BlockHash))
	actual, err = db.LoadConflictRecovery(ctx, intent.IntentID())
	require.NoError(t, err)
	require.Equal(t, r, actual)
	r.Phase = utxo.ConflictRecoveryFinished
	require.NoError(t, db.SaveConflictRecovery(ctx, intent.IntentID(), r))
	require.NoError(t, utxo.CompleteConflictIntentsForBlock(ctx, db, chainhash.Hash{}))
	actual, err = db.LoadConflictRecovery(ctx, intent.IntentID())
	require.NoError(t, err)
	require.Equal(t, r, actual)
	require.NoError(t, utxo.CompleteConflictIntentsForBlock(ctx, db, intent.BlockHash))
	require.NoError(t, db.CompleteConflictIntent(ctx, intent.IntentID()))
	actual, err = db.LoadConflictRecovery(ctx, intent.IntentID())
	require.NoError(t, err)
	require.Nil(t, actual)
}

func ConflictRecoveryUnexpectedOwner(t *testing.T, db utxo.Store) {
	ctx := t.Context()
	parent := crashParentTx(0xe1)
	_, _, err := db.SpendAndCreate(ctx, parent, 1, utxo.WithCreateOnly())
	require.NoError(t, err)
	loser := crashSpendTx(t, parent, 80000)
	_, _, err = db.SpendAndCreate(ctx, loser, 2)
	require.NoError(t, err)
	winner := crashSpendTx(t, parent, 70000)
	_, _, err = db.SpendAndCreate(ctx, winner, 2, utxo.WithCreateOnly(), utxo.WithConflicting(true))
	require.NoError(t, err)
	winners := []chainhash.Hash{*winner.TxIDChainHash()}
	_, _, err = utxo.ProcessConflicting(ctx, db, 3, chainhash.Hash{}, winners, nil, utxo.NoAncestryGuard, utxo.WithDeferredConflictCompletion())
	require.NoError(t, err)
	winningSpends, err := utxo.GetSpends(winner)
	require.NoError(t, err)
	require.NoError(t, db.Unspend(ctx, winningSpends))
	stranger := crashSpendTx(t, parent, 50000)
	_, _, err = db.SpendAndCreate(ctx, stranger, 3)
	require.NoError(t, err)
	intent := utxo.ConflictIntent{Kind: utxo.ConflictIntentForward, BlockHeight: 3, TxHashes: winners}
	require.Error(t, utxo.RollbackConflictIntent(ctx, db, intent))
	require.Equal(t, stranger.TxIDChainHash(), crashSpender(t, ctx, db, parent), "rollback must not overwrite an unexpected owner")
	require.True(t, crashConflicting(t, ctx, db, winner.TxIDChainHash()))
	require.True(t, crashConflicting(t, ctx, db, loser.TxIDChainHash()))
	r, err := db.LoadConflictRecovery(ctx, intent.IntentID())
	require.NoError(t, err)
	require.Equal(t, utxo.ConflictRecoveryRollingBack, r.Phase)
	// Restore the expected owner and finish recovery to leave the shared store clean.
	strangers, err := utxo.GetSpends(stranger)
	require.NoError(t, err)
	require.NoError(t, db.Unspend(ctx, strangers))
	require.NoError(t, utxo.RollbackConflictIntent(ctx, db, intent))
	require.NoError(t, db.CompleteConflictIntent(ctx, intent.IntentID()))
}

// Related winners must remain in the complete winner set during the ancestry
// guard. A winning child spending its winning parent is not a losing ancestor.
func ConflictRecoveryRelatedWinners(t *testing.T, db utxo.Store) {
	ctx := t.Context()
	parent := crashParentTx(0xe2)
	_, _, err := db.SpendAndCreate(ctx, parent, 1, utxo.WithCreateOnly())
	require.NoError(t, err)
	loser := crashSpendTx(t, parent, 80000)
	_, _, err = db.SpendAndCreate(ctx, loser, 2)
	require.NoError(t, err)
	winner := crashSpendTx(t, parent, 70000)
	child := crashSpendTx(t, winner, 60000)
	for _, tx := range []*bt.Tx{winner, child} {
		_, _, err = db.SpendAndCreate(ctx, tx, 2, utxo.WithCreateOnly(), utxo.WithConflicting(true))
		require.NoError(t, err)
	}
	_, err = db.SetMinedMulti(ctx, []*chainhash.Hash{winner.TxIDChainHash(), child.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: 99, BlockHeight: 3})
	require.NoError(t, err)
	winners := []chainhash.Hash{*winner.TxIDChainHash(), *child.TxIDChainHash()}
	guard := utxo.NewAncestryGuard(func(context.Context, []uint32, *chainhash.Hash) (bool, error) { return true, nil })
	_, _, err = utxo.ProcessConflicting(ctx, db, 3, chainhash.Hash{}, winners, nil, guard)
	require.NoError(t, err)
	require.False(t, crashConflicting(t, ctx, db, winner.TxIDChainHash()))
	require.False(t, crashConflicting(t, ctx, db, child.TxIDChainHash()))
	require.Equal(t, winner.TxIDChainHash(), crashSpender(t, ctx, db, parent))
	require.Equal(t, child.TxIDChainHash(), crashSpender(t, ctx, db, winner))
}

// A WAL remains historical evidence even after its winner is mined again.
// Recovery must consult all mined block IDs, not just the triggering block.
func ConflictRecoveryConfirmedWinner(t *testing.T, db utxo.Store) {
	ctx := t.Context()
	parent := crashParentTx(0xe3)
	_, _, err := db.SpendAndCreate(ctx, parent, 1, utxo.WithCreateOnly())
	require.NoError(t, err)
	loser := crashSpendTx(t, parent, 80000)
	_, _, err = db.SpendAndCreate(ctx, loser, 2)
	require.NoError(t, err)
	winner := crashSpendTx(t, parent, 70000)
	_, _, err = db.SpendAndCreate(ctx, winner, 2, utxo.WithCreateOnly(), utxo.WithConflicting(true))
	require.NoError(t, err)
	intent := utxo.ConflictIntent{Kind: utxo.ConflictIntentForward, BlockHeight: 3, TxHashes: []chainhash.Hash{*winner.TxIDChainHash()}}
	_, _, err = utxo.ProcessConflicting(ctx, db, intent.BlockHeight, intent.BlockHash, intent.TxHashes, nil, utxo.NoAncestryGuard, utxo.WithDeferredConflictCompletion())
	require.NoError(t, err)
	for _, id := range []uint32{154, 155} {
		_, err = db.SetMinedMulti(ctx, []*chainhash.Hash{winner.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: id, BlockHeight: id})
		require.NoError(t, err)
	}
	for _, lookupFails := range []bool{false, true} {
		guard := utxo.NewCurrentChainGuard(func(_ context.Context, ids []uint32) (bool, error) {
			require.Contains(t, ids, uint32(155), "must check the later confirmation")
			if lookupFails {
				return false, errors.NewStorageError("injected chain lookup failure")
			}
			return true, nil
		})
		require.Error(t, utxo.RollbackConflictIntent(ctx, db, intent, guard))
		_, _, err = utxo.ReverseProcessConflicting(ctx, db, intent.BlockHeight, intent.BlockHash, intent.TxHashes, guard)
		require.Error(t, err)
		require.False(t, crashConflicting(t, ctx, db, winner.TxIDChainHash()))
		require.Equal(t, winner.TxIDChainHash(), crashSpender(t, ctx, db, parent))
		require.False(t, crashLocked(t, ctx, db, parent.TxIDChainHash()))
		r, err := db.LoadConflictRecovery(ctx, intent.IntentID())
		require.NoError(t, err)
		require.Equal(t, utxo.ConflictRecoveryFinished, r.Phase)
	}
	// If neither mined block remains on the current chain, undo is permitted.
	orphaned := utxo.NewCurrentChainGuard(func(context.Context, []uint32) (bool, error) { return false, nil })
	require.NoError(t, utxo.RollbackConflictIntent(ctx, db, intent, orphaned))
	require.Equal(t, loser.TxIDChainHash(), crashSpender(t, ctx, db, parent))
	require.True(t, crashConflicting(t, ctx, db, winner.TxIDChainHash()))
	require.NoError(t, db.CompleteConflictIntent(ctx, intent.IntentID()))
	intent.Kind = utxo.ConflictIntentReverse
	require.NoError(t, db.CompleteConflictIntent(ctx, intent.IntentID()))
}

// The winners of a forward operation are mined in the block being added, which is on the current
// chain. That block alone must not make the rollback refuse them. Any other current-chain block still does.
func ConflictRecoveryForwardOwnBlock(t *testing.T, db utxo.Store) {
	ctx := t.Context()
	ownBlock := chainhash.HashH([]byte("forward-own-block"))
	const ownID, otherID = uint32(401), uint32(402)
	onChain := map[uint32]bool{ownID: true}
	guard := utxo.NewCurrentChainGuard(func(_ context.Context, ids []uint32) (bool, error) {
		for _, id := range ids {
			if onChain[id] {
				return true, nil
			}
		}
		return false, nil
	}).WithBlockID(func(_ context.Context, h chainhash.Hash) (uint32, error) {
		require.Equal(t, ownBlock, h)
		return ownID, nil
	})

	for index, otherBlock := range []bool{false, true} {
		t.Run(fmt.Sprintf("otherCurrentChainBlock=%t", otherBlock), func(t *testing.T) {
			parent := crashParentTx(0xb1 + byte(index))
			_, _, err := db.SpendAndCreate(ctx, parent, 1, utxo.WithCreateOnly())
			require.NoError(t, err)
			loser := crashSpendTx(t, parent, 80000)
			_, _, err = db.SpendAndCreate(ctx, loser, 2)
			require.NoError(t, err)
			winner := crashSpendTx(t, parent, 70000)
			_, _, err = db.SpendAndCreate(ctx, winner, 2, utxo.WithCreateOnly(), utxo.WithConflicting(true))
			require.NoError(t, err)
			_, err = db.SetMinedMulti(ctx, []*chainhash.Hash{winner.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: ownID, BlockHeight: 3})
			require.NoError(t, err)

			if otherBlock {
				_, err = db.SetMinedMulti(ctx, []*chainhash.Hash{winner.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: otherID, BlockHeight: 4})
				require.NoError(t, err)

				onChain[otherID] = true
				defer delete(onChain, otherID)
			}

			winners := []chainhash.Hash{*winner.TxIDChainHash()}
			wrapper := &failConflictPhase{Store: db, stage: "unspend"}
			_, _, err = utxo.ProcessConflicting(ctx, wrapper, 3, ownBlock, winners, nil, utxo.NoAncestryGuard, utxo.WithDeferredConflictCompletion(), utxo.WithRecoveryChainGuard(guard))
			require.Error(t, err)
			require.True(t, wrapper.fired)

			intent := utxo.ConflictIntent{Kind: utxo.ConflictIntentForward, BlockHeight: 3, BlockHash: ownBlock, TxHashes: winners}
			r, err := db.LoadConflictRecovery(ctx, intent.IntentID())
			require.NoError(t, err)

			if otherBlock {
				require.NotEqual(t, utxo.ConflictRecoveryRolledBack, r.Phase, "a winner confirmed elsewhere must keep the checklist pending")
				require.ErrorContains(t, utxo.RollbackConflictIntent(ctx, db, intent, guard), "confirmed on the current chain")
				require.NoError(t, utxo.RollbackConflictIntent(ctx, db, intent))
				require.NoError(t, db.CompleteConflictIntent(ctx, intent.IntentID()))

				return
			}

			require.Equal(t, utxo.ConflictRecoveryRolledBack, r.Phase, "the rollback must not refuse the winners' own block")
			require.Equal(t, loser.TxIDChainHash(), crashSpender(t, ctx, db, parent))
			require.True(t, crashConflicting(t, ctx, db, winner.TxIDChainHash()))
			require.False(t, crashConflicting(t, ctx, db, loser.TxIDChainHash()))

			losers, marked, err := utxo.ProcessConflicting(ctx, db, 3, ownBlock, winners, nil, utxo.NoAncestryGuard, utxo.WithDeferredConflictCompletion(), utxo.WithRecoveryChainGuard(guard))
			require.NoError(t, err)
			require.True(t, losers.Exists(*loser.TxIDChainHash()))
			require.False(t, losers.Exists(*winner.TxIDChainHash()))
			require.Equal(t, []chainhash.Hash{*loser.TxIDChainHash()}, marked)
			require.Equal(t, winner.TxIDChainHash(), crashSpender(t, ctx, db, parent))
			require.NoError(t, db.CompleteConflictIntent(ctx, intent.IntentID()))
		})
	}
}

// A parent locked by another holder must not stop the operation, and the operation must neither
// take nor release that lock.
func ConflictRecoveryForeignLock(t *testing.T, db utxo.Store) {
	ctx := t.Context()
	parent, other := crashParentTx(0xb5), crashParentTx(0xb6)
	for _, tx := range []*bt.Tx{parent, other} {
		_, _, err := db.SpendAndCreate(ctx, tx, 1, utxo.WithCreateOnly())
		require.NoError(t, err)
	}
	loser := crashSpendTx(t, parent, 80000)
	winner := crashSpendTx(t, parent, 70000)
	_, _, err := db.SpendAndCreate(ctx, loser, 2)
	require.NoError(t, err)
	child := crashSpendTx(t, loser, 60000)
	require.NoError(t, child.From(other.TxIDChainHash().String(), 0, other.Outputs[0].LockingScript.String(), other.Outputs[0].Satoshis))
	child.Inputs[1].UnlockingScript = dummyUnlockingScript
	_, _, err = db.SpendAndCreate(ctx, child, 2)
	require.NoError(t, err)
	_, _, err = db.SpendAndCreate(ctx, winner, 2, utxo.WithCreateOnly(), utxo.WithConflicting(true))
	require.NoError(t, err)
	require.NoError(t, db.SetLocked(ctx, []chainhash.Hash{*other.TxIDChainHash()}, true))
	t.Cleanup(func() { _ = db.SetLocked(ctx, []chainhash.Hash{*other.TxIDChainHash()}, false) })

	winners := []chainhash.Hash{*winner.TxIDChainHash()}
	losers, _, err := utxo.ProcessConflicting(ctx, db, 3, chainhash.Hash{}, winners, nil, utxo.NoAncestryGuard)
	require.NoError(t, err)
	require.True(t, losers.Exists(*child.TxIDChainHash()))
	require.Equal(t, winner.TxIDChainHash(), crashSpender(t, ctx, db, parent))
	require.False(t, crashLocked(t, ctx, db, parent.TxIDChainHash()), "the operation must release the lock it took")
	require.True(t, crashLocked(t, ctx, db, other.TxIDChainHash()), "the operation must not release another holder's lock")
}
