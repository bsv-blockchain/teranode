package tests

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	utxo "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
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
				failedLosers, mayBeMarked, err := utxo.ProcessConflicting(ctx, wrapper, 3, chainhash.Hash{}, winners, nil, utxo.NoAncestryGuard)
				require.Error(t, err)
				require.True(t, wrapper.fired)
				require.Nil(t, failedLosers)
				// With or without a completed rollback, the caller must evict every transaction the run
				// may have flipped. The winners count once the run has tried to clear their flag.
				expectMarked := []chainhash.Hash{*loser.TxIDChainHash(), *child.TxIDChainHash()}
				if stage == "clear_winners" || stage == "publish_applied" {
					expectMarked = append(expectMarked, *winner.TxIDChainHash())
				}
				require.ElementsMatch(t, expectMarked, mayBeMarked)
				intent := utxo.ConflictIntent{Kind: utxo.ConflictIntentForward, BlockHeight: 3, TxHashes: winners}
				if crash {
					// Exercise the public recovery entrypoint separately so original state
					// can be checked before a subsequent forward attempt changes it again.
					_, err = utxo.RollbackConflictIntent(ctx, db, intent)
					require.NoError(t, err)
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
	_, err = utxo.RollbackConflictIntent(ctx, db, intent)
	require.Error(t, err)
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
	_, err = utxo.RollbackConflictIntent(ctx, db, intent)
	require.NoError(t, err)
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
		_, err = utxo.RollbackConflictIntent(ctx, db, intent, guard)
		require.Error(t, err)
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
	_, err = utxo.RollbackConflictIntent(ctx, db, intent, orphaned)
	require.NoError(t, err)
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
				_, err = utxo.RollbackConflictIntent(ctx, db, intent, guard)
				require.ErrorContains(t, err, "confirmed on the current chain")
				_, err = utxo.RollbackConflictIntent(ctx, db, intent)
				require.NoError(t, err)
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

// countConflictReads wraps a real store and records the largest number of GetSpend calls in flight
// at once. The short delay makes overlapping calls observable.
type countConflictReads struct {
	utxo.Store
	inFlight    atomic.Int32
	maxInFlight atomic.Int32
}

func (s *countConflictReads) GetSpend(ctx context.Context, spend *utxo.Spend) (*utxo.SpendResponse, error) {
	n := s.inFlight.Add(1)
	defer s.inFlight.Add(-1)

	for {
		m := s.maxInFlight.Load()
		if n <= m || s.maxInFlight.CompareAndSwap(m, n) {
			break
		}
	}

	time.Sleep(2 * time.Millisecond)

	return s.Store.GetSpend(ctx, spend)
}

// ConflictRecoveryWideCascade checks the checklist for a wide cascade. The snapshot reads run
// concurrently, bounded by the walk fan-out of 128, and the checklist keeps discovery order: the
// inputs of each recorded transaction in turn, then the winning inputs.
func ConflictRecoveryWideCascade(t *testing.T, db utxo.Store) {
	const width = 200

	ctx := t.Context()
	parent := crashParentTx(0xb7)
	_, _, err := db.SpendAndCreate(ctx, parent, 1, utxo.WithCreateOnly())
	require.NoError(t, err)

	loser := crashSpendTx(t, parent, 80000)
	loser.Outputs = nil

	for i := 0; i < width; i++ {
		loser.Outputs = append(loser.Outputs, &bt.Output{Satoshis: 300, LockingScript: bscript.NewFromBytes([]byte{0x52})})
	}

	_, _, err = db.SpendAndCreate(ctx, loser, 2)
	require.NoError(t, err)

	children := make([]*bt.Tx, width)
	for i := range children {
		child := bt.NewTx()
		require.NoError(t, child.From(loser.TxIDChainHash().String(), uint32(i), loser.Outputs[i].LockingScript.String(), loser.Outputs[i].Satoshis))
		child.Inputs[0].UnlockingScript = dummyUnlockingScript
		child.Outputs = []*bt.Output{{Satoshis: 200, LockingScript: bscript.NewFromBytes([]byte{0x52})}}

		_, _, err = db.SpendAndCreate(ctx, child, 2)
		require.NoError(t, err)

		children[i] = child
	}

	winner := crashSpendTx(t, parent, 70000)
	_, _, err = db.SpendAndCreate(ctx, winner, 2, utxo.WithCreateOnly(), utxo.WithConflicting(true))
	require.NoError(t, err)

	counted := &countConflictReads{Store: db}
	blockHash := chainhash.HashH([]byte("wide-cascade-block"))
	winners := []chainhash.Hash{*winner.TxIDChainHash()}

	losers, marked, err := utxo.ProcessConflicting(ctx, counted, 3, blockHash, winners, nil, utxo.NoAncestryGuard, utxo.WithDeferredConflictCompletion())
	require.NoError(t, err)

	require.Equal(t, width+1, losers.Length())
	require.Len(t, marked, width+1)
	require.True(t, losers.Exists(*loser.TxIDChainHash()))
	require.False(t, losers.Exists(*winner.TxIDChainHash()), "a winner must not be returned as a loser")

	for _, child := range children {
		require.True(t, losers.Exists(*child.TxIDChainHash()))
		require.True(t, crashConflicting(t, ctx, db, child.TxIDChainHash()))
	}

	require.Equal(t, winner.TxIDChainHash(), crashSpender(t, ctx, db, parent))
	require.False(t, crashConflicting(t, ctx, db, winner.TxIDChainHash()))
	require.True(t, crashConflicting(t, ctx, db, loser.TxIDChainHash()))

	maxInFlight := counted.maxInFlight.Load()
	require.Greater(t, maxInFlight, int32(1), "the snapshot reads must overlap")
	require.LessOrEqual(t, maxInFlight, int32(128), "the snapshot reads must stay within the walk fan-out")

	intent := utxo.ConflictIntent{Kind: utxo.ConflictIntentForward, BlockHeight: 3, BlockHash: blockHash, TxHashes: winners}
	r, err := db.LoadConflictRecovery(ctx, intent.IntentID())
	require.NoError(t, err)
	require.NotNil(t, r)
	require.Equal(t, utxo.ConflictRecoveryFinished, r.Phase)

	// Rebuild the expected order the way a sequential snapshot discovers it.
	type outpoint struct {
		hash  chainhash.Hash
		index uint32
	}

	var expected []outpoint

	seen := make(map[outpoint]struct{})
	addExpected := func(spends []*utxo.Spend) {
		for _, sp := range spends {
			key := outpoint{*sp.TxID, sp.Vout}
			if _, ok := seen[key]; !ok {
				seen[key] = struct{}{}
				expected = append(expected, key)
			}
		}
	}

	txs := map[chainhash.Hash]*bt.Tx{*loser.TxIDChainHash(): loser, *winner.TxIDChainHash(): winner}
	for _, child := range children {
		txs[*child.TxIDChainHash()] = child
	}

	require.Len(t, r.Transactions, width+2)

	for _, recorded := range r.Transactions {
		tx, ok := txs[recorded.Hash]
		require.True(t, ok, "the checklist must record only cascade transactions")

		spends, err := utxo.GetSpends(tx)
		require.NoError(t, err)

		addExpected(spends)
	}

	winningSpends, err := utxo.GetSpends(winner)
	require.NoError(t, err)

	addExpected(winningSpends)

	require.Len(t, r.OriginalSpends, width+1)

	for i, original := range r.OriginalSpends {
		require.Equal(t, expected[i], outpoint{*original.TxID, original.Vout}, "checklist entry %d", i)
		require.NotNil(t, original.SpendingData)

		// The before-image holds the owner from before the operation: the loser for the parent
		// output, and each child for its loser output.
		if original.TxID.IsEqual(parent.TxIDChainHash()) {
			require.Equal(t, loser.TxIDChainHash(), original.SpendingData.TxID)
		} else {
			require.Equal(t, children[original.Vout].TxIDChainHash(), original.SpendingData.TxID)
		}
	}

	require.NoError(t, utxo.CompleteConflictIntent(ctx, db, intent.IntentID()))
}

// attachLoserChild wraps a real store. Just before the operation starts to mutate, it stores a new
// child of the loser, as a validator can while the checklist is prepared.
type attachLoserChild struct {
	utxo.Store
	child *bt.Tx
	done  bool
}

func (s *attachLoserChild) SaveConflictRecovery(ctx context.Context, id chainhash.Hash, r *utxo.ConflictRecovery) error {
	if r.Phase == utxo.ConflictRecoveryMutating && !s.done {
		s.done = true
		if _, _, err := s.Store.SpendAndCreate(ctx, s.child, 2); err != nil {
			return err
		}
	}
	return s.Store.SaveConflictRecovery(ctx, id, r)
}

// ConflictRecoveryLateLoserChild attaches a child to the loser after the checklist is prepared. The
// operation must stop and roll back, and the rollback must not fail on that child: it never touches
// the child's inputs. A retry then includes the child.
func ConflictRecoveryLateLoserChild(t *testing.T, db utxo.Store) {
	ctx := t.Context()
	parent := crashParentTx(0xd1)
	_, _, err := db.SpendAndCreate(ctx, parent, 1, utxo.WithCreateOnly())
	require.NoError(t, err)
	loser := crashSpendTx(t, parent, 80000)
	_, _, err = db.SpendAndCreate(ctx, loser, 2)
	require.NoError(t, err)
	winner := crashSpendTx(t, parent, 70000)
	_, _, err = db.SpendAndCreate(ctx, winner, 2, utxo.WithCreateOnly(), utxo.WithConflicting(true))
	require.NoError(t, err)
	child := crashSpendTx(t, loser, 60000)

	winners := []chainhash.Hash{*winner.TxIDChainHash()}
	wrapper := &attachLoserChild{Store: db, child: child}
	_, _, err = utxo.ProcessConflicting(ctx, wrapper, 3, chainhash.Hash{}, winners, nil, utxo.NoAncestryGuard, utxo.WithDeferredConflictCompletion())
	require.ErrorContains(t, err, "retry required")
	require.NotContains(t, err.Error(), "MANUAL INTERVENTION", "the rollback must not fail on a child outside the checklist")

	intent := utxo.ConflictIntent{Kind: utxo.ConflictIntentForward, BlockHeight: 3, TxHashes: winners}
	r, err := db.LoadConflictRecovery(ctx, intent.IntentID())
	require.NoError(t, err)
	require.Equal(t, utxo.ConflictRecoveryRolledBack, r.Phase)
	require.Equal(t, loser.TxIDChainHash(), crashSpender(t, ctx, db, parent))
	require.False(t, crashConflicting(t, ctx, db, loser.TxIDChainHash()))
	require.False(t, crashConflicting(t, ctx, db, child.TxIDChainHash()), "the loser ends usable, so its child must stay usable")
	require.True(t, crashConflicting(t, ctx, db, winner.TxIDChainHash()))
	require.False(t, crashLocked(t, ctx, db, parent.TxIDChainHash()))

	losers, _, err := utxo.ProcessConflicting(ctx, db, 3, chainhash.Hash{}, winners, nil, utxo.NoAncestryGuard, utxo.WithDeferredConflictCompletion())
	require.NoError(t, err)
	require.True(t, losers.Exists(*child.TxIDChainHash()), "the retry must include the late child")
	require.Equal(t, winner.TxIDChainHash(), crashSpender(t, ctx, db, parent))
	require.NoError(t, db.CompleteConflictIntent(ctx, intent.IntentID()))
}

// ConflictRecoveryLateWinnerChild rolls back a finished forward operation after a mempool child
// attached to the winner. The winner ends conflicting, so the child must end conflicting too, and
// the rollback must report it for txmeta cache eviction.
func ConflictRecoveryLateWinnerChild(t *testing.T, db utxo.Store) {
	ctx := t.Context()
	parent := crashParentTx(0xd2)
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

	child := crashSpendTx(t, winner, 60000)
	_, _, err = db.SpendAndCreate(ctx, child, 3)
	require.NoError(t, err)
	grandchild := crashSpendTx(t, child, 50000)
	_, _, err = db.SpendAndCreate(ctx, grandchild, 3)
	require.NoError(t, err)

	intent := utxo.ConflictIntent{Kind: utxo.ConflictIntentForward, BlockHeight: 3, TxHashes: winners}
	mayBeMarked, err := utxo.RollbackConflictIntent(ctx, db, intent)
	require.NoError(t, err)
	require.ElementsMatch(t, []chainhash.Hash{*winner.TxIDChainHash(), *loser.TxIDChainHash(), *child.TxIDChainHash(), *grandchild.TxIDChainHash()}, mayBeMarked,
		"the rollback reports the checklist and the children it cascaded")

	r, err := db.LoadConflictRecovery(ctx, intent.IntentID())
	require.NoError(t, err)
	require.Equal(t, utxo.ConflictRecoveryRolledBack, r.Phase)
	require.Equal(t, loser.TxIDChainHash(), crashSpender(t, ctx, db, parent))
	require.False(t, crashConflicting(t, ctx, db, loser.TxIDChainHash()))
	require.True(t, crashConflicting(t, ctx, db, winner.TxIDChainHash()))
	require.True(t, crashConflicting(t, ctx, db, child.TxIDChainHash()), "a child of a conflicting winner must be conflicting")
	require.True(t, crashConflicting(t, ctx, db, grandchild.TxIDChainHash()))
	require.NoError(t, db.CompleteConflictIntent(ctx, intent.IntentID()))
}

// spendFreeInput wraps a real store. When the checklist snapshot first reads the free outpoint, it
// stores a stranger that spends it, as a validator can after discovery and before the snapshot.
type spendFreeInput struct {
	utxo.Store
	free     *utxo.Spend
	stranger *bt.Tx
	done     atomic.Bool
}

func (s *spendFreeInput) GetSpend(ctx context.Context, sp *utxo.Spend) (*utxo.SpendResponse, error) {
	if sp.TxID.IsEqual(s.free.TxID) && sp.Vout == s.free.Vout && s.done.CompareAndSwap(false, true) {
		if _, _, err := s.Store.SpendAndCreate(ctx, s.stranger, 2); err != nil {
			return nil, err
		}
	}
	return s.Store.GetSpend(ctx, sp)
}

// ConflictRecoveryLateFreeInputSpender spends a free input of the winner after discovery and before
// the snapshot. The stranger is outside the discovered set, so the operation must stop before it
// mutates anything. A retry then discovers the stranger and marks it conflicting.
func ConflictRecoveryLateFreeInputSpender(t *testing.T, db utxo.Store) {
	ctx := t.Context()
	parent := crashParentTx(0xf1)
	_, _, err := db.SpendAndCreate(ctx, parent, 1, utxo.WithCreateOnly())
	require.NoError(t, err)
	freeParent := crashParentTx(0xf2)
	_, _, err = db.SpendAndCreate(ctx, freeParent, 1, utxo.WithCreateOnly())
	require.NoError(t, err)
	loser := crashSpendTx(t, parent, 80000)
	_, _, err = db.SpendAndCreate(ctx, loser, 2)
	require.NoError(t, err)
	winner := crashSpendTx(t, parent, 70000)
	require.NoError(t, winner.From(freeParent.TxIDChainHash().String(), 0, freeParent.Outputs[0].LockingScript.String(), freeParent.Outputs[0].Satoshis))
	winner.Inputs[1].UnlockingScript = dummyUnlockingScript
	_, _, err = db.SpendAndCreate(ctx, winner, 2, utxo.WithCreateOnly(), utxo.WithConflicting(true))
	require.NoError(t, err)
	stranger := crashSpendTx(t, freeParent, 60000)
	winningSpends, err := utxo.GetSpends(winner)
	require.NoError(t, err)
	require.Len(t, winningSpends, 2)

	winners := []chainhash.Hash{*winner.TxIDChainHash()}
	wrapper := &spendFreeInput{Store: db, free: winningSpends[1], stranger: stranger}
	_, _, err = utxo.ProcessConflicting(ctx, wrapper, 3, chainhash.Hash{}, winners, nil, utxo.NoAncestryGuard, utxo.WithDeferredConflictCompletion())
	require.True(t, wrapper.done.Load(), "the snapshot must read the free input")
	require.ErrorContains(t, err, "retry required")
	require.Equal(t, loser.TxIDChainHash(), crashSpender(t, ctx, db, parent), "nothing may change before the retry")
	require.Equal(t, stranger.TxIDChainHash(), crashSpender(t, ctx, db, freeParent))
	require.False(t, crashConflicting(t, ctx, db, stranger.TxIDChainHash()))
	require.False(t, crashLocked(t, ctx, db, parent.TxIDChainHash()))
	require.False(t, crashLocked(t, ctx, db, freeParent.TxIDChainHash()))

	losers, _, err := utxo.ProcessConflicting(ctx, db, 3, chainhash.Hash{}, winners, nil, utxo.NoAncestryGuard, utxo.WithDeferredConflictCompletion())
	require.NoError(t, err)
	require.True(t, losers.Exists(*loser.TxIDChainHash()))
	require.True(t, losers.Exists(*stranger.TxIDChainHash()), "the retry must include the stranger")
	require.True(t, crashConflicting(t, ctx, db, stranger.TxIDChainHash()), "a displaced spender must end conflicting")
	require.False(t, crashConflicting(t, ctx, db, winner.TxIDChainHash()))
	require.Equal(t, winner.TxIDChainHash(), crashSpender(t, ctx, db, parent))
	require.Equal(t, winner.TxIDChainHash(), crashSpender(t, ctx, db, freeParent))
	intent := utxo.ConflictIntent{Kind: utxo.ConflictIntentForward, BlockHeight: 3, TxHashes: winners}
	require.NoError(t, db.CompleteConflictIntent(ctx, intent.IntentID()))
}

// failBeforeMutation wraps a real store and fails one step that runs before the first flag change.
type failBeforeMutation struct {
	utxo.Store
	stage string
}

func (s *failBeforeMutation) SetConflictLocks(ctx context.Context, id chainhash.Hash, parents []chainhash.Hash, value bool) error {
	if value && s.stage == "lock" {
		return errors.NewStorageError("injected lock failure")
	}
	return s.Store.SetConflictLocks(ctx, id, parents, value)
}

func (s *failBeforeMutation) SaveConflictRecovery(ctx context.Context, id chainhash.Hash, r *utxo.ConflictRecovery) error {
	if r.Phase == utxo.ConflictRecoveryMutating && s.stage == "save_mutating" {
		return errors.NewStorageError("injected save failure")
	}
	return s.Store.SaveConflictRecovery(ctx, id, r)
}

// ConflictRecoveryEarlyFailure fails the operation before it changes a flag. Nothing changed, so
// the caller must get nothing to evict: an eviction leaves txmeta tombstones for the whole cascade.
func ConflictRecoveryEarlyFailure(t *testing.T, db utxo.Store) {
	ctx := t.Context()
	for index, stage := range []string{"lock", "save_mutating"} {
		t.Run(stage, func(t *testing.T) {
			parent := crashParentTx(0xb1 + byte(index))
			_, _, err := db.SpendAndCreate(ctx, parent, 1, utxo.WithCreateOnly())
			require.NoError(t, err)
			loser := crashSpendTx(t, parent, 80000)
			_, _, err = db.SpendAndCreate(ctx, loser, 2)
			require.NoError(t, err)
			child := crashSpendTx(t, loser, 60000)
			_, _, err = db.SpendAndCreate(ctx, child, 2)
			require.NoError(t, err)
			winner := crashSpendTx(t, parent, 70000)
			_, _, err = db.SpendAndCreate(ctx, winner, 2, utxo.WithCreateOnly(), utxo.WithConflicting(true))
			require.NoError(t, err)

			winners := []chainhash.Hash{*winner.TxIDChainHash()}
			losers, marked, err := utxo.ProcessConflicting(ctx, &failBeforeMutation{Store: db, stage: stage}, 3, chainhash.Hash{}, winners, nil, utxo.NoAncestryGuard, utxo.WithDeferredConflictCompletion())
			require.ErrorContains(t, err, "injected")
			require.Nil(t, losers)
			require.Empty(t, marked, "nothing changed, so nothing may be evicted")
			require.False(t, crashConflicting(t, ctx, db, loser.TxIDChainHash()))
			require.False(t, crashConflicting(t, ctx, db, child.TxIDChainHash()))
			require.Equal(t, loser.TxIDChainHash(), crashSpender(t, ctx, db, parent))
			require.False(t, crashLocked(t, ctx, db, parent.TxIDChainHash()))

			losers, _, err = utxo.ProcessConflicting(ctx, db, 3, chainhash.Hash{}, winners, nil, utxo.NoAncestryGuard, utxo.WithDeferredConflictCompletion())
			require.NoError(t, err)
			require.True(t, losers.Exists(*child.TxIDChainHash()))
			intent := utxo.ConflictIntent{Kind: utxo.ConflictIntentForward, BlockHeight: 3, TxHashes: winners}
			require.NoError(t, db.CompleteConflictIntent(ctx, intent.IntentID()))
		})
	}
}

// ConflictRecoveryRefusedAfterRollback leaves an applied checklist that a crash stopped before its
// rollback, and a child that then attached to the applied winner. The next run rolls the checklist
// back, which flags the winner and cascades to the child. marked is that run's loser set, so the run
// must hand the rolled-back transactions to the evictor, whether the guard refuses the new demotion
// or the run succeeds. The child stays Conflicting in both cases.
func ConflictRecoveryRefusedAfterRollback(t *testing.T, db utxo.Store) {
	for index, refused := range []bool{true, false} {
		t.Run(map[bool]string{true: "refused", false: "succeeds"}[refused], func(t *testing.T) {
			ctx := t.Context()
			parent := crashParentTx(0xc1 + byte(index))
			_, _, err := db.SpendAndCreate(ctx, parent, 1, utxo.WithCreateOnly())
			require.NoError(t, err)
			loser := crashSpendTx(t, parent, 80000)
			_, _, err = db.SpendAndCreate(ctx, loser, 2)
			require.NoError(t, err)
			winner := crashSpendTx(t, parent, 70000)
			_, _, err = db.SpendAndCreate(ctx, winner, 2, utxo.WithCreateOnly(), utxo.WithConflicting(true))
			require.NoError(t, err)

			// This loser was already flagged before the failed run, so rollback must carry
			// its still-pending result even when the ancestry guard refuses promotion.
			preflagged := crashSpendTx(t, loser, 50000)
			_, _, err = db.SpendAndCreate(ctx, preflagged, 2)
			require.NoError(t, err)
			_, _, err = db.SetConflicting(ctx, []chainhash.Hash{*preflagged.TxIDChainHash()}, true)
			require.NoError(t, err)

			winners := []chainhash.Hash{*winner.TxIDChainHash()}
			crashed := &failConflictPhase{Store: db, stage: "publish_applied", crash: true}
			_, _, err = utxo.ProcessConflicting(ctx, crashed, 3, chainhash.Hash{}, winners, nil, utxo.NoAncestryGuard, utxo.WithDeferredConflictCompletion())
			require.Error(t, err)
			require.False(t, crashConflicting(t, ctx, db, winner.TxIDChainHash()), "the crash must leave the winner applied")

			child := crashSpendTx(t, winner, 60000)
			_, _, err = db.SpendAndCreate(ctx, child, 3)
			require.NoError(t, err)

			guard := utxo.NoAncestryGuard
			if refused {
				_, err = db.SetMinedMulti(ctx, []*chainhash.Hash{loser.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: 50, BlockHeight: 2})
				require.NoError(t, err)
				guard = utxo.NewAncestryGuard(func(context.Context, []uint32, *chainhash.Hash) (bool, error) { return true, nil })
			}

			var evicted []chainhash.Hash
			losers, marked, err := utxo.ProcessConflicting(ctx, db, 3, chainhash.Hash{}, winners, nil, guard, utxo.WithDeferredConflictCompletion(),
				utxo.WithConflictEvictor(func(hashes []chainhash.Hash) { evicted = append(evicted, hashes...) }))
			require.NoError(t, err)
			require.True(t, crashConflicting(t, ctx, db, child.TxIDChainHash()), "the rollback cascades to the late child")
			require.Contains(t, evicted, *child.TxIDChainHash(), "the cascaded child must leave the txmeta cache")
			require.Contains(t, evicted, *winner.TxIDChainHash(), "the rollback flagged the winner")

			if refused {
				require.True(t, losers.Exists(*preflagged.TxIDChainHash()), "the pre-existing pending loser is still returned")
				require.ElementsMatch(t, []chainhash.Hash{*preflagged.TxIDChainHash()}, marked)
				// Startup passes the WAL's original input. Refusal must keep those identities
				// so an ordinary replay is not mistaken for adding winners.
				replayed, _, err := utxo.ProcessConflicting(ctx, db, 3, chainhash.Hash{}, winners, nil, guard, utxo.WithDeferredConflictCompletion())
				require.NoError(t, err)
				require.True(t, replayed.Exists(*preflagged.TxIDChainHash()))
				require.True(t, crashConflicting(t, ctx, db, winner.TxIDChainHash()))
				require.Equal(t, loser.TxIDChainHash(), crashSpender(t, ctx, db, parent))
				require.NoError(t, utxo.CompleteConflictIntentsForBlock(ctx, db, chainhash.Hash{}))
				return
			}

			require.True(t, losers.Exists(*loser.TxIDChainHash()))
			require.NotContains(t, marked, *child.TxIDChainHash(), "the child is not a loser of this run")
			require.False(t, crashConflicting(t, ctx, db, winner.TxIDChainHash()))
			require.Equal(t, winner.TxIDChainHash(), crashSpender(t, ctx, db, parent))
			intent := utxo.ConflictIntent{Kind: utxo.ConflictIntentForward, BlockHeight: 3, TxHashes: winners}
			require.NoError(t, db.CompleteConflictIntent(ctx, intent.IntentID()))
		})
	}
}

// failFinalUnlock wraps a real store and fails the unlock that moves a checklist from applied to
// finished, as a crash at that point does. The parents stay locked.
type failFinalUnlock struct {
	utxo.Store
}

func (s *failFinalUnlock) SetConflictLocks(ctx context.Context, id chainhash.Hash, parents []chainhash.Hash, value bool) error {
	if !value {
		return errors.NewStorageError("injected final unlock failure")
	}
	return s.Store.SetConflictLocks(ctx, id, parents, value)
}

// ConflictRecoveryHealStaleForward heals a forward intent whose block left the current chain. The
// operation has two winners, W1 and W2, each with its own loser and parent. Block 77 is on the
// current chain. notReached models a crash before block assembly committed the block, so no moveBack
// of it follows.
func ConflictRecoveryHealStaleForward(t *testing.T, db utxo.Store) {
	const currentBlockID = 77

	cases := []struct {
		name         string
		applied      bool
		reverse      bool
		minedW1      bool
		minedW2      bool
		notReached   bool
		w1LostInput  bool
		prunedParent bool
		guardErr     bool
		wantRetired  bool
		wantRollback bool
	}{
		{name: "finished/all confirmed", minedW1: true, minedW2: true, wantRetired: true},
		{name: "applied/all confirmed", applied: true, minedW1: true, minedW2: true, wantRetired: true},
		{name: "finished/all confirmed/not reached", minedW1: true, minedW2: true, notReached: true, wantRetired: true},
		{name: "finished/partial re-mine", minedW1: true, wantRetired: true},
		{name: "applied/partial re-mine", applied: true, minedW1: true, wantRetired: true},
		{name: "finished/partial re-mine/not reached", minedW1: true, notReached: true, wantRetired: true},
		{name: "applied/partial re-mine/not reached", applied: true, minedW1: true, notReached: true, wantRetired: true},
		{name: "finished/reversed", reverse: true, wantRetired: true},
		{name: "finished/reversed/parent pruned", reverse: true, prunedParent: true, wantRetired: true},
		{name: "finished/none confirmed", wantRollback: true},
		{name: "finished/confirmed winner lost an input", minedW1: true, w1LostInput: true},
		{name: "finished/guard error", minedW1: true, guardErr: true},
	}

	for index, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			seed := 0x60 + byte(index)*4
			parents := []*bt.Tx{crashParentTx(seed), crashParentTx(seed + 1)}
			var losers, winnerTxs []*bt.Tx
			for _, parent := range parents {
				_, _, err := db.SpendAndCreate(ctx, parent, 1, utxo.WithCreateOnly())
				require.NoError(t, err)
				loser := crashSpendTx(t, parent, 80000)
				_, _, err = db.SpendAndCreate(ctx, loser, 2)
				require.NoError(t, err)
				winner := crashSpendTx(t, parent, 70000)
				_, _, err = db.SpendAndCreate(ctx, winner, 2, utxo.WithCreateOnly(), utxo.WithConflicting(true))
				require.NoError(t, err)
				losers = append(losers, loser)
				winnerTxs = append(winnerTxs, winner)
			}

			block := chainhash.HashH([]byte("heal-stale-forward-" + tc.name))
			winners := []chainhash.Hash{*winnerTxs[0].TxIDChainHash(), *winnerTxs[1].TxIDChainHash()}
			intent := utxo.ConflictIntent{Kind: utxo.ConflictIntentForward, BlockHeight: 3, BlockHash: block, TxHashes: winners}

			var run utxo.Store = db
			if tc.applied {
				run = &failFinalUnlock{Store: db}
			}
			_, _, err := utxo.ProcessConflicting(ctx, run, 3, block, winners, nil, utxo.NoAncestryGuard, utxo.WithDeferredConflictCompletion())
			if tc.applied {
				require.ErrorContains(t, err, "injected final unlock failure")
			} else {
				require.NoError(t, err)
			}

			r, err := db.LoadConflictRecovery(ctx, intent.IntentID())
			require.NoError(t, err)
			if tc.applied {
				require.Equal(t, utxo.ConflictRecoveryApplied, r.Phase)
			} else {
				require.Equal(t, utxo.ConflictRecoveryFinished, r.Phase)
			}

			if tc.reverse {
				_, _, err = utxo.ReverseProcessConflicting(ctx, db, 3, block, winners)
				require.NoError(t, err)
			}

			// The restored loser was confirmed and its parent, fully spent, was pruned. The heal must
			// still see the reversed winner and retire the checklist instead of refusing to start.
			if tc.prunedParent {
				_, err = db.SetMinedMulti(ctx, []*chainhash.Hash{losers[0].TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: currentBlockID, BlockHeight: 4})
				require.NoError(t, err)
				require.NoError(t, db.Delete(ctx, parents[0].TxIDChainHash()))
			}

			mined := []bool{tc.minedW1, tc.minedW2}
			for i := range mined {
				if mined[i] {
					_, err = db.SetMinedMulti(ctx, []*chainhash.Hash{winnerTxs[i].TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: currentBlockID, BlockHeight: 4})
					require.NoError(t, err)
				}
			}

			if tc.w1LostInput {
				w1Spends, err := utxo.GetSpends(winnerTxs[0])
				require.NoError(t, err)
				require.NoError(t, db.Unspend(ctx, w1Spends))
			}

			guard := utxo.NewCurrentChainGuard(func(_ context.Context, ids []uint32) (bool, error) {
				if tc.guardErr {
					return false, errors.NewServiceError("injected chain lookup failure")
				}
				for _, id := range ids {
					if id == currentBlockID {
						return true, nil
					}
				}
				return false, nil
			})

			mayBeMarked, err := utxo.HealStaleForwardIntent(ctx, db, intent, guard, func(context.Context) (bool, error) {
				return !tc.notReached, nil
			})

			r, loadErr := db.LoadConflictRecovery(ctx, intent.IntentID())
			require.NoError(t, loadErr)

			switch {
			case tc.guardErr:
				require.ErrorContains(t, err, "injected chain lookup failure")
				require.Empty(t, mayBeMarked, "nothing changed")
				require.NotNil(t, r, "the intent stays for the next attempt")
				require.False(t, crashConflicting(t, ctx, db, winnerTxs[0].TxIDChainHash()))
			case tc.w1LostInput:
				require.ErrorContains(t, err, "confirmed conflict winner no longer owns its inputs")
				require.Empty(t, mayBeMarked, "nothing changed")
				require.NotNil(t, r, "the checklist stays for repair")
				require.False(t, crashConflicting(t, ctx, db, winnerTxs[1].TxIDChainHash()))
				require.Equal(t, winnerTxs[1].TxIDChainHash(), crashSpender(t, ctx, db, parents[1]))
			case tc.wantRetired:
				require.NoError(t, err)
				require.Nil(t, r, "the intent is retired")
				var wantMarked []chainhash.Hash
				for i := range winnerTxs {
					// Without a moveBack to come, the heal reverses a winner that the chain does not confirm.
					reversed := tc.reverse || (tc.notReached && !mined[i])
					if reversed && !tc.reverse {
						wantMarked = append(wantMarked, winners[i])
					}
					require.Equal(t, reversed, crashConflicting(t, ctx, db, winnerTxs[i].TxIDChainHash()), "winner %d", i)
					require.Equal(t, reversed, !crashConflicting(t, ctx, db, losers[i].TxIDChainHash()), "loser %d", i)
					if tc.prunedParent && i == 0 {
						continue
					}
					want := winnerTxs[i].TxIDChainHash()
					if reversed {
						want = losers[i].TxIDChainHash()
					}
					require.Equal(t, want, crashSpender(t, ctx, db, parents[i]), "owner of parent %d", i)
				}
				require.ElementsMatch(t, wantMarked, mayBeMarked, "only the reverse changes a flag")
				pending, err := db.PendingConflictIntents(ctx)
				require.NoError(t, err)
				for _, p := range pending {
					require.NotEqual(t, block, p.BlockHash, "the reverse completes its own intent")
				}
			case tc.wantRollback:
				require.NoError(t, err)
				require.ElementsMatch(t, append(append([]chainhash.Hash(nil), winners...), *losers[0].TxIDChainHash(), *losers[1].TxIDChainHash()), mayBeMarked)
				require.Equal(t, utxo.ConflictRecoveryRolledBack, r.Phase)
				for i := range winnerTxs {
					require.True(t, crashConflicting(t, ctx, db, winnerTxs[i].TxIDChainHash()))
					require.False(t, crashConflicting(t, ctx, db, losers[i].TxIDChainHash()))
					require.Equal(t, losers[i].TxIDChainHash(), crashSpender(t, ctx, db, parents[i]))
				}
			}

			if !tc.guardErr && !tc.w1LostInput {
				for i, parent := range parents {
					if tc.prunedParent && i == 0 {
						continue
					}
					require.False(t, crashLocked(t, ctx, db, parent.TxIDChainHash()), "parent %d must not stay locked", i)
				}
			}

			if r != nil {
				require.NoError(t, db.CompleteConflictIntent(ctx, intent.IntentID()))
			}
		})
	}
}

// ConflictRecoveryOutsiderOwnedInput runs an operation whose cascade reaches a conflicting child C
// that also spends Q:0. Q:0 belongs to Z, which is outside the cascade. The operation must succeed
// and must not touch Q:0: Z keeps its input and stays usable.
func ConflictRecoveryOutsiderOwnedInput(t *testing.T, db utxo.Store) {
	ctx := t.Context()
	parent := crashParentTx(0xa1)
	_, _, err := db.SpendAndCreate(ctx, parent, 1, utxo.WithCreateOnly())
	require.NoError(t, err)
	q := crashParentTx(0xa2)
	_, _, err = db.SpendAndCreate(ctx, q, 1, utxo.WithCreateOnly())
	require.NoError(t, err)
	loser := crashSpendTx(t, parent, 80000)
	_, _, err = db.SpendAndCreate(ctx, loser, 2)
	require.NoError(t, err)
	z := crashSpendTx(t, q, 50000)
	_, _, err = db.SpendAndCreate(ctx, z, 2)
	require.NoError(t, err)
	c := crashSpendTx(t, loser, 40000)
	require.NoError(t, c.From(q.TxIDChainHash().String(), 0, q.Outputs[0].LockingScript.String(), q.Outputs[0].Satoshis))
	c.Inputs[1].UnlockingScript = dummyUnlockingScript
	_, _, err = db.SpendAndCreate(ctx, c, 2, utxo.WithCreateOnly(), utxo.WithConflicting(true))
	require.NoError(t, err)
	winner := crashSpendTx(t, parent, 70000)
	_, _, err = db.SpendAndCreate(ctx, winner, 2, utxo.WithCreateOnly(), utxo.WithConflicting(true))
	require.NoError(t, err)

	winners := []chainhash.Hash{*winner.TxIDChainHash()}
	losers, _, err := utxo.ProcessConflicting(ctx, db, 3, chainhash.Hash{}, winners, nil, utxo.NoAncestryGuard, utxo.WithDeferredConflictCompletion())
	require.NoError(t, err)
	require.True(t, losers.Exists(*loser.TxIDChainHash()))
	require.True(t, losers.Exists(*c.TxIDChainHash()), "the cascade must reach the conflicting child")
	require.False(t, losers.Exists(*z.TxIDChainHash()))
	require.Equal(t, winner.TxIDChainHash(), crashSpender(t, ctx, db, parent))
	require.Equal(t, z.TxIDChainHash(), crashSpender(t, ctx, db, q), "Z must keep its input")
	require.False(t, crashConflicting(t, ctx, db, z.TxIDChainHash()))
	require.True(t, crashConflicting(t, ctx, db, c.TxIDChainHash()))
	require.False(t, crashLocked(t, ctx, db, q.TxIDChainHash()), "the operation must not leave the outsider's parent locked")

	intent := utxo.ConflictIntent{Kind: utxo.ConflictIntentForward, BlockHeight: 3, TxHashes: winners}
	r, err := db.LoadConflictRecovery(ctx, intent.IntentID())
	require.NoError(t, err)
	for _, sp := range r.OriginalSpends {
		require.False(t, sp.TxID.IsEqual(q.TxIDChainHash()), "the checklist must not record the outsider's spend")
	}
	require.NoError(t, db.CompleteConflictIntent(ctx, intent.IntentID()))
}

// ConflictRecoveryReverseThenReadd adds a block, moves it back and adds it again, while the forward
// intent of each add still waits for acknowledgement. The reverse leaves the finished checklist to its
// caller. In the first round the caller does not retire it, as after a crash, so the next add must
// retire it because its winners are Conflicting again. In the second round the caller retires it.
func ConflictRecoveryReverseThenReadd(t *testing.T, db utxo.Store) {
	ctx := t.Context()
	parent := crashParentTx(0xd3)
	_, _, err := db.SpendAndCreate(ctx, parent, 1, utxo.WithCreateOnly())
	require.NoError(t, err)
	loser := crashSpendTx(t, parent, 80000)
	_, _, err = db.SpendAndCreate(ctx, loser, 2)
	require.NoError(t, err)
	winner := crashSpendTx(t, parent, 70000)
	_, _, err = db.SpendAndCreate(ctx, winner, 2, utxo.WithCreateOnly(), utxo.WithConflicting(true))
	require.NoError(t, err)

	block := chainhash.HashH([]byte("reverse-then-readd-block"))
	winners := []chainhash.Hash{*winner.TxIDChainHash()}
	forward := utxo.ConflictIntent{Kind: utxo.ConflictIntentForward, BlockHeight: 3, BlockHash: block, TxHashes: winners}

	for round := 0; round < 2; round++ {
		_, _, err = utxo.ProcessConflicting(ctx, db, 3, block, winners, nil, utxo.NoAncestryGuard, utxo.WithDeferredConflictCompletion())
		require.NoError(t, err, "add %d", round+1)
		require.Equal(t, winner.TxIDChainHash(), crashSpender(t, ctx, db, parent))

		_, _, err = utxo.ReverseProcessConflicting(ctx, db, 3, block, winners)
		require.NoError(t, err, "move back %d", round+1)
		require.Equal(t, loser.TxIDChainHash(), crashSpender(t, ctx, db, parent))
		require.True(t, crashConflicting(t, ctx, db, winner.TxIDChainHash()))

		r, err := db.LoadConflictRecovery(ctx, forward.IntentID())
		require.NoError(t, err)
		require.NotNil(t, r, "the reverse leaves the forward checklist to its caller")

		if round == 0 {
			// Model a crash before the caller retired the checklist: the next add must retire the
			// stale checklist itself, because its winners are Conflicting again.
			continue
		}

		require.NoError(t, utxo.RetireConflictIntentsForBlocks(ctx, db, []chainhash.Hash{block}))
		r, err = db.LoadConflictRecovery(ctx, forward.IntentID())
		require.NoError(t, err)
		require.Nil(t, r, "the caller retires the finished forward checklist")
	}

	_, _, err = utxo.ProcessConflicting(ctx, db, 3, block, winners, nil, utxo.NoAncestryGuard)
	require.NoError(t, err, "final add")
	require.Equal(t, winner.TxIDChainHash(), crashSpender(t, ctx, db, parent))
}

// countPendingScans wraps a real store and counts the scans of the whole WAL.
type countPendingScans struct {
	utxo.Store
	scans atomic.Int32
}

func (s *countPendingScans) PendingConflictIntents(ctx context.Context) ([]utxo.ConflictIntent, error) {
	s.scans.Add(1)
	return s.Store.PendingConflictIntents(ctx)
}

// ConflictRecoveryLegacyIntent checks that a new operation finds an existing intent without a scan
// of the whole WAL, and that an intent recorded without a checklist is still refused.
func ConflictRecoveryLegacyIntent(t *testing.T, db utxo.Store) {
	ctx := t.Context()
	parent := crashParentTx(0xd4)
	_, _, err := db.SpendAndCreate(ctx, parent, 1, utxo.WithCreateOnly())
	require.NoError(t, err)
	loser := crashSpendTx(t, parent, 80000)
	_, _, err = db.SpendAndCreate(ctx, loser, 2)
	require.NoError(t, err)
	winner := crashSpendTx(t, parent, 70000)
	_, _, err = db.SpendAndCreate(ctx, winner, 2, utxo.WithCreateOnly(), utxo.WithConflicting(true))
	require.NoError(t, err)
	winners := []chainhash.Hash{*winner.TxIDChainHash()}

	legacy := utxo.ConflictIntent{Kind: utxo.ConflictIntentForward, BlockHeight: 3, BlockHash: chainhash.HashH([]byte("legacy-block")), TxHashes: winners, StartedAt: 1}
	require.NoError(t, db.BeginConflictIntent(ctx, legacy))

	counter := &countPendingScans{Store: db}
	_, _, err = utxo.ProcessConflicting(ctx, counter, 3, legacy.BlockHash, winners, nil, utxo.NoAncestryGuard)
	require.ErrorContains(t, err, "legacy conflict intent")
	require.Equal(t, loser.TxIDChainHash(), crashSpender(t, ctx, db, parent), "a refused legacy intent must change nothing")
	require.NoError(t, db.CompleteConflictIntent(ctx, legacy.IntentID()))

	_, _, err = utxo.ProcessConflicting(ctx, counter, 3, chainhash.HashH([]byte("new-block")), winners, nil, utxo.NoAncestryGuard)
	require.NoError(t, err)
	require.Equal(t, winner.TxIDChainHash(), crashSpender(t, ctx, db, parent))
	require.Zero(t, counter.scans.Load(), "an operation must not scan the whole WAL")
}

// cancelDuringRestore wraps a real store. Its restore cancels the rollback's ctx and writes nothing,
// as when compensationTimeout expires during the restore.
type cancelDuringRestore struct {
	utxo.Store
	cancel context.CancelFunc
}

func (s *cancelDuringRestore) RestoreConflictSpends(context.Context, []*utxo.Spend, uint32) error {
	s.cancel()
	return nil
}

// The writes after the restore ignore the cancellation, so only the verify reads see it.
func (s *cancelDuringRestore) SetConflicting(ctx context.Context, hashes []chainhash.Hash, value bool) ([]*utxo.Spend, []chainhash.Hash, error) {
	return s.Store.SetConflicting(context.WithoutCancel(ctx), hashes, value)
}

func (s *cancelDuringRestore) SetConflictLocks(ctx context.Context, id chainhash.Hash, parents []chainhash.Hash, locked bool) error {
	return s.Store.SetConflictLocks(context.WithoutCancel(ctx), id, parents, locked)
}

func (s *cancelDuringRestore) SaveConflictRecovery(ctx context.Context, id chainhash.Hash, r *utxo.ConflictRecovery) error {
	return s.Store.SaveConflictRecovery(context.WithoutCancel(ctx), id, r)
}

// ConflictRecoveryCancelledVerify cancels the rollback before it verifies the restored owners. An
// outpoint that was never read is not verified, so the rollback must fail and keep the losers
// conflicting instead of marking them usable.
func ConflictRecoveryCancelledVerify(t *testing.T, db utxo.Store) {
	ctx := t.Context()
	parent := crashParentTx(0xd6)
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

	rollbackCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	intent := utxo.ConflictIntent{Kind: utxo.ConflictIntentForward, BlockHeight: 3, TxHashes: winners}
	_, err = utxo.RollbackConflictIntent(rollbackCtx, &cancelDuringRestore{Store: db, cancel: cancel}, intent)
	require.Error(t, err, "unverified owners must fail the rollback")

	r, err := db.LoadConflictRecovery(ctx, intent.IntentID())
	require.NoError(t, err)
	require.NotEqual(t, utxo.ConflictRecoveryRolledBack, r.Phase)
	require.True(t, crashConflicting(t, ctx, db, loser.TxIDChainHash()), "the loser must stay conflicting")

	_, err = utxo.RollbackConflictIntent(ctx, db, intent)
	require.NoError(t, err)
	require.Equal(t, loser.TxIDChainHash(), crashSpender(t, ctx, db, parent))
	require.NoError(t, db.CompleteConflictIntent(ctx, intent.IntentID()))
}

// ConflictRecoverySetConflictingChildren pins what SetConflicting reports as children. Both backends
// read the spender of every output with GetSpend before they write the flag. SQL's GetSpend reports
// an output of a Conflicting transaction as CONFLICTING, not SPENT, so SQL reports the children only
// while the transaction is not yet Conflicting. Aerospike reports them on every call. The rollback
// cascade relies on its first mark, which runs while the applied winners are not Conflicting.
func ConflictRecoverySetConflictingChildren(t *testing.T, db utxo.Store, reportsWhileConflicting bool) {
	ctx := t.Context()
	grandparent := crashParentTx(0x91)
	_, _, err := db.SpendAndCreate(ctx, grandparent, 1, utxo.WithCreateOnly())
	require.NoError(t, err)
	parent := crashSpendTx(t, grandparent, 80000)
	_, _, err = db.SpendAndCreate(ctx, parent, 2)
	require.NoError(t, err)
	child := crashSpendTx(t, parent, 60000)
	_, _, err = db.SpendAndCreate(ctx, child, 2)
	require.NoError(t, err)

	want := []chainhash.Hash{*child.TxIDChainHash()}
	_, children, err := db.SetConflicting(ctx, []chainhash.Hash{*parent.TxIDChainHash()}, true)
	require.NoError(t, err)
	require.Equal(t, want, children, "the first mark reports the child")

	whileConflicting := []chainhash.Hash{}
	if reportsWhileConflicting {
		whileConflicting = want
	}
	_, children, err = db.SetConflicting(ctx, []chainhash.Hash{*parent.TxIDChainHash()}, true)
	require.NoError(t, err)
	require.ElementsMatch(t, whileConflicting, children, "a second mark of a Conflicting parent")
	_, children, err = db.SetConflicting(ctx, []chainhash.Hash{*parent.TxIDChainHash()}, false)
	require.NoError(t, err)
	require.ElementsMatch(t, whileConflicting, children, "a clear of a Conflicting parent")
}

// failCompletion wraps a real store and fails every CompleteConflictIntent.
type failCompletion struct {
	utxo.Store
}

func (s *failCompletion) CompleteConflictIntent(context.Context, chainhash.Hash) error {
	return errors.NewStorageError("injected WAL completion failure")
}

// ConflictRecoveryReverseCompletionFailure fails the completion of a reverse intent after the reverse
// has run. The call must return error-shaped results: no allTouched, and the demoted winner for
// txmeta cache eviction.
func ConflictRecoveryReverseCompletionFailure(t *testing.T, db utxo.Store) {
	ctx := t.Context()
	parent := crashParentTx(0x95)
	_, _, err := db.SpendAndCreate(ctx, parent, 1, utxo.WithCreateOnly())
	require.NoError(t, err)
	loser := crashSpendTx(t, parent, 80000)
	_, _, err = db.SpendAndCreate(ctx, loser, 2)
	require.NoError(t, err)
	winner := crashSpendTx(t, parent, 70000)
	_, _, err = db.SpendAndCreate(ctx, winner, 2, utxo.WithCreateOnly(), utxo.WithConflicting(true))
	require.NoError(t, err)

	block := chainhash.HashH([]byte("reverse-completion-failure"))
	winners := []chainhash.Hash{*winner.TxIDChainHash()}
	_, _, err = utxo.ProcessConflicting(ctx, db, 3, block, winners, nil, utxo.NoAncestryGuard)
	require.NoError(t, err)

	cascaded, allTouched, err := utxo.ReverseProcessConflicting(ctx, &failCompletion{Store: db}, 3, block, winners)
	require.ErrorContains(t, err, "injected WAL completion failure")
	require.Nil(t, allTouched, "an error must not come with a success-shaped allTouched")
	require.Contains(t, cascaded, *winner.TxIDChainHash(), "the demoted winner must be returned for eviction")
	require.True(t, crashConflicting(t, ctx, db, winner.TxIDChainHash()))

	reverse := utxo.ConflictIntent{Kind: utxo.ConflictIntentReverse, BlockHeight: 3, BlockHash: block, TxHashes: winners}
	require.NoError(t, db.CompleteConflictIntent(ctx, reverse.IntentID()))
}

// ConflictRecoveryWinnerWithConflictingChild adds a block, lets a mempool child spend its applied
// winner, moves the block back and adds it again. The reverse demotes the winner and the child, and
// the child keeps its spend of the winner's output. The child spends the winner, not a cascade member,
// so the re-add must succeed and leave the child Conflicting. Aerospike reports that child on every
// mark, so a check over the winners' children refused every retry.
func ConflictRecoveryWinnerWithConflictingChild(t *testing.T, db utxo.Store) {
	ctx := t.Context()
	parent := crashParentTx(0x97)
	_, _, err := db.SpendAndCreate(ctx, parent, 1, utxo.WithCreateOnly())
	require.NoError(t, err)
	loser := crashSpendTx(t, parent, 80000)
	_, _, err = db.SpendAndCreate(ctx, loser, 2)
	require.NoError(t, err)
	winner := crashSpendTx(t, parent, 70000)
	_, _, err = db.SpendAndCreate(ctx, winner, 2, utxo.WithCreateOnly(), utxo.WithConflicting(true))
	require.NoError(t, err)

	block := chainhash.HashH([]byte("winner-with-conflicting-child"))
	winners := []chainhash.Hash{*winner.TxIDChainHash()}
	_, _, err = utxo.ProcessConflicting(ctx, db, 3, block, winners, nil, utxo.NoAncestryGuard)
	require.NoError(t, err)

	child := crashSpendTx(t, winner, 60000)
	_, _, err = db.SpendAndCreate(ctx, child, 3)
	require.NoError(t, err)

	_, _, err = utxo.ReverseProcessConflicting(ctx, db, 3, block, winners)
	require.NoError(t, err)
	require.True(t, crashConflicting(t, ctx, db, winner.TxIDChainHash()))
	require.True(t, crashConflicting(t, ctx, db, child.TxIDChainHash()))
	require.Equal(t, child.TxIDChainHash(), crashSpender(t, ctx, db, winner), "the child keeps its spend of the winner")

	losers, _, err := utxo.ProcessConflicting(ctx, db, 3, block, winners, nil, utxo.NoAncestryGuard)
	require.NoError(t, err, "a child of the winner must not block the re-add")
	require.True(t, losers.Exists(*loser.TxIDChainHash()))
	require.False(t, losers.Exists(*child.TxIDChainHash()))
	require.Equal(t, winner.TxIDChainHash(), crashSpender(t, ctx, db, parent))
	require.False(t, crashConflicting(t, ctx, db, winner.TxIDChainHash()))
	require.True(t, crashConflicting(t, ctx, db, child.TxIDChainHash()))
}

// ConflictRecoveryPartialReverseReadd adds a block with winners W1 and W2, lets a reverse of it stop
// part way, and adds the block again. The reverse either undid W1 and stopped before W2, or marked W1
// Conflicting and stopped before it gave W1's input back. The block's forward checklist is either
// still pending or already acknowledged. In each case the re-add must plan W1 again and leave W2 as it
// is, under the one forward intent of the block.
func ConflictRecoveryPartialReverseReadd(t *testing.T, db utxo.Store) {
	cases := []struct {
		name         string
		acknowledged bool
		markOnly     bool
	}{
		{name: "checklist/W1 reversed"},
		{name: "acknowledged/W1 reversed", acknowledged: true},
		{name: "checklist/W1 marked only", markOnly: true},
		{name: "acknowledged/W1 marked only", acknowledged: true, markOnly: true},
	}

	for index, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			seed := 0x10 + byte(index)*2
			parents := []*bt.Tx{crashParentTx(seed), crashParentTx(seed + 1)}
			var losers, winnerTxs []*bt.Tx
			for _, parent := range parents {
				_, _, err := db.SpendAndCreate(ctx, parent, 1, utxo.WithCreateOnly())
				require.NoError(t, err)
				loser := crashSpendTx(t, parent, 80000)
				_, _, err = db.SpendAndCreate(ctx, loser, 2)
				require.NoError(t, err)
				winner := crashSpendTx(t, parent, 70000)
				_, _, err = db.SpendAndCreate(ctx, winner, 2, utxo.WithCreateOnly(), utxo.WithConflicting(true))
				require.NoError(t, err)
				losers = append(losers, loser)
				winnerTxs = append(winnerTxs, winner)
			}

			block := chainhash.HashH([]byte("partial-reverse-readd-" + tc.name))
			winners := []chainhash.Hash{*winnerTxs[0].TxIDChainHash(), *winnerTxs[1].TxIDChainHash()}
			_, _, err := utxo.ProcessConflicting(ctx, db, 3, block, winners, nil, utxo.NoAncestryGuard, utxo.WithDeferredConflictCompletion())
			require.NoError(t, err)

			if tc.acknowledged {
				require.NoError(t, utxo.CompleteConflictIntentsForBlock(ctx, db, block))
			}

			if tc.markOnly {
				_, _, err = db.SetConflicting(ctx, winners[:1], true)
				require.NoError(t, err)
				require.Equal(t, winnerTxs[0].TxIDChainHash(), crashSpender(t, ctx, db, parents[0]))
			} else {
				_, _, err = utxo.ReverseProcessConflicting(ctx, db, 3, block, winners[:1])
				require.NoError(t, err)
				require.Equal(t, losers[0].TxIDChainHash(), crashSpender(t, ctx, db, parents[0]))
			}
			require.True(t, crashConflicting(t, ctx, db, winnerTxs[0].TxIDChainHash()))

			retryWinners := winners
			if !tc.acknowledged {
				retryWinners = winners[1:] // the retained checklist must recover W1 too
			}
			readded, _, err := utxo.ProcessConflicting(ctx, db, 3, block, retryWinners, nil, utxo.NoAncestryGuard, utxo.WithDeferredConflictCompletion())
			require.NoError(t, err, "the re-add must plan the reversed winner again")
			// After a full reverse the loser owns the input again, so the re-add demotes it again. After a
			// mark only, the loser is still Conflicting and owns nothing, so there is no counter to demote;
			// the re-add still reports it while the earlier checklist, which recorded it, awaits its
			// acknowledgement.
			require.Equal(t, !tc.markOnly || !tc.acknowledged, readded.Exists(*losers[0].TxIDChainHash()))

			for i := range winnerTxs {
				require.False(t, crashConflicting(t, ctx, db, winnerTxs[i].TxIDChainHash()), "winner %d", i)
				require.True(t, crashConflicting(t, ctx, db, losers[i].TxIDChainHash()), "loser %d", i)
				require.Equal(t, winnerTxs[i].TxIDChainHash(), crashSpender(t, ctx, db, parents[i]), "owner of parent %d", i)
				require.False(t, crashLocked(t, ctx, db, parents[i].TxIDChainHash()), "parent %d must not stay locked", i)
			}

			pending, err := db.PendingConflictIntents(ctx)
			require.NoError(t, err)
			var forBlock []utxo.ConflictIntent
			for _, p := range pending {
				if p.BlockHash == block {
					forBlock = append(forBlock, p)
				}
			}
			require.Len(t, forBlock, 1, "the block keeps one forward intent")
			require.Equal(t, utxo.ForwardConflictIntentID(block), forBlock[0].IntentID())
			require.ElementsMatch(t, winners, forBlock[0].TxHashes, "planning intent must retain every merged winner for crash replay")

			require.NoError(t, utxo.CompleteConflictIntentsForBlock(ctx, db, block))
			r, err := db.LoadConflictRecovery(ctx, utxo.ForwardConflictIntentID(block))
			require.NoError(t, err)
			require.Nil(t, r)
		})
	}
}

// ConflictRecoveryFreedInputTaken rolls back a finished forward operation after the validator gave
// an input that the operation freed to a new transaction. Loser L spends P1:0, which the winner
// takes, and P2:0, which the operation frees. Y then spends P2:0, and child C spends L. Y's spend is
// legitimate, so the rollback must leave it, and L and C must end Conflicting.
func ConflictRecoveryFreedInputTaken(t *testing.T, db utxo.Store) {
	ctx := t.Context()
	p1 := crashParentTx(0x18)
	p2 := crashParentTx(0x19)
	for _, parent := range []*bt.Tx{p1, p2} {
		_, _, err := db.SpendAndCreate(ctx, parent, 1, utxo.WithCreateOnly())
		require.NoError(t, err)
	}
	loser := crashSpendTx(t, p1, 80000)
	require.NoError(t, loser.From(p2.TxIDChainHash().String(), 0, p2.Outputs[0].LockingScript.String(), p2.Outputs[0].Satoshis))
	loser.Inputs[1].UnlockingScript = dummyUnlockingScript
	_, _, err := db.SpendAndCreate(ctx, loser, 2)
	require.NoError(t, err)
	child := crashSpendTx(t, loser, 60000)
	_, _, err = db.SpendAndCreate(ctx, child, 2)
	require.NoError(t, err)
	winner := crashSpendTx(t, p1, 70000)
	_, _, err = db.SpendAndCreate(ctx, winner, 2, utxo.WithCreateOnly(), utxo.WithConflicting(true))
	require.NoError(t, err)

	block := chainhash.HashH([]byte("freed-input-taken"))
	winners := []chainhash.Hash{*winner.TxIDChainHash()}
	_, _, err = utxo.ProcessConflicting(ctx, db, 3, block, winners, nil, utxo.NoAncestryGuard, utxo.WithDeferredConflictCompletion())
	require.NoError(t, err)
	require.Nil(t, crashSpender(t, ctx, db, p2), "the operation frees the loser's other input")

	y := crashSpendTx(t, p2, 50000)
	_, _, err = db.SpendAndCreate(ctx, y, 3)
	require.NoError(t, err)

	intent := utxo.ConflictIntent{Kind: utxo.ConflictIntentForward, BlockHeight: 3, BlockHash: block, TxHashes: winners}
	mayBeMarked, err := utxo.RollbackConflictIntent(ctx, db, intent)
	require.NoError(t, err, "a legitimate spend of a freed input must not block the rollback")
	require.Subset(t, mayBeMarked, []chainhash.Hash{*winner.TxIDChainHash(), *loser.TxIDChainHash(), *child.TxIDChainHash()})

	require.Equal(t, y.TxIDChainHash(), crashSpender(t, ctx, db, p2), "Y keeps its input")
	require.False(t, crashConflicting(t, ctx, db, y.TxIDChainHash()))
	require.Nil(t, crashSpender(t, ctx, db, p1), "neither the winner nor the displaced loser owns P1:0")
	require.True(t, crashConflicting(t, ctx, db, winner.TxIDChainHash()))
	require.True(t, crashConflicting(t, ctx, db, loser.TxIDChainHash()), "the loser lost an input")
	require.True(t, crashConflicting(t, ctx, db, child.TxIDChainHash()), "the child spends a conflicting output")
	for _, parent := range []*bt.Tx{p1, p2} {
		require.False(t, crashLocked(t, ctx, db, parent.TxIDChainHash()))
	}

	r, err := db.LoadConflictRecovery(ctx, intent.IntentID())
	require.NoError(t, err)
	require.Equal(t, utxo.ConflictRecoveryRolledBack, r.Phase)
	require.NoError(t, db.CompleteConflictIntent(ctx, intent.IntentID()))
}

// ConflictRecoveryBlockKeyedIntent checks that a block has one forward intent whatever winner set a
// caller passes, and that acknowledgement and retirement find it without a scan of the WAL.
func ConflictRecoveryBlockKeyedIntent(t *testing.T, db utxo.Store) {
	ctx := t.Context()
	parents := make([]*bt.Tx, 0, 2)
	losers := make([]*bt.Tx, 0, 2)
	winnerTxs := make([]*bt.Tx, 0, 2)
	for _, seed := range []byte{0x1a, 0x1b} {
		parent := crashParentTx(seed)
		_, _, err := db.SpendAndCreate(ctx, parent, 1, utxo.WithCreateOnly())
		require.NoError(t, err)
		loser := crashSpendTx(t, parent, 80000)
		_, _, err = db.SpendAndCreate(ctx, loser, 2)
		require.NoError(t, err)
		winner := crashSpendTx(t, parent, 70000)
		_, _, err = db.SpendAndCreate(ctx, winner, 2, utxo.WithCreateOnly(), utxo.WithConflicting(true))
		require.NoError(t, err)
		parents = append(parents, parent)
		losers = append(losers, loser)
		winnerTxs = append(winnerTxs, winner)
	}

	counted := &countPendingScans{Store: db}
	block := chainhash.HashH([]byte("block-keyed-intent"))
	winners := []chainhash.Hash{*winnerTxs[0].TxIDChainHash(), *winnerTxs[1].TxIDChainHash()}
	_, _, err := utxo.ProcessConflicting(ctx, counted, 3, block, winners, nil, utxo.NoAncestryGuard, utxo.WithDeferredConflictCompletion())
	require.NoError(t, err)

	// A caller that filters the winners differently finds the same finished checklist.
	again, _, err := utxo.ProcessConflicting(ctx, counted, 3, block, winners[1:], nil, utxo.NoAncestryGuard, utxo.WithDeferredConflictCompletion())
	require.NoError(t, err)
	require.True(t, again.Exists(*losers[0].TxIDChainHash()))
	require.True(t, again.Exists(*losers[1].TxIDChainHash()))

	pending, err := db.PendingConflictIntents(ctx)
	require.NoError(t, err)
	count := 0
	for _, p := range pending {
		if p.BlockHash == block {
			count++
		}
	}
	require.Equal(t, 1, count, "the block keeps one forward intent")

	require.NoError(t, utxo.CompleteConflictIntentsForBlock(ctx, counted, block))
	r, err := db.LoadConflictRecovery(ctx, utxo.ForwardConflictIntentID(block))
	require.NoError(t, err)
	require.Nil(t, r, "the acknowledgement completes the block's intent")

	require.NoError(t, utxo.RetireConflictIntentsForBlocks(ctx, counted, []chainhash.Hash{block}))
	require.Zero(t, counted.scans.Load(), "acknowledgement and retirement must not scan the WAL")

	for i := range parents {
		require.Equal(t, winnerTxs[i].TxIDChainHash(), crashSpender(t, ctx, db, parents[i]))
	}
}

// ConflictRecoverySupersetWinners refuses additional winners until the caller acknowledges the
// first result. A child accepted after W1 was applied must stay valid throughout refusal, retry,
// acknowledgement and the next operation that applies W2.
func ConflictRecoverySupersetWinners(t *testing.T, db utxo.Store) {
	ctx := t.Context()
	var parents, losers, winnerTxs []*bt.Tx
	for _, seed := range []byte{0x1c, 0x1d} {
		parent := crashParentTx(seed)
		_, _, err := db.SpendAndCreate(ctx, parent, 1, utxo.WithCreateOnly())
		require.NoError(t, err)
		loser := crashSpendTx(t, parent, 80000)
		_, _, err = db.SpendAndCreate(ctx, loser, 2)
		require.NoError(t, err)
		winner := crashSpendTx(t, parent, 70000)
		_, _, err = db.SpendAndCreate(ctx, winner, 2, utxo.WithCreateOnly(), utxo.WithConflicting(true))
		require.NoError(t, err)
		parents = append(parents, parent)
		losers = append(losers, loser)
		winnerTxs = append(winnerTxs, winner)
	}

	block := chainhash.HashH([]byte("superset-winners"))
	id := utxo.ForwardConflictIntentID(block)
	winners := []chainhash.Hash{*winnerTxs[0].TxIDChainHash(), *winnerTxs[1].TxIDChainHash()}
	first, firstMarked, err := utxo.ProcessConflicting(ctx, db, 3, block, winners[:1], nil, utxo.NoAncestryGuard, utxo.WithDeferredConflictCompletion())
	require.NoError(t, err)
	require.True(t, first.Exists(*losers[0].TxIDChainHash()))
	before, err := db.LoadConflictRecovery(ctx, id)
	require.NoError(t, err)

	child := crashSpendTx(t, winnerTxs[0], 60000)
	_, _, err = db.SpendAndCreate(ctx, child, 3)
	require.NoError(t, err)

	// Both completion modes must refuse. A caller cannot bypass acknowledgement by asking the
	// superset operation to complete its WAL immediately.
	for _, deferred := range []bool{false, true} {
		var evicted []chainhash.Hash
		opts := []utxo.ProcessConflictingOption{utxo.WithConflictEvictor(func(h []chainhash.Hash) { evicted = append(evicted, h...) })}
		if deferred {
			opts = append(opts, utxo.WithDeferredConflictCompletion())
		}
		result, marked, err := utxo.ProcessConflicting(ctx, db, 3, block, winners, nil, utxo.NoAncestryGuard, opts...)
		require.ErrorContains(t, err, "acknowledge the existing conflict intent before adding winners")
		require.Nil(t, result)
		require.Empty(t, marked, "refusal must not evict valid transactions")
		require.Empty(t, evicted)
		after, err := db.LoadConflictRecovery(ctx, id)
		require.NoError(t, err)
		require.Equal(t, before, after, "refusal preserves the before-image and pending losers")
		require.Equal(t, winnerTxs[0].TxIDChainHash(), crashSpender(t, ctx, db, parents[0]))
		require.Equal(t, losers[1].TxIDChainHash(), crashSpender(t, ctx, db, parents[1]))
		require.False(t, crashConflicting(t, ctx, db, child.TxIDChainHash()))
		require.False(t, crashConflicting(t, ctx, db, winnerTxs[0].TxIDChainHash()))
		require.True(t, crashConflicting(t, ctx, db, winnerTxs[1].TxIDChainHash()))
	}

	// The caller can still recover the original result and finish its subtree and queue updates.
	again, marked, err := utxo.ProcessConflicting(ctx, db, 3, block, winners[:1], nil, utxo.NoAncestryGuard, utxo.WithDeferredConflictCompletion())
	require.NoError(t, err)
	require.True(t, again.Exists(*losers[0].TxIDChainHash()))
	require.ElementsMatch(t, firstMarked, marked)
	require.NoError(t, utxo.CompleteConflictIntentsForBlock(ctx, db, block))

	second, secondMarked, err := utxo.ProcessConflicting(ctx, db, 3, block, winners, nil, utxo.NoAncestryGuard, utxo.WithDeferredConflictCompletion())
	require.NoError(t, err)
	require.True(t, second.Exists(*losers[1].TxIDChainHash()))
	require.False(t, second.Exists(*losers[0].TxIDChainHash()), "the earlier loser was acknowledged")
	require.ElementsMatch(t, []chainhash.Hash{*losers[1].TxIDChainHash()}, secondMarked)
	for i := range parents {
		require.Equal(t, winnerTxs[i].TxIDChainHash(), crashSpender(t, ctx, db, parents[i]))
		require.False(t, crashConflicting(t, ctx, db, winnerTxs[i].TxIDChainHash()))
		require.True(t, crashConflicting(t, ctx, db, losers[i].TxIDChainHash()))
	}
	require.NoError(t, utxo.CompleteConflictIntentsForBlock(ctx, db, block))
	require.False(t, crashConflicting(t, ctx, db, child.TxIDChainHash()), "the accepted child stays valid after WAL completion")
	require.Equal(t, child.TxIDChainHash(), crashSpender(t, ctx, db, winnerTxs[0]))
}

// ConflictRecoverySupersetRefusedPhases checks the admission rule for every durable phase. The
// new winner is still Conflicting, so it asks for a plan: refusal must precede the reads of the
// recorded winner as well as recovery writes.
func ConflictRecoverySupersetRefusedPhases(t *testing.T, db utxo.Store) {
	ctx := t.Context()
	for i, phase := range []string{utxo.ConflictRecoveryPlanning, utxo.ConflictRecoveryPrepared,
		utxo.ConflictRecoveryMutating, utxo.ConflictRecoveryRollingBack, utxo.ConflictRecoveryRolledBack,
		utxo.ConflictRecoveryApplied, utxo.ConflictRecoveryFinished} {
		t.Run(phase, func(t *testing.T) {
			block := chainhash.HashH([]byte("superset-refused-" + phase))
			winner := chainhash.HashH([]byte("recorded-winner-" + phase))
			parent := crashParentTx(byte(0x40 + i))
			_, _, err := db.SpendAndCreate(ctx, parent, 1, utxo.WithCreateOnly())
			require.NoError(t, err)
			extraTx := crashSpendTx(t, parent, 70000)
			_, _, err = db.SpendAndCreate(ctx, extraTx, 2, utxo.WithCreateOnly(), utxo.WithConflicting(true))
			require.NoError(t, err)
			extra := *extraTx.TxIDChainHash()
			r := &utxo.ConflictRecovery{Version: 1, Phase: phase, Winners: []chainhash.Hash{winner}}
			intent := utxo.ConflictIntent{Kind: utxo.ConflictIntentForward, BlockHash: block, TxHashes: r.Winners,
				Recovery: &utxo.ConflictRecovery{Version: 1, Phase: utxo.ConflictRecoveryPlanning, Winners: r.Winners}}
			if phase == utxo.ConflictRecoveryPlanning {
				// Exercise the production Begin path: discovery fails because the winner is
				// absent, but its identity must already be durable for the admission check.
				_, _, err := utxo.ProcessConflicting(ctx, db, 3, block, r.Winners, nil, utxo.NoAncestryGuard)
				require.ErrorContains(t, err, "error getting tx")
				loaded, err := db.LoadConflictRecovery(ctx, intent.IntentID())
				require.NoError(t, err)
				require.Equal(t, r, loaded)
			} else {
				require.NoError(t, db.BeginConflictIntent(ctx, intent))
				require.NoError(t, db.SaveConflictRecovery(ctx, intent.IntentID(), r))
			}
			before, err := db.PendingConflictIntents(ctx)
			require.NoError(t, err)
			result, marked, err := utxo.ProcessConflicting(ctx, db, 3, block, []chainhash.Hash{winner, extra}, nil, utxo.NoAncestryGuard)
			require.ErrorContains(t, err, "acknowledge the existing conflict intent before adding winners")
			require.Nil(t, result)
			require.Empty(t, marked)
			after, err := db.PendingConflictIntents(ctx)
			require.NoError(t, err)
			require.Equal(t, before, after, "the intent and recovery checklist must be unchanged")
			require.NoError(t, db.CompleteConflictIntent(ctx, intent.IntentID()))
		})
	}
}

// ConflictRecoveryFreedWinningInputTaken rolls back a finished forward operation after a new
// transaction took a winning input that was free at planning. Winner W spends P1:0, contested by L,
// and P2:0, which was unspent. The rollback frees P2:0, and T takes it before the verification. T's
// spend is legitimate, so the rollback must leave it and still succeed.
func ConflictRecoveryFreedWinningInputTaken(t *testing.T, db utxo.Store) {
	ctx := t.Context()
	p1 := crashParentTx(0x1e)
	p2 := crashParentTx(0x1f)
	for _, parent := range []*bt.Tx{p1, p2} {
		_, _, err := db.SpendAndCreate(ctx, parent, 1, utxo.WithCreateOnly())
		require.NoError(t, err)
	}
	loser := crashSpendTx(t, p1, 80000)
	_, _, err := db.SpendAndCreate(ctx, loser, 2)
	require.NoError(t, err)
	winner := crashSpendTx(t, p1, 70000)
	require.NoError(t, winner.From(p2.TxIDChainHash().String(), 0, p2.Outputs[0].LockingScript.String(), p2.Outputs[0].Satoshis))
	winner.Inputs[1].UnlockingScript = dummyUnlockingScript
	_, _, err = db.SpendAndCreate(ctx, winner, 2, utxo.WithCreateOnly(), utxo.WithConflicting(true))
	require.NoError(t, err)

	block := chainhash.HashH([]byte("freed-winning-input-taken"))
	winners := []chainhash.Hash{*winner.TxIDChainHash()}
	_, _, err = utxo.ProcessConflicting(ctx, db, 3, block, winners, nil, utxo.NoAncestryGuard, utxo.WithDeferredConflictCompletion())
	require.NoError(t, err)
	require.Equal(t, winner.TxIDChainHash(), crashSpender(t, ctx, db, p2))

	// The rollback's first step frees the winning spends. Model T taking P2:0 right after it.
	winningSpends, err := utxo.GetSpends(winner)
	require.NoError(t, err)
	require.NoError(t, db.Unspend(ctx, winningSpends[1:]))
	taker := crashSpendTx(t, p2, 50000)
	_, _, err = db.SpendAndCreate(ctx, taker, 3)
	require.NoError(t, err)

	intent := utxo.ConflictIntent{Kind: utxo.ConflictIntentForward, BlockHeight: 3, BlockHash: block, TxHashes: winners}
	_, err = utxo.RollbackConflictIntent(ctx, db, intent)
	require.NoError(t, err, "a legitimate spend of a freed winning input must not block the rollback")

	require.Equal(t, taker.TxIDChainHash(), crashSpender(t, ctx, db, p2), "T keeps P2:0")
	require.False(t, crashConflicting(t, ctx, db, taker.TxIDChainHash()))
	require.Equal(t, loser.TxIDChainHash(), crashSpender(t, ctx, db, p1), "L gets P1:0 back")
	require.False(t, crashConflicting(t, ctx, db, loser.TxIDChainHash()))
	require.True(t, crashConflicting(t, ctx, db, winner.TxIDChainHash()))

	r, err := db.LoadConflictRecovery(ctx, intent.IntentID())
	require.NoError(t, err)
	require.Equal(t, utxo.ConflictRecoveryRolledBack, r.Phase)
	require.NoError(t, db.CompleteConflictIntent(ctx, intent.IntentID()))
}

// ConflictRecoveryAppliedWinnerPrunedParent runs a block again after its winner was applied and
// acknowledged, and after the pruner removed the winner's parent. The winner is confirmed on the
// current chain, so the run must skip it instead of failing on the missing parent.
func ConflictRecoveryAppliedWinnerPrunedParent(t *testing.T, db utxo.Store) {
	const currentBlockID = 78

	ctx := t.Context()
	parent := crashParentTx(0x20)
	_, _, err := db.SpendAndCreate(ctx, parent, 1, utxo.WithCreateOnly())
	require.NoError(t, err)
	loser := crashSpendTx(t, parent, 80000)
	_, _, err = db.SpendAndCreate(ctx, loser, 2)
	require.NoError(t, err)
	winner := crashSpendTx(t, parent, 70000)
	_, _, err = db.SpendAndCreate(ctx, winner, 2, utxo.WithCreateOnly(), utxo.WithConflicting(true))
	require.NoError(t, err)

	block := chainhash.HashH([]byte("applied-winner-pruned-parent"))
	winners := []chainhash.Hash{*winner.TxIDChainHash()}
	_, _, err = utxo.ProcessConflicting(ctx, db, 3, block, winners, nil, utxo.NoAncestryGuard)
	require.NoError(t, err)

	_, err = db.SetMinedMulti(ctx, []*chainhash.Hash{winner.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: currentBlockID, BlockHeight: 3})
	require.NoError(t, err)
	require.NoError(t, db.Delete(ctx, parent.TxIDChainHash()))

	guard := utxo.NewCurrentChainGuard(func(_ context.Context, ids []uint32) (bool, error) {
		for _, id := range ids {
			if id == currentBlockID {
				return true, nil
			}
		}
		return false, nil
	})

	losers, _, err := utxo.ProcessConflicting(ctx, db, 3, block, winners, nil, utxo.NoAncestryGuard, utxo.WithRecoveryChainGuard(guard))
	require.NoError(t, err, "a confirmed applied winner with a pruned parent must be skipped")
	require.Zero(t, losers.Length())

	_, _, err = utxo.ProcessConflicting(ctx, db, 3, block, winners, nil, utxo.NoAncestryGuard)
	require.Error(t, err, "without confirmation a missing parent is still an error")
}

// ConflictRecoveryLegacyKey checks that a forward record written by an older build keeps its own key.
// A new operation on the same block has its own record, and the legacy record stays for repair.
func ConflictRecoveryLegacyKey(t *testing.T, db utxo.Store) {
	ctx := t.Context()
	parent := crashParentTx(0x21)
	_, _, err := db.SpendAndCreate(ctx, parent, 1, utxo.WithCreateOnly())
	require.NoError(t, err)
	loser := crashSpendTx(t, parent, 80000)
	_, _, err = db.SpendAndCreate(ctx, loser, 2)
	require.NoError(t, err)
	winner := crashSpendTx(t, parent, 70000)
	_, _, err = db.SpendAndCreate(ctx, winner, 2, utxo.WithCreateOnly(), utxo.WithConflicting(true))
	require.NoError(t, err)

	block := chainhash.HashH([]byte("legacy-key-block"))
	winners := []chainhash.Hash{*winner.TxIDChainHash()}
	legacyID := utxo.LegacyConflictIntentID(utxo.ConflictIntentForward, block, 3, winners)
	require.NotEqual(t, utxo.ForwardConflictIntentID(block), legacyID)

	legacy := utxo.ConflictIntent{Kind: utxo.ConflictIntentForward, BlockHeight: 3, BlockHash: block, TxHashes: winners, StartedAt: 1, ID: legacyID}
	require.NoError(t, db.BeginConflictIntent(ctx, legacy))

	pending, err := db.PendingConflictIntents(ctx)
	require.NoError(t, err)
	var found bool
	for _, p := range pending {
		if p.BlockHash == block {
			require.Equal(t, legacyID, p.IntentID(), "a legacy record is addressed by its stored key")
			found = true
		}
	}
	require.True(t, found)

	r, err := db.LoadConflictRecovery(ctx, legacyID)
	require.NoError(t, err)
	require.Nil(t, r, "a legacy record has no checklist")

	_, _, err = utxo.ProcessConflicting(ctx, db, 3, block, winners, nil, utxo.NoAncestryGuard, utxo.WithDeferredConflictCompletion())
	require.NoError(t, err, "a new operation on the block has its own record")
	require.Equal(t, winner.TxIDChainHash(), crashSpender(t, ctx, db, parent))

	pending, err = db.PendingConflictIntents(ctx)
	require.NoError(t, err)
	var ids []chainhash.Hash
	for _, p := range pending {
		if p.BlockHash == block {
			ids = append(ids, p.IntentID())
		}
	}
	require.ElementsMatch(t, []chainhash.Hash{legacyID, utxo.ForwardConflictIntentID(block)}, ids)

	require.NoError(t, utxo.CompleteConflictIntentsForBlock(ctx, db, block))
	require.NoError(t, db.CompleteConflictIntent(ctx, legacyID))
}

// failRollbackStep wraps a real store and fails one step of a rollback. nth counts the calls of the
// failing method, so a later call of a method can fail while the earlier ones succeed.
type failRollbackStep struct {
	utxo.Store
	step  string
	nth   int
	mu    sync.Mutex
	calls map[string]int
}

// hit counts a call. The rollback reads in parallel, so the count is guarded.
func (s *failRollbackStep) hit(name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.calls == nil {
		s.calls = map[string]int{}
	}

	s.calls[name]++

	return s.step == name && s.calls[name] == s.nth
}

func (s *failRollbackStep) SetConflicting(ctx context.Context, hashes []chainhash.Hash, value bool) ([]*utxo.Spend, []chainhash.Hash, error) {
	if s.hit("setConflicting") {
		return nil, nil, errors.NewStorageError("injected setConflicting failure")
	}

	return s.Store.SetConflicting(ctx, hashes, value)
}

func (s *failRollbackStep) Unspend(ctx context.Context, spends []*utxo.Spend, locked ...bool) error {
	if s.hit("unspend") {
		return errors.NewStorageError("injected unspend failure")
	}

	return s.Store.Unspend(ctx, spends, locked...)
}

func (s *failRollbackStep) GetSpend(ctx context.Context, sp *utxo.Spend) (*utxo.SpendResponse, error) {
	if s.hit("getSpend") {
		return nil, errors.NewStorageError("injected getSpend failure")
	}

	return s.Store.GetSpend(ctx, sp)
}

func (s *failRollbackStep) Get(ctx context.Context, hash *chainhash.Hash, f ...fields.FieldName) (*meta.Data, error) {
	if s.hit("get") {
		return nil, errors.NewStorageError("injected get failure")
	}

	return s.Store.Get(ctx, hash, f...)
}

// ConflictRecoveryRollbackStepFailures fails each store step of a rollback in turn. A finished
// operation has a loser L, a winner W and a mempool child C of W that attached after the checklist.
// Every failure leaves the checklist pending; the rollback then succeeds on a clean retry.
func ConflictRecoveryRollbackStepFailures(t *testing.T, db utxo.Store) {
	cases := []struct {
		name string
		step string
		nth  int
		want string
	}{
		{"first mark fails", "setConflicting", 1, "injected setConflicting failure"},
		{"unspend of the winning spends fails", "unspend", 1, "injected unspend failure"},
		{"owner read before restore fails", "getSpend", 1, "injected getSpend failure"},
		{"verification read fails", "getSpend", 2, "injected getSpend failure"},
		{"flag restore fails", "setConflicting", 2, "injected setConflicting failure"},
		{"winner spender read fails", "get", 1, "injected get failure"},
		{"outside child read fails", "get", 2, "injected get failure"},
	}

	for index, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			parent := crashParentTx(0x22 + byte(index))
			_, _, err := db.SpendAndCreate(ctx, parent, 1, utxo.WithCreateOnly())
			require.NoError(t, err)
			loser := crashSpendTx(t, parent, 80000)
			_, _, err = db.SpendAndCreate(ctx, loser, 2)
			require.NoError(t, err)
			winner := crashSpendTx(t, parent, 70000)
			_, _, err = db.SpendAndCreate(ctx, winner, 2, utxo.WithCreateOnly(), utxo.WithConflicting(true))
			require.NoError(t, err)

			block := chainhash.HashH([]byte("rollback-step-" + tc.name))
			winners := []chainhash.Hash{*winner.TxIDChainHash()}
			_, _, err = utxo.ProcessConflicting(ctx, db, 3, block, winners, nil, utxo.NoAncestryGuard, utxo.WithDeferredConflictCompletion())
			require.NoError(t, err)

			child := crashSpendTx(t, winner, 60000)
			_, _, err = db.SpendAndCreate(ctx, child, 3)
			require.NoError(t, err)

			intent := utxo.ConflictIntent{Kind: utxo.ConflictIntentForward, BlockHeight: 3, BlockHash: block, TxHashes: winners}
			_, err = utxo.RollbackConflictIntent(ctx, &failRollbackStep{Store: db, step: tc.step, nth: tc.nth}, intent)
			require.ErrorContains(t, err, tc.want)

			r, err := db.LoadConflictRecovery(ctx, intent.IntentID())
			require.NoError(t, err)
			require.NotEqual(t, utxo.ConflictRecoveryRolledBack, r.Phase, "a failed rollback keeps the checklist pending")

			_, err = utxo.RollbackConflictIntent(ctx, db, intent)
			require.NoError(t, err, "a clean retry finishes the rollback")
			require.Equal(t, loser.TxIDChainHash(), crashSpender(t, ctx, db, parent))
			require.True(t, crashConflicting(t, ctx, db, winner.TxIDChainHash()))
			require.True(t, crashConflicting(t, ctx, db, child.TxIDChainHash()))
			require.False(t, crashConflicting(t, ctx, db, loser.TxIDChainHash()))
			require.NoError(t, db.CompleteConflictIntent(ctx, intent.IntentID()))
		})
	}
}

// ConflictRecoveryReplanThenHeal plans a block again over an earlier checklist that applied W1,
// and then heals the block as stale. The re-plan happens for a reverse that undid W1, and for a
// mark that left W1 Conflicting but still the spender. After each re-plan
// the record must hold the before-image of every applied winner, so the heal restores L1 and L2 as
// the owners of their inputs. A record that kept only W2's before-image would leave W1 applied and
// L1 Conflicting with no record to undo them.
func ConflictRecoveryReplanThenHeal(t *testing.T, db utxo.Store) {
	cases := []struct {
		name     string
		reverse  bool
		markOnly bool
	}{
		{name: "W1 reversed", reverse: true},
		{name: "W1 marked only", markOnly: true},
	}

	for index, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			var parents, losers, winnerTxs []*bt.Tx
			for i := 0; i < 2; i++ {
				parent := crashParentTx(0xa0 + byte(index)*2 + byte(i))
				_, _, err := db.SpendAndCreate(ctx, parent, 1, utxo.WithCreateOnly())
				require.NoError(t, err)
				loser := crashSpendTx(t, parent, 80000)
				_, _, err = db.SpendAndCreate(ctx, loser, 2)
				require.NoError(t, err)
				winner := crashSpendTx(t, parent, 70000)
				_, _, err = db.SpendAndCreate(ctx, winner, 2, utxo.WithCreateOnly(), utxo.WithConflicting(true))
				require.NoError(t, err)
				parents = append(parents, parent)
				losers = append(losers, loser)
				winnerTxs = append(winnerTxs, winner)
			}

			block := chainhash.HashH([]byte("replan-then-heal-" + tc.name))
			intent := utxo.ConflictIntent{Kind: utxo.ConflictIntentForward, BlockHeight: 3, BlockHash: block}
			winners := []chainhash.Hash{*winnerTxs[0].TxIDChainHash(), *winnerTxs[1].TxIDChainHash()}
			_, _, err := utxo.ProcessConflicting(ctx, db, 3, block, winners, nil, utxo.NoAncestryGuard, utxo.WithDeferredConflictCompletion())
			require.NoError(t, err)

			if tc.reverse {
				_, _, err = utxo.ReverseProcessConflicting(ctx, db, 3, block, winners[:1])
				require.NoError(t, err)
			}
			if tc.markOnly {
				_, _, err = db.SetConflicting(ctx, winners[:1], true)
				require.NoError(t, err)
			}

			result, _, err := utxo.ProcessConflicting(ctx, db, 3, block, winners, nil, utxo.NoAncestryGuard, utxo.WithDeferredConflictCompletion())
			require.NoError(t, err, "the re-plan must succeed")
			require.True(t, result.Exists(*losers[0].TxIDChainHash()), "the re-plan reports L1")
			require.True(t, result.Exists(*losers[1].TxIDChainHash()), "the re-plan reports L2")

			r, err := db.LoadConflictRecovery(ctx, intent.IntentID())
			require.NoError(t, err)
			require.Equal(t, utxo.ConflictRecoveryFinished, r.Phase)
			var recorded []chainhash.Hash
			for _, tx := range r.Transactions {
				recorded = append(recorded, tx.Hash)
			}
			for i := range parents {
				require.Contains(t, recorded, winners[i], "the record holds winner %d", i)
				require.Contains(t, recorded, *losers[i].TxIDChainHash(), "the record holds loser %d", i)
				require.Equal(t, winnerTxs[i].TxIDChainHash(), crashSpender(t, ctx, db, parents[i]), "owner of parent %d after the re-plan", i)
			}
			var owners []chainhash.Hash
			for _, sp := range r.OriginalSpends {
				if sp.SpendingData != nil && sp.SpendingData.TxID != nil {
					owners = append(owners, *sp.SpendingData.TxID)
				}
			}
			require.ElementsMatch(t, []chainhash.Hash{*losers[0].TxIDChainHash(), *losers[1].TxIDChainHash()}, owners, "the record holds the original owner of every winning input")

			// The block went stale and no block on the current chain confirms a winner.
			guard := utxo.NewCurrentChainGuard(func(context.Context, []uint32) (bool, error) { return false, nil })
			mayBeMarked, err := utxo.HealStaleForwardIntent(ctx, db, intent, guard, func(context.Context) (bool, error) { return true, nil })
			require.NoError(t, err)
			require.ElementsMatch(t, append(append([]chainhash.Hash(nil), winners...), *losers[0].TxIDChainHash(), *losers[1].TxIDChainHash()), mayBeMarked)

			r, err = db.LoadConflictRecovery(ctx, intent.IntentID())
			require.NoError(t, err)
			require.Equal(t, utxo.ConflictRecoveryRolledBack, r.Phase)
			for i := range parents {
				require.Equal(t, losers[i].TxIDChainHash(), crashSpender(t, ctx, db, parents[i]), "the heal restores the owner of parent %d", i)
				require.True(t, crashConflicting(t, ctx, db, winnerTxs[i].TxIDChainHash()), "winner %d is undone", i)
				require.False(t, crashConflicting(t, ctx, db, losers[i].TxIDChainHash()), "loser %d is restored", i)
				require.False(t, crashLocked(t, ctx, db, parents[i].TxIDChainHash()), "parent %d must not stay locked", i)
			}
			require.NoError(t, db.CompleteConflictIntent(ctx, intent.IntentID()))
		})
	}
}

// ConflictRecoveryRollbackRetryTwoWinners rolls back a finished operation with two winners after
// each gained a mempool child outside the checklist. The first attempt fails after the winners are
// Conflicting, so the retry must find both children through the winners' spenders, read in
// parallel, and cascade both.
func ConflictRecoveryRollbackRetryTwoWinners(t *testing.T, db utxo.Store) {
	ctx := t.Context()
	var parents, losers, winnerTxs, children []*bt.Tx
	for i := 0; i < 2; i++ {
		parent := crashParentTx(0x9a + byte(i))
		_, _, err := db.SpendAndCreate(ctx, parent, 1, utxo.WithCreateOnly())
		require.NoError(t, err)
		loser := crashSpendTx(t, parent, 80000)
		_, _, err = db.SpendAndCreate(ctx, loser, 2)
		require.NoError(t, err)
		winner := crashSpendTx(t, parent, 70000)
		_, _, err = db.SpendAndCreate(ctx, winner, 2, utxo.WithCreateOnly(), utxo.WithConflicting(true))
		require.NoError(t, err)
		parents = append(parents, parent)
		losers = append(losers, loser)
		winnerTxs = append(winnerTxs, winner)
	}

	block := chainhash.HashH([]byte("rollback-retry-two-winners"))
	winners := []chainhash.Hash{*winnerTxs[0].TxIDChainHash(), *winnerTxs[1].TxIDChainHash()}
	_, _, err := utxo.ProcessConflicting(ctx, db, 3, block, winners, nil, utxo.NoAncestryGuard, utxo.WithDeferredConflictCompletion())
	require.NoError(t, err)

	for _, winner := range winnerTxs {
		child := crashSpendTx(t, winner, 60000)
		_, _, err = db.SpendAndCreate(ctx, child, 3)
		require.NoError(t, err)
		children = append(children, child)
	}

	intent := utxo.ConflictIntent{Kind: utxo.ConflictIntentForward, BlockHeight: 3, BlockHash: block, TxHashes: winners}
	_, err = utxo.RollbackConflictIntent(ctx, &failRollbackStep{Store: db, step: "unspend", nth: 1}, intent)
	require.ErrorContains(t, err, "injected unspend failure")

	cascaded, err := utxo.RollbackConflictIntent(ctx, db, intent)
	require.NoError(t, err, "a clean retry finishes the rollback")
	for i := range parents {
		require.Equal(t, losers[i].TxIDChainHash(), crashSpender(t, ctx, db, parents[i]), "owner of parent %d", i)
		require.True(t, crashConflicting(t, ctx, db, winnerTxs[i].TxIDChainHash()), "winner %d", i)
		require.True(t, crashConflicting(t, ctx, db, children[i].TxIDChainHash()), "child %d", i)
		require.Contains(t, cascaded, *children[i].TxIDChainHash(), "child %d is reported", i)
	}
	require.NoError(t, db.CompleteConflictIntent(ctx, intent.IntentID()))
}
