package sql

import (
	"context"
	"fmt"
	"net/url"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

func TestConflictRecoveryDescendants(t *testing.T) {
	store, _ := newTestStore(t)
	tests.ConflictRecoveryDescendants(t, store)
}

func TestConflictRecoveryForwardOwnBlock(t *testing.T) {
	store, _ := newTestStore(t)
	tests.ConflictRecoveryForwardOwnBlock(t, store)
}

func TestConflictRecoveryForeignLock(t *testing.T) {
	store, _ := newTestStore(t)
	tests.ConflictRecoveryForeignLock(t, store)
}

func TestConflictRecoveryFailures(t *testing.T) {
	store, _ := newTestStore(t)
	tests.ConflictRecoveryFailures(t, store)
}

func TestConflictRecoveryLockOwnership(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := t.Context()
	parent := newExtendedTxWithOutputs(t, 1)
	_, _, err := store.SpendAndCreate(ctx, parent, 1, utxo.WithCreateOnly())
	require.NoError(t, err)
	parents := []chainhash.Hash{*parent.TxIDChainHash()}
	owner := chainhash.HashH([]byte("recovery-operation"))
	other := chainhash.HashH([]byte("other-operation"))
	require.NoError(t, store.SetConflictLocks(ctx, owner, parents, true))
	require.Error(t, store.SetConflictLocks(ctx, other, parents, false))
	require.NoError(t, store.SetConflictLocks(ctx, owner, parents, true))
	// A normal lock writer replaces the operation's ownership atomically.
	require.NoError(t, store.SetLocked(ctx, parents, true))
	require.Error(t, store.SetConflictLocks(ctx, owner, parents, false))
	m, err := store.Get(ctx, parent.TxIDChainHash(), fields.Locked)
	require.NoError(t, err)
	require.True(t, m.Locked)
}

func TestConflictRecoveryPersistence(t *testing.T) {
	store, _ := newTestStore(t)
	tests.ConflictRecoveryPersistence(t, store)
}

func TestConflictRecoveryReopen(t *testing.T) {
	ctx := t.Context()
	settings := test.CreateBaseTestSettings(t)
	settings.BatcherDrainMode = true
	location := &url.URL{Scheme: "sqlite", Path: filepath.Join(t.TempDir(), "recovery.db")}
	store, err := New(ctx, ulogger.TestLogger{}, settings, location)
	require.NoError(t, err)
	intent := utxo.ConflictIntent{Kind: utxo.ConflictIntentForward, BlockHash: chainhash.HashH([]byte("reopen")), Recovery: &utxo.ConflictRecovery{Version: 1, Phase: utxo.ConflictRecoveryPlanning}}
	require.NoError(t, store.BeginConflictIntent(ctx, intent))
	recovery := &utxo.ConflictRecovery{Version: 1, Phase: utxo.ConflictRecoveryMutating, Transactions: []utxo.ConflictTransaction{{Hash: intent.BlockHash, Conflicting: true}}}
	require.NoError(t, store.SaveConflictRecovery(ctx, intent.IntentID(), recovery))
	require.NoError(t, store.Close(ctx))
	reopened, err := New(ctx, ulogger.TestLogger{}, settings, location)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close(context.Background())) })
	loaded, err := reopened.LoadConflictRecovery(ctx, intent.IntentID())
	require.NoError(t, err)
	require.Equal(t, recovery, loaded)
	require.NoError(t, reopened.BeginConflictIntent(ctx, intent))
	loaded, err = reopened.LoadConflictRecovery(ctx, intent.IntentID())
	require.NoError(t, err)
	require.Equal(t, recovery, loaded)
}

func TestConflictRecoveryUnexpectedOwner(t *testing.T) {
	store, _ := newTestStore(t)
	tests.ConflictRecoveryUnexpectedOwner(t, store)
}

func TestConflictRecoveryRelatedWinners(t *testing.T) {
	store, _ := newTestStore(t)
	tests.ConflictRecoveryRelatedWinners(t, store)
}

func TestConflictRecoveryConfirmedWinner(t *testing.T) {
	store, _ := newTestStore(t)
	tests.ConflictRecoveryConfirmedWinner(t, store)
}

func TestConflictRecoveryWideCascade(t *testing.T) {
	store, _ := newTestStore(t)
	tests.ConflictRecoveryWideCascade(t, store)
}

// cancelAfterChecks reports no error for the first n Err calls and context.Canceled after that.
type cancelAfterChecks struct {
	context.Context
	n      int32
	checks atomic.Int32
}

func (c *cancelAfterChecks) Err() error {
	if c.checks.Add(1) > c.n {
		return context.Canceled
	}

	return nil
}

// A timed-out rollback must not send further restore writes. The write that already ran stays.
func TestRestoreConflictSpendsStopsOnContext(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := t.Context()

	parent := newExtendedTxWithOutputs(t, 3)
	_, _, err := store.SpendAndCreate(ctx, parent, 1, utxo.WithCreateOnly())
	require.NoError(t, err)

	spender := newExtendedSpendingTx(t, parent, 0)
	for vout := uint32(1); vout < 3; vout++ {
		input := &bt.Input{
			PreviousTxOutIndex: vout,
			SequenceNumber:     0xFFFFFFFF,
			UnlockingScript:    &bscript.Script{},
			PreviousTxSatoshis: parent.Outputs[vout].Satoshis,
			PreviousTxScript:   parent.Outputs[vout].LockingScript,
		}
		require.NoError(t, input.PreviousTxIDAdd(parent.TxIDChainHash()))
		spender.Inputs = append(spender.Inputs, input)
	}

	spends, err := utxo.GetSpends(spender)
	require.NoError(t, err)
	require.Len(t, spends, 3)

	requireSpentBy := func(sp *utxo.Spend, spent bool) {
		t.Helper()

		res, err := store.GetSpend(ctx, sp)
		require.NoError(t, err)

		if !spent {
			require.Nil(t, res.SpendingData, "output %d must stay unspent", sp.Vout)
			return
		}

		require.NotNil(t, res.SpendingData, "output %d must be restored", sp.Vout)
		require.Equal(t, spender.TxIDChainHash(), res.SpendingData.TxID)
	}

	cancelled, cancel := context.WithCancel(ctx)
	cancel()

	err = store.RestoreConflictSpends(cancelled, spends, 2)
	require.ErrorIs(t, err, context.Canceled)

	for _, sp := range spends {
		requireSpentBy(sp, false)
	}

	err = store.RestoreConflictSpends(&cancelAfterChecks{Context: ctx, n: 1}, spends, 2)
	require.ErrorIs(t, err, context.Canceled)

	requireSpentBy(spends[0], true)
	requireSpentBy(spends[1], false)
	requireSpentBy(spends[2], false)

	require.NoError(t, store.RestoreConflictSpends(ctx, spends, 2))

	for _, sp := range spends {
		requireSpentBy(sp, true)
	}
}

func TestConflictRecoveryLateLoserChild(t *testing.T) {
	store, _ := newTestStore(t)
	tests.ConflictRecoveryLateLoserChild(t, store)
}

func TestConflictRecoveryLateFreeInputSpender(t *testing.T) {
	store, _ := newTestStore(t)
	tests.ConflictRecoveryLateFreeInputSpender(t, store)
}

func TestConflictRecoveryOutsiderOwnedInput(t *testing.T) {
	store, _ := newTestStore(t)
	tests.ConflictRecoveryOutsiderOwnedInput(t, store)
}

func TestConflictRecoveryEarlyFailure(t *testing.T) {
	store, _ := newTestStore(t)
	tests.ConflictRecoveryEarlyFailure(t, store)
}

func TestConflictRecoveryRefusedAfterRollback(t *testing.T) {
	store, _ := newTestStore(t)
	tests.ConflictRecoveryRefusedAfterRollback(t, store)
}

func TestConflictRecoveryHealStaleForward(t *testing.T) {
	store, _ := newTestStore(t)
	tests.ConflictRecoveryHealStaleForward(t, store)
}

func TestConflictRecoverySetConflictingChildren(t *testing.T) {
	store, _ := newTestStore(t)
	tests.ConflictRecoverySetConflictingChildren(t, store, false)
}

func TestConflictRecoveryReverseCompletionFailure(t *testing.T) {
	store, _ := newTestStore(t)
	tests.ConflictRecoveryReverseCompletionFailure(t, store)
}

func TestConflictRecoveryWinnerWithConflictingChild(t *testing.T) {
	store, _ := newTestStore(t)
	tests.ConflictRecoveryWinnerWithConflictingChild(t, store)
}

func TestConflictRecoveryLateWinnerChild(t *testing.T) {
	store, _ := newTestStore(t)
	tests.ConflictRecoveryLateWinnerChild(t, store)
}

func TestConflictRecoveryReverseThenReadd(t *testing.T) {
	store, _ := newTestStore(t)
	tests.ConflictRecoveryReverseThenReadd(t, store)
}

// lockAfterPrepare wraps the store. Once the checklist is prepared, another holder locks the parent,
// so the operation cannot take its locks.
type lockAfterPrepare struct {
	*Store
	parent chainhash.Hash
}

func (s *lockAfterPrepare) SaveConflictRecovery(ctx context.Context, id chainhash.Hash, r *utxo.ConflictRecovery) error {
	if err := s.Store.SaveConflictRecovery(ctx, id, r); err != nil {
		return err
	}
	if r.Phase == utxo.ConflictRecoveryPrepared {
		return s.Store.SetLocked(ctx, []chainhash.Hash{s.parent}, true)
	}
	return nil
}

// A parent locked by another holder after preparation stops the operation before it mutates
// anything. The rollback must not try to release that lock, which the operation never took.
func TestConflictRecoveryPreparedForeignLock(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := t.Context()

	parent := newExtendedTxWithOutputs(t, 1)
	_, _, err := store.SpendAndCreate(ctx, parent, 1, utxo.WithCreateOnly())
	require.NoError(t, err)
	loser := newExtendedSpendingTx(t, parent, 0)
	_, _, err = store.SpendAndCreate(ctx, loser, 2)
	require.NoError(t, err)
	winner := newExtendedSpendingTx(t, parent, 0)
	winner.Inputs[0].SequenceNumber = 1
	_, _, err = store.SpendAndCreate(ctx, winner, 2, utxo.WithCreateOnly(), utxo.WithConflicting(true))
	require.NoError(t, err)

	winners := []chainhash.Hash{*winner.TxIDChainHash()}
	wrapper := &lockAfterPrepare{Store: store, parent: *parent.TxIDChainHash()}
	_, _, err = utxo.ProcessConflicting(ctx, wrapper, 3, chainhash.Hash{}, winners, nil, utxo.NoAncestryGuard)
	require.Error(t, err)
	require.NotContains(t, err.Error(), "MANUAL INTERVENTION", "nothing was mutated, so the rollback must succeed")

	intent := utxo.ConflictIntent{Kind: utxo.ConflictIntentForward, BlockHeight: 3, TxHashes: winners}
	r, err := store.LoadConflictRecovery(ctx, intent.IntentID())
	require.NoError(t, err)
	require.Equal(t, utxo.ConflictRecoveryRolledBack, r.Phase)

	m, err := store.Get(ctx, parent.TxIDChainHash(), fields.Locked)
	require.NoError(t, err)
	require.True(t, m.Locked, "the other holder keeps its lock")

	m, err = store.Get(ctx, winner.TxIDChainHash(), fields.Conflicting)
	require.NoError(t, err)
	require.True(t, m.Conflicting)
}

func TestConflictRecoveryLegacyIntent(t *testing.T) {
	store, _ := newTestStore(t)
	tests.ConflictRecoveryLegacyIntent(t, store)
}

func TestConflictRecoveryCancelledVerify(t *testing.T) {
	store, _ := newTestStore(t)
	tests.ConflictRecoveryCancelledVerify(t, store)
}

func TestConflictRecoveryPartialReverseReadd(t *testing.T) {
	store, _ := newTestStore(t)
	tests.ConflictRecoveryPartialReverseReadd(t, store)
}

func TestConflictRecoveryFreedInputTaken(t *testing.T) {
	store, _ := newTestStore(t)
	tests.ConflictRecoveryFreedInputTaken(t, store)
}

func TestConflictRecoveryBlockKeyedIntent(t *testing.T) {
	store, _ := newTestStore(t)
	tests.ConflictRecoveryBlockKeyedIntent(t, store)
}

// The compensation deadline must also bound a restore write that blocks. Another transaction holds the
// row lock of the output, so the restore statement waits. It must return when ctx ends, and its
// transaction must roll back, so no write lands after the restore returns.
func TestRestoreConflictSpendsHonoursDeadline_Postgres(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Postgres integration test in short mode")
	}

	store, ctx := setupPostgresStore(t)

	for outputs, bulk := range map[int]bool{1: false, 2: true} {
		t.Run(fmt.Sprintf("bulk=%t", bulk), func(t *testing.T) {
			store.settings.UtxoStore.BatchSQLOperations = bulk

			parent := newExtendedTxWithOutputs(t, outputs)
			_, _, err := store.SpendAndCreate(ctx, parent, 1, utxo.WithCreateOnly())
			require.NoError(t, err)

			spends, err := utxo.GetSpends(newExtendedSpendingTx(t, parent, 0))
			require.NoError(t, err)

			holder, err := store.db.BeginTx(ctx, nil)
			require.NoError(t, err)

			_, err = holder.ExecContext(ctx, `SELECT o.idx FROM outputs o JOIN transactions t ON t.id = o.transaction_id WHERE t.hash = $1 AND o.idx = 0 FOR UPDATE`, parent.TxIDChainHash()[:])
			require.NoError(t, err)

			// Release the lock in the end, so a restore that ignores ctx returns late instead of hanging.
			released := make(chan struct{})
			go func() {
				defer close(released)
				time.Sleep(10 * time.Second)
				_ = holder.Rollback()
			}()

			deadline, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
			defer cancel()

			start := time.Now()
			err = store.RestoreConflictSpends(deadline, spends, 2)
			elapsed := time.Since(start)

			require.Error(t, err)
			require.Less(t, elapsed, 5*time.Second, "the restore must stop at the deadline, not when the lock is released")

			<-released

			res, err := store.GetSpend(ctx, spends[0])
			require.NoError(t, err)
			require.Nil(t, res.SpendingData, "the cancelled restore must not land later")

			require.NoError(t, store.RestoreConflictSpends(ctx, spends, 2))
			res, err = store.GetSpend(ctx, spends[0])
			require.NoError(t, err)
			require.NotNil(t, res.SpendingData)
		})
	}
}

func TestConflictRecoverySupersetWinners(t *testing.T) {
	store, _ := newTestStore(t)
	tests.ConflictRecoverySupersetWinners(t, store)
}

func TestConflictRecoverySupersetRefusedPhases(t *testing.T) {
	store, _ := newTestStore(t)
	tests.ConflictRecoverySupersetRefusedPhases(t, store)
}

func TestConflictRecoveryFreedWinningInputTaken(t *testing.T) {
	store, _ := newTestStore(t)
	tests.ConflictRecoveryFreedWinningInputTaken(t, store)
}

func TestConflictRecoveryAppliedWinnerPrunedParent(t *testing.T) {
	store, _ := newTestStore(t)
	tests.ConflictRecoveryAppliedWinnerPrunedParent(t, store)
}

func TestConflictRecoveryLegacyKey(t *testing.T) {
	store, _ := newTestStore(t)
	tests.ConflictRecoveryLegacyKey(t, store)
}

// The SQL recovery methods report a missing intent, an invalid spend, and a store that is closed.
func TestConflictRecovery_SQLErrorBranches(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := t.Context()
	id := chainhash.Hash{0xee}
	r := &utxo.ConflictRecovery{Version: 1, Phase: utxo.ConflictRecoveryPrepared}

	require.ErrorContains(t, store.SaveConflictRecovery(ctx, id, r), "is missing")
	require.ErrorContains(t, store.SaveConflictRecovery(ctx, id, &utxo.ConflictRecovery{Version: 2}), "unsupported")
	require.ErrorContains(t, store.RestoreConflictSpends(ctx, []*utxo.Spend{{}}, 1), "invalid conflict restoration spend")

	// A locked parent that this operation does not own is reported, and one that is already unlocked
	// is skipped.
	parent := newExtendedTxWithOutputs(t, 1)
	_, _, err := store.SpendAndCreate(ctx, parent, 1, utxo.WithCreateOnly())
	require.NoError(t, err)
	require.NoError(t, store.SetConflictLocks(ctx, id, []chainhash.Hash{*parent.TxIDChainHash()}, false))
	require.NoError(t, store.SetLocked(ctx, []chainhash.Hash{*parent.TxIDChainHash()}, true))
	require.ErrorContains(t, store.SetConflictLocks(ctx, id, []chainhash.Hash{*parent.TxIDChainHash()}, false), "does not own parent lock")
	require.NoError(t, store.ReleaseConflictLocks(ctx, id, []chainhash.Hash{*parent.TxIDChainHash()}), "a lock of another holder is left alone")

	require.NoError(t, store.db.Close())

	_, err = store.LoadConflictRecovery(ctx, id)
	require.ErrorContains(t, err, "loading conflict recovery checklist")
	require.ErrorContains(t, store.SaveConflictRecovery(ctx, id, r), "saving conflict recovery checklist")
	require.Error(t, store.SetConflictLocks(ctx, id, []chainhash.Hash{{1}}, true))
	require.Error(t, store.ReleaseConflictLocks(ctx, id, []chainhash.Hash{{1}}))
	_, err = store.PendingConflictIntents(ctx)
	require.Error(t, err)
}

func TestConflictRecoveryRollbackStepFailures(t *testing.T) {
	store, _ := newTestStore(t)
	tests.ConflictRecoveryRollbackStepFailures(t, store)
}

func TestConflictRecoveryReplanThenHeal(t *testing.T) {
	store, _ := newTestStore(t)
	tests.ConflictRecoveryReplanThenHeal(t, store)
}

func TestConflictRecoveryRollbackRetryTwoWinners(t *testing.T) {
	store, _ := newTestStore(t)
	tests.ConflictRecoveryRollbackRetryTwoWinners(t, store)
}
