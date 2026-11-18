package aerospike_test

import (
	"context"
	"encoding/hex"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	teranode_aerospike "github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	"github.com/stretchr/testify/require"
)

// recordBins reads the named bins of record idx of a transaction straight off the master or a
// pagination child with the raw client, which is the only way to see a bin the store does not
// expose. It returns nil when the record does not exist.
func recordBins(t *testing.T, client *uaerospike.Client, store *teranode_aerospike.Store, hash *chainhash.Hash, idx uint32, names ...fields.FieldName) aerospike.BinMap {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)

	key, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), uaerospike.CalculateKeySourceInternal(hash, idx))
	require.NoError(t, err)

	binNames := make([]string, len(names))
	for i, n := range names {
		binNames[i] = n.String()
	}

	rec, aErr := client.Get(util.GetAerospikeReadPolicy(tSettings), key, binNames...)
	if aErr != nil {
		require.ErrorIs(t, aErr, aerospike.ErrKeyNotFound)

		return nil
	}

	return rec.Bins
}

// attemptOf reads the token of the attempt that wrote the creating record of hash, the way the
// sweep and the setMined pre-flight read it. It is nil for a record that is not creating.
func attemptOf(t *testing.T, ctx context.Context, store *teranode_aerospike.Store, hash *chainhash.Hash) []byte {
	t.Helper()

	md, err := store.Get(ctx, hash, fields.Attempt)
	if errors.Is(err, errors.ErrTxNotFound) {
		return nil
	}

	require.NoError(t, err)

	return md.Attempt
}

// requireHeld asserts that the record at hash is the hold a rolled back attempt leaves: it reads
// as creating and rolling back, so every gate refuses it, and it carries no transaction.
func requireHeld(t *testing.T, ctx context.Context, store *teranode_aerospike.Store, hash *chainhash.Hash, msgAndArgs ...any) {
	t.Helper()

	got := &meta.Data{}
	require.NoError(t, store.GetMeta(ctx, hash, got), msgAndArgs...)
	require.True(t, got.Creating && got.RollingBack && got.Hold, msgAndArgs...)
	require.Nil(t, got.Tx, msgAndArgs...)
}

func TestBeginCreateFirstRollback(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)
	cleanDB(t, client)

	_, err := store.Create(ctx, tests.ParentTx, 100)
	require.NoError(t, err)

	t.Run("a creating record is marked and stays creating", func(t *testing.T) {
		child := spendableChildTx(t, tests.ParentTx, 0)
		hash := child.TxIDChainHash()

		_, err := store.Create(ctx, child, 101, utxo.WithCreating(true))
		require.NoError(t, err)

		began, err := beginAsCreator(ctx, store, hash, attemptOf(t, ctx, store, hash))
		require.NoError(t, err)
		require.True(t, began)

		bins := recordBins(t, client, store, hash, 0, fields.Creating, fields.RollingBack)
		require.Equal(t, true, bins[fields.RollingBack.String()], "the rollingBack bin is set")
		require.Equal(t, true, bins[fields.Creating.String()], "the creating bin stays, so the Lua spend gate still rejects the outputs")

		got := &meta.Data{}
		require.NoError(t, store.GetMeta(ctx, hash, got))
		require.True(t, got.Creating)
		require.True(t, got.RollingBack, "GetMeta must report the marker")
	})

	t.Run("an already rolling-back record is not claimed a second time", func(t *testing.T) {
		child := spendableChildTx(t, tests.ParentTx, 1)
		hash := child.TxIDChainHash()

		_, err := store.Create(ctx, child, 101, utxo.WithCreating(true))
		require.NoError(t, err)

		began, err := beginAsCreator(ctx, store, hash, attemptOf(t, ctx, store, hash))
		require.NoError(t, err)
		require.True(t, began, "the first caller owns the rollback")

		began, err = beginAsCreator(ctx, store, hash, attemptOf(t, ctx, store, hash))
		require.NoError(t, err)
		require.False(t, began, "a second caller does not own a rollback that is already running")

		bins := recordBins(t, client, store, hash, 0, fields.Creating, fields.RollingBack)
		require.Equal(t, true, bins[fields.RollingBack.String()], "the marker the owner set is untouched")
	})

	t.Run("concurrent claims give exactly one owner", func(t *testing.T) {
		child := spendableChildTx(t, tests.ParentTx, 0)
		child.Version = 31
		hash := child.TxIDChainHash()

		_, err := store.Create(ctx, child, 101, utxo.WithCreating(true))
		require.NoError(t, err)

		const callers = 16

		var (
			owners  atomic.Int32
			wg      sync.WaitGroup
			start   = make(chan struct{})
			errored atomic.Int32
		)

		for range callers {
			wg.Add(1)

			go func() {
				defer wg.Done()

				<-start

				began, callErr := beginAsCreator(ctx, store, hash, attemptOf(t, ctx, store, hash))
				if callErr != nil {
					errored.Add(1)
					return
				}

				if began {
					owners.Add(1)
				}
			}()
		}

		close(start)
		wg.Wait()

		require.Zero(t, errored.Load())
		require.Equal(t, int32(1), owners.Load(), "exactly one caller owns the rollback")
	})

	t.Run("a finalized record is not marked", func(t *testing.T) {
		child := spendableChildTx(t, tests.ParentTx, 2)
		hash := child.TxIDChainHash()

		_, err := store.Create(ctx, child, 101, utxo.WithCreating(true))
		require.NoError(t, err)
		require.NoError(t, store.FinalizeTransaction(ctx, child, attemptOf(t, ctx, store, child.TxIDChainHash())))

		began, err := beginAsCreator(ctx, store, hash, attemptOf(t, ctx, store, hash))
		require.NoError(t, err)
		require.False(t, began)

		bins := recordBins(t, client, store, hash, 0, fields.Creating, fields.RollingBack)
		require.NotContains(t, bins, fields.RollingBack.String(), "nothing is written to a finalized record")

		got := &meta.Data{}
		require.NoError(t, store.GetMeta(ctx, hash, got))
		require.False(t, got.Creating)
		require.False(t, got.RollingBack)
	})

	t.Run("an absent record reports false without creating one", func(t *testing.T) {
		absent := spendableChildTx(t, tests.ParentTx, 0)
		absent.Version = 77

		began, err := beginAsCreator(ctx, store, absent.TxIDChainHash(), attemptOf(t, ctx, store, absent.TxIDChainHash()))
		require.NoError(t, err)
		require.False(t, began)
		require.Nil(t, recordBins(t, client, store, absent.TxIDChainHash(), 0, fields.RollingBack), "UPDATE_ONLY must not create the record")
	})
}

// leaseRollBacker is the store subset rollBackAsCreator needs.
type leaseRollBacker interface {
	utxo.CreatingRollBacker
	ReleaseSpendLease(ctx context.Context, hash *chainhash.Hash, attempt, lease []byte) error
}

// rollBackAsCreator rolls an attempt back the way the call that created it does: it releases the
// spend lease its create took, which is filed under the attempt token, and then runs the rollback.
func rollBackAsCreator(ctx context.Context, logger ulogger.Logger, s leaseRollBacker, tx *bt.Tx, attempt []byte) error {
	if err := s.ReleaseSpendLease(ctx, tx.TxIDChainHash(), attempt, attempt); err != nil {
		return err
	}

	return utxo.RollBackCreating(ctx, logger, s, tx, attempt)
}

// beginAsCreator claims the rollback of an attempt the way the call that created it does: it
// releases the spend lease its create took, which is filed under the attempt token, and then sets
// the marker. A token that names no lease releases nothing.
func beginAsCreator(ctx context.Context, store *teranode_aerospike.Store, hash *chainhash.Hash, attempt []byte) (bool, error) {
	if err := store.ReleaseSpendLease(ctx, hash, attempt, attempt); err != nil {
		return false, err
	}

	return store.BeginCreateFirstRollback(ctx, hash, attempt)
}

// leasesOf returns the spenders bin of the master record of hash, nil when it has none.
func leasesOf(t *testing.T, client *uaerospike.Client, store *teranode_aerospike.Store, hash *chainhash.Hash) map[interface{}]interface{} {
	t.Helper()

	leases, _ := recordBins(t, client, store, hash, 0, fields.Spenders)[fields.Spenders.String()].(map[interface{}]interface{})

	return leases
}

// rolledBackHold creates child in the creating state, marks it as rolling back and ends the
// rollback, which leaves the hold. It returns the token of the attempt that was rolled back.
func rolledBackHold(t *testing.T, ctx context.Context, store *teranode_aerospike.Store, child *bt.Tx) []byte {
	t.Helper()

	hash := child.TxIDChainHash()

	md, err := store.Create(ctx, child, 101, utxo.WithCreating(true))
	require.NoError(t, err)

	began, err := beginAsCreator(ctx, store, hash, md.Attempt)
	require.NoError(t, err)
	require.True(t, began)
	require.NoError(t, store.ReleaseRollbackHold(ctx, hash, md.Attempt))
	require.NoError(t, store.EndCreateFirstRollback(ctx, hash, md.Attempt))
	requireHeld(t, ctx, store, hash, "precondition: the rolled back attempt left a hold")

	return md.Attempt
}

// TestRollbackHold covers the hold a rollback leaves for its txid: who may take it to reverse
// spends, who may clear it, and when it goes.
func TestRollbackHold(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.SpendRollbackTimeout = 2 * time.Second

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)
	cleanDB(t, client)

	_, err := store.Create(ctx, tests.ParentTx, 100)
	require.NoError(t, err)

	t.Run("a take needs the mark, the token and no busy holder, and a release clears only the busy bin", func(t *testing.T) {
		child := spendableChildTx(t, tests.ParentTx, 0)
		hash := child.TxIDChainHash()

		md, err := store.Create(ctx, child, 101, utxo.WithCreating(true))
		require.NoError(t, err)

		took, err := store.TakeRollbackHold(ctx, hash, md.Attempt)
		require.NoError(t, err)
		require.False(t, took, "a record that was never marked as rolling back has nothing to take")

		began, err := beginAsCreator(ctx, store, hash, md.Attempt)
		require.NoError(t, err)
		require.True(t, began)

		took, err = store.TakeRollbackHold(ctx, hash, []byte("another attempt!"))
		require.NoError(t, err)
		require.False(t, took, "a take with another attempt's token is refused")

		took, err = store.TakeRollbackHold(ctx, hash, md.Attempt)
		require.NoError(t, err)
		require.False(t, took, "the caller that marked the record holds the hold, so nobody else can take it")

		require.ErrorContains(t, store.EndCreateFirstRollback(ctx, hash, md.Attempt), "holder is reversing spends", "the end of the rollback must not drop the busy marker of a holder")
		require.NoError(t, store.GetMeta(ctx, hash, &meta.Data{}))
		require.NotContains(t, recordBins(t, client, store, hash, 0, fields.Reversed), fields.Reversed.String(), "a rollback that a holder is still reversing is not recorded as reversed")

		bins := recordBins(t, client, store, hash, 0, fields.Busy, fields.HoldUntil)
		require.Equal(t, true, bins[fields.Busy.String()])
		require.GreaterOrEqual(t, bins[fields.HoldUntil.String()], int(time.Now().Unix())+1, "the take renews holdUntil")

		require.NoError(t, store.ReleaseRollbackHold(ctx, hash, []byte("another attempt!")))
		require.Equal(t, true, recordBins(t, client, store, hash, 0, fields.Busy)[fields.Busy.String()], "a release with another attempt's token leaves the busy bin")

		require.NoError(t, store.ReleaseRollbackHold(ctx, hash, md.Attempt))
		require.NotContains(t, recordBins(t, client, store, hash, 0, fields.Busy), fields.Busy.String())

		took, err = store.TakeRollbackHold(ctx, hash, md.Attempt)
		require.NoError(t, err)
		require.True(t, took, "once the owner released it the attempt that was marked can take its hold again")
		require.NoError(t, store.ReleaseRollbackHold(ctx, hash, md.Attempt))

		require.NoError(t, store.EndCreateFirstRollback(ctx, hash, md.Attempt), "with the busy marker gone the rollback ends")
		requireHeld(t, ctx, store, hash)

		require.NotContains(t, recordBins(t, client, store, hash, 0, fields.Busy), fields.Busy.String(), "the end of the rollback left no busy marker")

		took, err = store.TakeRollbackHold(ctx, hash, []byte("another attempt!"))
		require.NoError(t, err)
		require.False(t, took, "a hold is not taken under another attempt's token")

		took, err = store.TakeRollbackHold(ctx, hash, md.Attempt)
		require.NoError(t, err)
		require.True(t, took, "a hold can be taken, by a caller that must reverse spends it made beside it")

		cleared, err := store.ClearCreateFirstHold(ctx, hash, true)
		require.NoError(t, err)
		require.False(t, cleared, "a busy hold is not adopted, so the holder cannot clear the spends of a replacement")

		took, err = store.TakeRollbackHold(ctx, hash, md.Attempt)
		require.NoError(t, err)
		require.False(t, took, "a busy hold is not taken twice")

		require.NoError(t, store.ReleaseRollbackHold(ctx, hash, md.Attempt))
		requireHeld(t, ctx, store, hash, "the release leaves the hold in place")

		time.Sleep(3 * time.Second)

		require.NoError(t, store.GetMeta(ctx, hash, &meta.Data{}), "the take renewed the hold's time to live, and the release did not remove it")

		time.Sleep(2 * time.Second)

		require.ErrorIs(t, store.GetMeta(ctx, hash, &meta.Data{}), errors.ErrTxNotFound, "the hold still expires on its own after a take and a release")
	})

	t.Run("a rollback that is recorded as reversed cannot be taken", func(t *testing.T) {
		child := spendableChildTx(t, tests.ParentTx, 0)
		child.Version = 17
		hash := child.TxIDChainHash()

		md, err := store.Create(ctx, child, 101, utxo.WithCreating(true))
		require.NoError(t, err)

		began, err := beginAsCreator(ctx, store, hash, md.Attempt)
		require.NoError(t, err)
		require.True(t, began)
		require.NoError(t, store.ReleaseRollbackHold(ctx, hash, md.Attempt))

		writeRawBins(t, client, store, hash, 0, aerospike.NewBin(fields.Reversed.String(), true))

		took, err := store.TakeRollbackHold(ctx, hash, md.Attempt)
		require.NoError(t, err)
		require.False(t, took, "the transaction data may be gone once the reversal is recorded, so a reversal that failed after this take could not be repeated")

		require.NoError(t, store.EndCreateFirstRollback(ctx, hash, md.Attempt))
		requireHeld(t, ctx, store, hash)
	})

	t.Run("a take of an absent record reports false", func(t *testing.T) {
		absent := spendableChildTx(t, tests.ParentTx, 0)
		absent.Version = 78

		took, err := store.TakeRollbackHold(ctx, absent.TxIDChainHash(), []byte("anything"))
		require.NoError(t, err)
		require.False(t, took)
		require.NoError(t, store.ReleaseRollbackHold(ctx, absent.TxIDChainHash(), []byte("anything")))
		require.Nil(t, recordBins(t, client, store, absent.TxIDChainHash(), 0, fields.Busy), "UPDATE_ONLY must not create the record")
	})

	t.Run("a stale claim, take and finalize of the old token touch nothing of the adopted record", func(t *testing.T) {
		child := spendableChildTx(t, tests.ParentTx, 1)
		child.Version = 12
		hash := child.TxIDChainHash()

		old := rolledBackHold(t, ctx, store, child)

		cleared, err := store.ClearCreateFirstHold(ctx, hash, true)
		require.NoError(t, err)
		require.True(t, cleared)

		adopted, err := store.Create(ctx, child, 102, utxo.WithCreating(true))
		require.NoError(t, err)
		require.NotEqual(t, old, adopted.Attempt)

		began, err := beginAsCreator(ctx, store, hash, old)
		require.NoError(t, err)
		require.False(t, began, "a stale roll-forward cannot claim the adopted record")

		took, err := store.TakeRollbackHold(ctx, hash, old)
		require.NoError(t, err)
		require.False(t, took)

		require.ErrorIs(t, store.FinalizeTransaction(ctx, child, old), errors.ErrTxNotFound, "a stale roll-forward cannot finalize the adopted record")

		_, _, err = utxo.RollForwardCreating(ctx, logger, store, child, old, 102)
		require.ErrorIs(t, err, errors.ErrProcessing, "the fence refuses a roll-forward that read an earlier attempt")
		require.Contains(t, err.Error(), "another attempt")

		got := &meta.Data{}
		require.NoError(t, store.GetMeta(ctx, hash, got))
		require.True(t, got.Creating, "the adopted record is untouched")
		require.False(t, got.RollingBack)
		require.False(t, got.Hold)
		require.Equal(t, adopted.Attempt, got.Attempt)
		require.NotContains(t, recordBins(t, client, store, hash, 0, fields.RollingBack, fields.Busy), fields.RollingBack.String())
	})

	t.Run("a hold is cleared at once for a block-context create unless a holder is busy, and otherwise only after it expires", func(t *testing.T) {
		child := spendableChildTx(t, tests.ParentTx, 2)
		child.Version = 13
		hash := child.TxIDChainHash()

		token := rolledBackHold(t, ctx, store, child)

		cleared, err := store.ClearCreateFirstHold(ctx, hash, false)
		require.NoError(t, err)
		require.False(t, cleared, "a live hold is not cleared by a create that is not in block context")
		requireHeld(t, ctx, store, hash)

		setBusy(t, client, store, hash, time.Now().Unix()+1)

		cleared, err = store.ClearCreateFirstHold(ctx, hash, true)
		require.NoError(t, err)
		require.False(t, cleared, "a busy holder is waited for even in block context")
		requireHeld(t, ctx, store, hash)

		require.NoError(t, store.ReleaseRollbackHold(ctx, hash, token))

		cleared, err = store.ClearCreateFirstHold(ctx, hash, true)
		require.NoError(t, err)
		require.True(t, cleared, "a block-context create adopts a hold at once when no holder is busy")
		require.ErrorIs(t, store.GetMeta(ctx, hash, &meta.Data{}), errors.ErrTxNotFound)

		cleared, err = store.ClearCreateFirstHold(ctx, hash, false)
		require.NoError(t, err)
		require.True(t, cleared, "an absent record is free")
	})

	t.Run("a record that is not a hold is never cleared", func(t *testing.T) {
		child := spendableChildTx(t, tests.ParentTx, 0)
		child.Version = 16
		hash := child.TxIDChainHash()

		_, err := store.Create(ctx, child, 101, utxo.WithCreating(true))
		require.NoError(t, err)

		for _, adopt := range []bool{false, true} {
			cleared, err := store.ClearCreateFirstHold(ctx, hash, adopt)
			require.NoError(t, err)
			require.False(t, cleared)
		}

		got := &meta.Data{}
		require.NoError(t, store.GetMeta(ctx, hash, got))
		require.True(t, got.Creating, "a creating record survives every clear")
	})

	t.Run("a hold is found by the sweep query, refuses a clear before its time and then expires on its own", func(t *testing.T) {
		free := spendableChildTx(t, tests.ParentTx, 0)
		free.Version = 14
		hash := free.TxIDChainHash()

		rolledBackHold(t, ctx, store, free)

		stale, err := store.QueryStaleCreatingTxs(ctx, 200, 0)
		require.NoError(t, err)
		require.Contains(t, stale, *hash, "the sweep selects a hold by unminedSince")

		cleared, err := store.ClearCreateFirstHold(ctx, hash, false)
		require.NoError(t, err)
		require.False(t, cleared, "the hold has not expired yet")

		time.Sleep(3 * time.Second)

		require.ErrorIs(t, store.GetMeta(ctx, hash, &meta.Data{}), errors.ErrTxNotFound, "the hold is written with a time to live, so it goes without a sweep")

		stale, err = store.QueryStaleCreatingTxs(ctx, 200, 0)
		require.NoError(t, err)
		require.NotContains(t, stale, *hash, "an expired hold takes no slot of a sweep pass")

		_, err = store.Create(ctx, free, 102, utxo.WithCreating(true))
		require.NoError(t, err, "the txid is free again")
	})
}

// setBusy writes the busy and holdUntil bins of the master record of hash with the raw client, which
// is how the state a holder that crashed before its release leaves is reproduced.
func setBusy(t *testing.T, client *uaerospike.Client, store *teranode_aerospike.Store, hash *chainhash.Hash, holdUntil int64) {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)

	key, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), hash[:])
	require.NoError(t, err)

	policy := util.GetAerospikeWritePolicy(tSettings, 0)
	policy.RecordExistsAction = aerospike.UPDATE_ONLY

	require.Nil(t, client.PutBins(policy, key,
		aerospike.NewBin(fields.Busy.String(), true),
		aerospike.NewBin(fields.HoldUntil.String(), int(holdUntil)),
	))
}

// TestRollbackHold_CrashedHolderAgesOut pins that a busy marker no holder will ever clear does not
// wedge a txid: once holdUntil has passed by a further timeout, a take and an adopting clear
// proceed, and a busy marker inside that window still stops both.
func TestRollbackHold_CrashedHolderAgesOut(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.SpendRollbackTimeout = 2 * time.Second

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)
	cleanDB(t, client)

	_, err := store.Create(ctx, tests.ParentTx, 100)
	require.NoError(t, err)

	t.Run("a take succeeds over a stale busy marker and not over a live one", func(t *testing.T) {
		child := spendableChildTx(t, tests.ParentTx, 0)
		child.Version = 21
		hash := child.TxIDChainHash()

		md, err := store.Create(ctx, child, 101, utxo.WithCreating(true))
		require.NoError(t, err)

		began, err := beginAsCreator(ctx, store, hash, md.Attempt)
		require.NoError(t, err)
		require.True(t, began)

		now := time.Now().Unix()

		setBusy(t, client, store, hash, now+1)

		took, err := store.TakeRollbackHold(ctx, hash, md.Attempt)
		require.NoError(t, err)
		require.False(t, took, "a busy marker inside the hold is a live holder's")

		setBusy(t, client, store, hash, now-1)

		took, err = store.TakeRollbackHold(ctx, hash, md.Attempt)
		require.NoError(t, err)
		require.False(t, took, "a hold that has expired once is not yet a crashed holder's")

		setBusy(t, client, store, hash, now-10)

		took, err = store.TakeRollbackHold(ctx, hash, md.Attempt)
		require.NoError(t, err)
		require.True(t, took, "a busy marker that holdUntil passed by a further timeout is a crashed holder's")

		took, err = store.TakeRollbackHold(ctx, hash, md.Attempt)
		require.NoError(t, err)
		require.False(t, took, "the take renewed holdUntil, so the new holder is live")
	})

	t.Run("the rollback of a record a holder crashed on is finished by the next holder", func(t *testing.T) {
		child := spendableChildTx(t, tests.ParentTx, 1)
		child.Version = 22
		hash := child.TxIDChainHash()

		md, err := store.Create(ctx, child, 101, utxo.WithCreating(true))
		require.NoError(t, err)

		_, err = store.Spend(ctx, child, 101)
		require.NoError(t, err)

		inputs, err := utxo.GetSpends(child)
		require.NoError(t, err)

		began, err := beginAsCreator(ctx, store, hash, md.Attempt)
		require.NoError(t, err)
		require.True(t, began)

		setBusy(t, client, store, hash, time.Now().Unix())
		require.ErrorContains(t, store.EndCreateFirstRollback(ctx, hash, md.Attempt), "holder is reversing spends")

		setBusy(t, client, store, hash, time.Now().Unix()-10)

		took, err := store.TakeRollbackHold(ctx, hash, md.Attempt)
		require.NoError(t, err)
		require.True(t, took, "the sweep's take succeeds over the crashed holder's marker")

		require.NoError(t, utxo.UnspendTolerantOfMissingParents(ctx, store, inputs))
		require.NoError(t, store.ReleaseRollbackHold(ctx, hash, md.Attempt))
		require.NoError(t, store.EndCreateFirstRollback(ctx, hash, md.Attempt))
		requireHeld(t, ctx, store, hash)

		resp, err := store.GetSpend(ctx, inputs[0])
		require.NoError(t, err)
		require.Nil(t, resp.SpendingData, "the input is unspent")
	})

	t.Run("a reversed rollback is converted over the stale busy marker of a holder that crashed", func(t *testing.T) {
		child := spendableChildTx(t, tests.ParentTx, 0)
		child.Version = 25
		hash := child.TxIDChainHash()

		md, err := store.Create(ctx, child, 101, utxo.WithCreating(true))
		require.NoError(t, err)

		began, err := beginAsCreator(ctx, store, hash, md.Attempt)
		require.NoError(t, err)
		require.True(t, began)

		writeRawBins(t, client, store, hash, 0, aerospike.NewBin(fields.Reversed.String(), true))
		setBusy(t, client, store, hash, time.Now().Unix()-10)

		require.NoError(t, store.EndCreateFirstRollback(ctx, hash, md.Attempt), "the stale marker does not strand a reversed record")
		requireHeld(t, ctx, store, hash)

		live := spendableChildTx(t, tests.ParentTx, 0)
		live.Version = 26
		liveHash := live.TxIDChainHash()

		liveMd, err := store.Create(ctx, live, 101, utxo.WithCreating(true))
		require.NoError(t, err)

		began, err = beginAsCreator(ctx, store, liveHash, liveMd.Attempt)
		require.NoError(t, err)
		require.True(t, began)

		writeRawBins(t, client, store, liveHash, 0, aerospike.NewBin(fields.Reversed.String(), true))
		setBusy(t, client, store, liveHash, time.Now().Unix())

		require.ErrorContains(t, store.EndCreateFirstRollback(ctx, liveHash, liveMd.Attempt), "holder is reversing spends", "a live marker still stops the conversion")
	})

	t.Run("an adopting clear removes a hold whose busy marker is stale and not one whose marker is live", func(t *testing.T) {
		stale := spendableChildTx(t, tests.ParentTx, 2)
		stale.Version = 23
		live := spendableChildTx(t, tests.ParentTx, 2)
		live.Version = 24

		rolledBackHold(t, ctx, store, stale)
		rolledBackHold(t, ctx, store, live)

		now := time.Now().Unix()

		setBusy(t, client, store, live.TxIDChainHash(), now+1)
		setBusy(t, client, store, stale.TxIDChainHash(), now-10)

		cleared, err := store.ClearCreateFirstHold(ctx, live.TxIDChainHash(), true)
		require.NoError(t, err)
		require.False(t, cleared, "a live holder is waited for")
		requireHeld(t, ctx, store, live.TxIDChainHash())

		cleared, err = store.ClearCreateFirstHold(ctx, stale.TxIDChainHash(), true)
		require.NoError(t, err)
		require.True(t, cleared, "a crashed holder does not stop a block-context create")
		require.ErrorIs(t, store.GetMeta(ctx, stale.TxIDChainHash(), &meta.Data{}), errors.ErrTxNotFound)
	})
}

// TestSpendLease covers the lease every spender of a creating record's inputs holds: the create
// writes the creator's, a roll-forward takes its own before it spends, the rolling back marker
// refuses a new one, and the owner of a rollback sees a lease as live until it is released or
// stale.
func TestSpendLease(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.SpendRollbackTimeout = 2 * time.Second

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)
	cleanDB(t, client)

	_, err := store.Create(ctx, tests.ParentTx, 100)
	require.NoError(t, err)

	t.Run("the create writes the creator's lease, a take adds one and a release removes it", func(t *testing.T) {
		child := spendableChildTx(t, tests.ParentTx, 0)
		child.Version = 31
		hash := child.TxIDChainHash()

		md, err := store.Create(ctx, child, 101, utxo.WithCreating(true))
		require.NoError(t, err)

		creator := hex.EncodeToString(md.Attempt)
		require.Contains(t, leasesOf(t, client, store, hash), creator, "the creator holds a lease under its attempt token from the create on")

		live, err := store.SpendLeasesLive(ctx, hash, md.Attempt)
		require.NoError(t, err)
		require.True(t, live)

		lease, took, err := store.TakeSpendLease(ctx, hash, []byte("another attempt!"))
		require.NoError(t, err)
		require.False(t, took, "a lease for another attempt's token is refused")
		require.Nil(t, lease)

		lease, took, err = store.TakeSpendLease(ctx, hash, md.Attempt)
		require.NoError(t, err)
		require.True(t, took)
		require.Len(t, leasesOf(t, client, store, hash), 2)

		require.NoError(t, store.ReleaseSpendLease(ctx, hash, md.Attempt, md.Attempt), "the creator releases its lease")

		live, err = store.SpendLeasesLive(ctx, hash, md.Attempt)
		require.NoError(t, err)
		require.True(t, live, "the roll-forward's lease is still live")

		require.NoError(t, store.ReleaseSpendLease(ctx, hash, []byte("another attempt!"), lease))
		require.Len(t, leasesOf(t, client, store, hash), 1, "a release under another attempt's token removes nothing")

		require.NoError(t, store.ReleaseSpendLease(ctx, hash, md.Attempt, lease))
		require.Empty(t, leasesOf(t, client, store, hash))

		live, err = store.SpendLeasesLive(ctx, hash, md.Attempt)
		require.NoError(t, err)
		require.False(t, live)

		require.NoError(t, store.ReleaseSpendLease(ctx, hash, md.Attempt, lease), "a second release is a no-op")
	})

	t.Run("a live lease refuses the rolling back marker, and the marker refuses a new lease", func(t *testing.T) {
		child := spendableChildTx(t, tests.ParentTx, 0)
		child.Version = 32
		hash := child.TxIDChainHash()

		md, err := store.Create(ctx, child, 101, utxo.WithCreating(true))
		require.NoError(t, err)

		began, err := store.BeginCreateFirstRollback(ctx, hash, md.Attempt)
		require.False(t, began)
		require.True(t, utxo.IsSpendLeaseLive(err), "the creator's lease is live, so no rollback begins, got: %v", err)
		require.NotContains(t, recordBins(t, client, store, hash, 0, fields.RollingBack, fields.Busy), fields.RollingBack.String(), "a refused claim writes nothing")

		lease, took, err := store.TakeSpendLease(ctx, hash, md.Attempt)
		require.NoError(t, err)
		require.True(t, took, "the record is still creating, so a roll-forward can take a lease")

		require.NoError(t, store.ReleaseSpendLease(ctx, hash, md.Attempt, md.Attempt))

		began, err = store.BeginCreateFirstRollback(ctx, hash, md.Attempt)
		require.False(t, began)
		require.True(t, utxo.IsSpendLeaseLive(err), "the roll-forward's lease is live, got: %v", err)

		require.NoError(t, store.ReleaseSpendLease(ctx, hash, md.Attempt, lease))

		began, err = store.BeginCreateFirstRollback(ctx, hash, md.Attempt)
		require.NoError(t, err)
		require.True(t, began, "with no live lease the rollback begins")

		_, took, err = store.TakeSpendLease(ctx, hash, md.Attempt)
		require.NoError(t, err)
		require.False(t, took, "a spender that asks after the marker never starts")
	})

	t.Run("a stale lease does not refuse the rolling back marker", func(t *testing.T) {
		child := spendableChildTx(t, tests.ParentTx, 0)
		child.Version = 36
		hash := child.TxIDChainHash()

		md, err := store.Create(ctx, child, 101, utxo.WithCreating(true))
		require.NoError(t, err)

		now := time.Now().Unix()

		writeRawBins(t, client, store, hash, 0, aerospike.NewBin(fields.Spenders.String(), map[interface{}]interface{}{"crashed": int(now - 1)}))

		began, err := store.BeginCreateFirstRollback(ctx, hash, md.Attempt)
		require.False(t, began)
		require.True(t, utxo.IsSpendLeaseLive(err), "a lease whose time has just passed is still live, got: %v", err)

		writeRawBins(t, client, store, hash, 0, aerospike.NewBin(fields.Spenders.String(), map[interface{}]interface{}{"crashed": int(now - 10)}))

		began, err = store.BeginCreateFirstRollback(ctx, hash, md.Attempt)
		require.NoError(t, err)
		require.True(t, began, "a crashed spender's lease does not wedge the record")
	})

	t.Run("an absent record and a final one give no lease", func(t *testing.T) {
		absent := spendableChildTx(t, tests.ParentTx, 0)
		absent.Version = 33

		_, took, err := store.TakeSpendLease(ctx, absent.TxIDChainHash(), []byte("anything"))
		require.NoError(t, err)
		require.False(t, took)
		require.Nil(t, recordBins(t, client, store, absent.TxIDChainHash(), 0, fields.Spenders), "UPDATE_ONLY must not create the record")

		live, err := store.SpendLeasesLive(ctx, absent.TxIDChainHash(), []byte("anything"))
		require.NoError(t, err)
		require.False(t, live)

		final := spendableChildTx(t, tests.ParentTx, 0)
		final.Version = 34

		md, err := store.Create(ctx, final, 101, utxo.WithCreating(true))
		require.NoError(t, err)
		require.NoError(t, store.FinalizeTransaction(ctx, final, md.Attempt))
		require.NotContains(t, recordBins(t, client, store, final.TxIDChainHash(), 0, fields.Spenders), fields.Spenders.String(), "the finalize removes the leases with the creating state")

		_, took, err = store.TakeSpendLease(ctx, final.TxIDChainHash(), md.Attempt)
		require.NoError(t, err)
		require.False(t, took)
	})

	t.Run("a lease that was never released is live until it is stale, and a take drops stale ones", func(t *testing.T) {
		child := spendableChildTx(t, tests.ParentTx, 0)
		child.Version = 35
		hash := child.TxIDChainHash()

		md, err := store.Create(ctx, child, 101, utxo.WithCreating(true))
		require.NoError(t, err)

		now := time.Now().Unix()

		writeRawBins(t, client, store, hash, 0, aerospike.NewBin(fields.Spenders.String(), map[interface{}]interface{}{"crashed": int(now - 1)}))

		live, err := store.SpendLeasesLive(ctx, hash, md.Attempt)
		require.NoError(t, err)
		require.True(t, live, "a spend that ran out of time can still land for a further timeout")

		writeRawBins(t, client, store, hash, 0, aerospike.NewBin(fields.Spenders.String(), map[interface{}]interface{}{"crashed": int(now - 10)}))

		live, err = store.SpendLeasesLive(ctx, hash, md.Attempt)
		require.NoError(t, err)
		require.False(t, live, "a lease that its time passed by a further timeout is a crashed spender's")

		lease, took, err := store.TakeSpendLease(ctx, hash, md.Attempt)
		require.NoError(t, err)
		require.True(t, took)

		leases := leasesOf(t, client, store, hash)
		require.NotContains(t, leases, "crashed", "the take removed the stale lease")
		require.Contains(t, leases, hex.EncodeToString(lease))
	})
}

// TestEndCreateFirstRollback_OwnsItsCleanup pins that the end of a rollback is exclusive from the
// write that records the reversal to the write that leaves the hold. A second caller that arrives
// while the first removes the children and blobs must not run that cleanup too: it could finish
// later than the hold, and then delete the blobs of an attempt that replaced it.
func TestEndCreateFirstRollback_OwnsItsCleanup(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.SpendRollbackTimeout = 2 * time.Second

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)
	cleanDB(t, client)

	_, err := store.Create(ctx, tests.ParentTx, 100)
	require.NoError(t, err)

	child := spendableChildTx(t, tests.ParentTx, 0)
	child.Version = 41
	hash := child.TxIDChainHash()

	md, err := store.Create(ctx, child, 101, utxo.WithCreating(true))
	require.NoError(t, err)

	began, err := beginAsCreator(ctx, store, hash, md.Attempt)
	require.NoError(t, err)
	require.True(t, began)
	require.NoError(t, store.ReleaseRollbackHold(ctx, hash, md.Attempt))

	// The state the first caller's opening write leaves while its cleanup is in flight.
	writeRawBins(t, client, store, hash, 0, aerospike.NewBin(fields.Reversed.String(), true))
	setBusy(t, client, store, hash, time.Now().Unix()+2)

	require.ErrorContains(t, store.EndCreateFirstRollback(ctx, hash, md.Attempt), "removing its records", "a second caller does not run the cleanup beside the first")

	got := &meta.Data{}
	require.NoError(t, store.GetMeta(ctx, hash, got))
	require.False(t, got.Hold, "the refused caller did not convert the record")
	require.Equal(t, true, recordBins(t, client, store, hash, 0, fields.Busy)[fields.Busy.String()], "the refused caller left the first caller's marker in place")

	require.NoError(t, store.ReleaseRollbackHold(ctx, hash, md.Attempt), "the first caller's cleanup failed and released its marker")
	require.NoError(t, store.EndCreateFirstRollback(ctx, hash, md.Attempt), "a retry then owns the cleanup and ends the rollback")
	requireHeld(t, ctx, store, hash)
	require.NotContains(t, recordBins(t, client, store, hash, 0, fields.Busy, fields.Spenders), fields.Busy.String(), "the hold carries no busy marker")
}

// unspendBeforeCreateStore reverses the given spends once, just before the first Create reaches the
// store. It places the reversal of a rollback owner between a spend-first caller's spend and its
// create. Every store call is the real store's.
type unspendBeforeCreateStore struct {
	*teranode_aerospike.Store

	reverse []*utxo.Spend
}

func (s *unspendBeforeCreateStore) Create(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...utxo.CreateOption) (*meta.Data, error) {
	if reverse := s.reverse; reverse != nil {
		s.reverse = nil

		if err := s.Store.Unspend(ctx, reverse); err != nil {
			return nil, err
		}
	}

	return s.Store.Create(ctx, tx, blockHeight, opts...)
}

// adoptDuringUnspendStore tries to adopt the hold of hash, as a create in block context does, once,
// just before the first Unspend reaches the store. Every store call is the real store's.
type adoptDuringUnspendStore struct {
	*teranode_aerospike.Store

	hash    *chainhash.Hash
	tried   bool
	adopted bool
	err     error
}

func (s *adoptDuringUnspendStore) Unspend(ctx context.Context, spends []*utxo.Spend, flagAsLocked ...bool) error {
	if !s.tried {
		s.tried = true
		s.adopted, s.err = s.Store.ClearCreateFirstHold(ctx, s.hash, true)
	}

	return s.Store.Unspend(ctx, spends, flagAsLocked...)
}

// TestSpendFirstBesideARollback pins the path a node takes after utxostore_useCreateFirstOrder is
// turned off while the records of create-first rollbacks remain. A spend-first SpendAndCreate takes
// no spend lease, and a spend is the txid and the input index, so the reversal of the rollback and
// the reversal of the spend-first caller can each clear the other's spends. The caller therefore
// never creates over spends it made before a hold was gone, and it reverses its own spends only
// under the hold.
func TestSpendFirstBesideARollback(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.UseCreateFirstOrder = false

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)
	cleanDB(t, client)

	_, err := store.Create(ctx, tests.ParentTx, 100)
	require.NoError(t, err)

	blockContext := utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 102, SubtreeIdx: 0})

	t.Run("a hold that stands: the spends are reversed under the hold and no create can adopt it meanwhile", func(t *testing.T) {
		child := spendableChildTx(t, tests.ParentTx, 0)
		child.Version = 51
		hash := child.TxIDChainHash()

		inputs, err := utxo.GetSpends(child)
		require.NoError(t, err)

		rolledBackHold(t, ctx, store, child)

		caller := &adoptDuringUnspendStore{Store: store, hash: hash}

		md, spends, err := utxo.SequentialSpendAndCreate(ctx, logger, caller, child, 102, false)
		require.ErrorIs(t, err, errors.ErrProcessing, "a hold that stands is a retryable failure")
		require.NotErrorIs(t, err, errors.ErrTxExists, "a hold is not a stored transaction")
		require.Nil(t, md)
		require.Nil(t, spends, "the spends are no longer in effect")

		require.True(t, caller.tried, "precondition: the caller reversed its spends")
		require.NoError(t, caller.err)
		require.False(t, caller.adopted, "a block-context create cannot adopt the hold while the caller reverses under it, so the caller cannot clear the spends of a replacement")

		resp, err := store.GetSpend(ctx, inputs[0])
		require.NoError(t, err)
		require.Nil(t, resp.SpendingData, "the input must not stay spent by a txid that is only a hold")
		requireHeld(t, ctx, store, hash, "the hold stands")
		require.NotContains(t, recordBins(t, client, store, hash, 0, fields.Busy), fields.Busy.String(), "the caller released the hold")
	})

	t.Run("a hold in block context: the caller spends again after it cleared the hold", func(t *testing.T) {
		child := spendableChildTx(t, tests.ParentTx, 1)
		child.Version = 52
		hash := child.TxIDChainHash()

		inputs, err := utxo.GetSpends(child)
		require.NoError(t, err)

		rolledBackHold(t, ctx, store, child)

		// The reversal of the rollback lands after the caller's first spend and before its create.
		caller := &unspendBeforeCreateStore{Store: store, reverse: inputs}

		md, spends, err := utxo.SequentialSpendAndCreate(ctx, logger, caller, child, 102, false, blockContext)
		require.NoError(t, err, "block context adopts the hold and creates")
		require.NotNil(t, md)
		require.Len(t, spends, 1)

		got := &meta.Data{}
		require.NoError(t, store.GetMeta(ctx, hash, got))
		require.False(t, got.Hold)
		require.False(t, got.Creating, "the record is a stored transaction")

		resp, err := store.GetSpend(ctx, inputs[0])
		require.NoError(t, err)
		require.NotNil(t, resp.SpendingData, "a stored transaction must not have an input that reads unspent")
		require.Equal(t, hash.String(), resp.SpendingData.TxID.String())
	})

	t.Run("an open rollback: the spends are reversed under its hold", func(t *testing.T) {
		child := spendableChildTx(t, tests.ParentTx, 2)
		child.Version = 53
		hash := child.TxIDChainHash()

		inputs, err := utxo.GetSpends(child)
		require.NoError(t, err)

		md, err := store.Create(ctx, child, 101, utxo.WithCreating(true))
		require.NoError(t, err)

		began, err := beginAsCreator(ctx, store, hash, md.Attempt)
		require.NoError(t, err)
		require.True(t, began)
		require.NoError(t, store.ReleaseRollbackHold(ctx, hash, md.Attempt), "the owner reversed its spends and stopped before it ended the rollback")

		_, spends, err := store.SpendAndCreate(ctx, child, 102)
		require.ErrorIs(t, err, errors.ErrProcessing)
		require.NotErrorIs(t, err, errors.ErrTxExists)
		require.Nil(t, spends)

		resp, err := store.GetSpend(ctx, inputs[0])
		require.NoError(t, err)
		require.Nil(t, resp.SpendingData, "the rollback reverses the inputs once, so a spend made beside it must not stay")
		require.NotContains(t, recordBins(t, client, store, hash, 0, fields.Busy), fields.Busy.String())
	})
}

// TestSpendBesideAHold_PartialSpendIsReversed pins the store's own rollback of a partial spend when
// the spender's txid carries a hold. The hold is a record, but not a spender: it carries no
// transaction and expires. A spend that took one input and failed on another must not leave that
// input naming the hold.
func TestSpendBesideAHold_PartialSpendIsReversed(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.UseCreateFirstOrder = false

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)
	cleanDB(t, client)

	_, err := store.Create(ctx, tests.ParentTx, 100)
	require.NoError(t, err)

	winner := spendableChildTx(t, tests.ParentTx, 1)
	_, err = store.Spend(ctx, winner, 101, utxo.IgnoreFlags{SkipUTXOHashCheck: true})
	require.NoError(t, err)

	loser := spendableChildTx(t, tests.ParentTx, 1)
	require.NoError(t, loser.From(
		tests.ParentTx.TxIDChainHash().String(), 2,
		tests.ParentTx.Outputs[2].LockingScript.String(),
		tests.ParentTx.Outputs[2].Satoshis,
	))
	loser.Version = 54
	hash := loser.TxIDChainHash()

	inputs, err := utxo.GetSpends(loser)
	require.NoError(t, err)

	rolledBackHold(t, ctx, store, loser)

	_, _, err = store.SpendAndCreate(ctx, loser, 102)
	require.ErrorIs(t, err, errors.ErrSpent, "the spend fails on the input the rival took")

	resp, err := store.GetSpend(ctx, inputs[1])
	require.NoError(t, err)
	require.Nil(t, resp.SpendingData, "the free input must not stay spent by a txid that is only a hold")

	resp, err = store.GetSpend(ctx, inputs[0])
	require.NoError(t, err)
	require.NotNil(t, resp.SpendingData)
	require.Equal(t, winner.TxIDChainHash().String(), resp.SpendingData.TxID.String(), "the rival's spend is intact")

	requireHeld(t, ctx, store, hash)
	require.NotContains(t, recordBins(t, client, store, hash, 0, fields.Busy), fields.Busy.String(), "the store released the hold after its rollback")
}
