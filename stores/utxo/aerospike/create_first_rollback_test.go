package aerospike_test

import (
	"context"
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

		began, err := store.BeginCreateFirstRollback(ctx, hash, attemptOf(t, ctx, store, hash))
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

		began, err := store.BeginCreateFirstRollback(ctx, hash, attemptOf(t, ctx, store, hash))
		require.NoError(t, err)
		require.True(t, began, "the first caller owns the rollback")

		began, err = store.BeginCreateFirstRollback(ctx, hash, attemptOf(t, ctx, store, hash))
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

				began, callErr := store.BeginCreateFirstRollback(ctx, hash, attemptOf(t, ctx, store, hash))
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

		began, err := store.BeginCreateFirstRollback(ctx, hash, attemptOf(t, ctx, store, hash))
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

		began, err := store.BeginCreateFirstRollback(ctx, absent.TxIDChainHash(), attemptOf(t, ctx, store, absent.TxIDChainHash()))
		require.NoError(t, err)
		require.False(t, began)
		require.Nil(t, recordBins(t, client, store, absent.TxIDChainHash(), 0, fields.RollingBack), "UPDATE_ONLY must not create the record")
	})
}

// rolledBackHold creates child in the creating state, marks it as rolling back and ends the
// rollback, which leaves the hold. It returns the token of the attempt that was rolled back.
func rolledBackHold(t *testing.T, ctx context.Context, store *teranode_aerospike.Store, child *bt.Tx) []byte {
	t.Helper()

	hash := child.TxIDChainHash()

	md, err := store.Create(ctx, child, 101, utxo.WithCreating(true))
	require.NoError(t, err)

	began, err := store.BeginCreateFirstRollback(ctx, hash, md.Attempt)
	require.NoError(t, err)
	require.True(t, began)
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

		began, err := store.BeginCreateFirstRollback(ctx, hash, md.Attempt)
		require.NoError(t, err)
		require.True(t, began)

		took, err = store.TakeRollbackHold(ctx, hash, []byte("another attempt!"))
		require.NoError(t, err)
		require.False(t, took, "a take with another attempt's token is refused")

		took, err = store.TakeRollbackHold(ctx, hash, md.Attempt)
		require.NoError(t, err)
		require.True(t, took, "the attempt that was marked can take its hold, which is marked busy while the record is still rolling back")

		took, err = store.TakeRollbackHold(ctx, hash, md.Attempt)
		require.NoError(t, err)
		require.False(t, took, "a busy hold is taken by one actor only")

		require.ErrorContains(t, store.EndCreateFirstRollback(ctx, hash, md.Attempt), "holder is reversing spends", "the end of the rollback must not drop the busy marker of a holder")
		require.NoError(t, store.GetMeta(ctx, hash, &meta.Data{}))

		bins := recordBins(t, client, store, hash, 0, fields.Busy, fields.HoldUntil)
		require.Equal(t, true, bins[fields.Busy.String()])
		require.GreaterOrEqual(t, bins[fields.HoldUntil.String()], int(time.Now().Unix())+1, "the take renews holdUntil")

		require.NoError(t, store.ReleaseRollbackHold(ctx, hash, []byte("another attempt!")))
		require.Equal(t, true, recordBins(t, client, store, hash, 0, fields.Busy)[fields.Busy.String()], "a release with another attempt's token leaves the busy bin")

		require.NoError(t, store.ReleaseRollbackHold(ctx, hash, md.Attempt))
		require.NotContains(t, recordBins(t, client, store, hash, 0, fields.Busy), fields.Busy.String())

		require.NoError(t, store.EndCreateFirstRollback(ctx, hash, md.Attempt), "with the busy marker gone the rollback ends")
		requireHeld(t, ctx, store, hash)

		took, err = store.TakeRollbackHold(ctx, hash, md.Attempt)
		require.NoError(t, err)
		require.True(t, took, "a hold can be taken too, and the take renews it")
		require.NoError(t, store.ReleaseRollbackHold(ctx, hash, md.Attempt))
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

		began, err := store.BeginCreateFirstRollback(ctx, hash, old)
		require.NoError(t, err)
		require.False(t, began, "a stale roll-forward cannot claim the adopted record")

		took, err := store.TakeRollbackHold(ctx, hash, old)
		require.NoError(t, err)
		require.False(t, took)

		require.ErrorIs(t, store.FinalizeTransaction(ctx, child, old), errors.ErrTxNotFound, "a stale roll-forward cannot finalize the adopted record")

		_, err = utxo.RollForwardCreating(ctx, logger, store, child, old, 102)
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

		took, err := store.TakeRollbackHold(ctx, hash, token)
		require.NoError(t, err)
		require.True(t, took)

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

	t.Run("an expired hold is cleared, a busy one only after a further expiry, and the sweep query finds it", func(t *testing.T) {
		free := spendableChildTx(t, tests.ParentTx, 0)
		free.Version = 14
		busy := spendableChildTx(t, tests.ParentTx, 0)
		busy.Version = 15

		rolledBackHold(t, ctx, store, free)
		busyToken := rolledBackHold(t, ctx, store, busy)

		took, err := store.TakeRollbackHold(ctx, busy.TxIDChainHash(), busyToken)
		require.NoError(t, err)
		require.True(t, took)

		stale, err := store.QueryStaleCreatingTxs(ctx, 200, 0)
		require.NoError(t, err)
		require.Contains(t, stale, *free.TxIDChainHash(), "the sweep selects a hold by unminedSince")
		require.Contains(t, stale, *busy.TxIDChainHash())

		cleared, err := store.ClearCreateFirstHold(ctx, free.TxIDChainHash(), false)
		require.NoError(t, err)
		require.False(t, cleared, "the hold has not expired yet")

		time.Sleep(3 * time.Second)

		cleared, err = store.ClearCreateFirstHold(ctx, free.TxIDChainHash(), false)
		require.NoError(t, err)
		require.True(t, cleared, "an expired hold with no busy holder is deleted")
		require.ErrorIs(t, store.GetMeta(ctx, free.TxIDChainHash(), &meta.Data{}), errors.ErrTxNotFound)

		cleared, err = store.ClearCreateFirstHold(ctx, busy.TxIDChainHash(), false)
		require.NoError(t, err)
		require.False(t, cleared, "a busy hold survives its first expiry")
		requireHeld(t, ctx, store, busy.TxIDChainHash())

		time.Sleep(2 * time.Second)

		cleared, err = store.ClearCreateFirstHold(ctx, busy.TxIDChainHash(), false)
		require.NoError(t, err)
		require.True(t, cleared, "a hold whose holder crashed is deleted once it has expired by a further timeout")
		require.ErrorIs(t, store.GetMeta(ctx, busy.TxIDChainHash(), &meta.Data{}), errors.ErrTxNotFound)
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

		began, err := store.BeginCreateFirstRollback(ctx, hash, md.Attempt)
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

		began, err := store.BeginCreateFirstRollback(ctx, hash, md.Attempt)
		require.NoError(t, err)
		require.True(t, began)

		setBusy(t, client, store, hash, time.Now().Unix()-10)
		require.ErrorContains(t, store.EndCreateFirstRollback(ctx, hash, md.Attempt), "holder is reversing spends")

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
