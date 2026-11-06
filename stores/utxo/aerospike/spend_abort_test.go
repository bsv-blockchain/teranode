package aerospike_test

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	teranode_aerospike "github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestStore_Spend_AbortReturnsNilSlice exercises the group.Wait timeout/abort
// branch of Store.Spend under the race detector. Two guarantees are asserted:
//
//  1. Spend returns a NIL spends slice on abort — it must not hand back the
//     live slice whose *utxo.Spend elements the dispatcher goroutine may still
//     be writing (running on the store ctx, not the caller ctx). Returning the
//     live slice was a data race and could misclassify an in-flight, still
//     Err==nil item as a successful spend.
//
//  2. Run with -race, the internal resolveSpendCompletions(onlyCompleted=true)
//     pass that computes abort-time rollback must not race the dispatcher: it
//     reads a slot only after observing that item's published flag.
func TestStore_Spend_AbortReturnsNilSlice(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(func() {
		deferFn()
	})

	cleanDB(t, client)

	// Create the parent so spendTx's referenced UTXOs exist and are spendable.
	_, err := store.Create(ctx, tx, 101)
	require.NoError(t, err)

	// An extremely short wait forces group.Wait to abort before the batcher can
	// respond, taking the timeout branch while the dispatcher is still in flight.
	tSettings.UtxoStore.SpendWaitTimeout = time.Nanosecond

	spends, err := store.Spend(ctx, spendTx, 101)
	require.Error(t, err)
	require.True(t,
		errors.Is(err, errors.ErrServiceUnavailable) || errors.Is(err, errors.ErrContextCanceled),
		"abort must surface a service-unavailable/context error, got: %v", err)
	require.Nil(t, spends, "Spend must not return the live spends slice on the abort path")
}

// A claim that started before its caller gave up and finishes after it must
// settle before the caller returns, and its spend must be rolled back, because
// the caller got an error and no spender record is ever created. The second
// shape is a claim with the wrong UTXO hash ahead of a claim with the right
// hash, so the right claim runs in a later backend part.
func TestSpendAwaitsAdmittedClaimThatFinishesAfterCallerGaveUp(t *testing.T) {
	for _, expressions := range []bool{false, true} {
		for _, shape := range []string{"single claim", "wrong hash claim first"} {
			for _, cancelCaller := range []bool{false, true} {
				name := shape + " lua"
				if expressions {
					name = shape + " expressions"
				}

				wantClass := errors.ErrServiceUnavailable
				if cancelCaller {
					name += " cancellation"
					wantClass = errors.ErrContextCanceled
				} else {
					name += " timeout"
				}

				t.Run(name, func(t *testing.T) {
					tSettings := test.CreateBaseTestSettings(t)
					tSettings.UtxoStore.SpendWaitTimeout = 300 * time.Millisecond
					tSettings.Aerospike.EnableSpendFilterExpressions = expressions

					client, store, storeCtx, deferFn := initAerospike(t, tSettings, ulogger.NewErrorTestLogger(t))
					t.Cleanup(deferFn)
					cleanDB(t, client)
					require.NoError(t, store.SetBlockHeight(2))

					parent := bt.NewTx()
					prev := chainhash.DoubleHashH([]byte("late-" + name))
					require.NoError(t, parent.From(prev.String(), 0, "76a91471d7dd96d9edda09180fe9d57a477b5acc9cad1188ac", 1e8))
					parent.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{bscript.OpTRUE})
					require.NoError(t, parent.PayToAddress("1BUBHgpCBWCsD2Ajbeu7ZgCgvMHsoYHm3W", 1000))
					_, err := store.Create(storeCtx, parent, 1)
					require.NoError(t, err)

					laterTx := bt.NewTx()
					require.NoError(t, laterTx.From(parent.TxID(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
					require.NoError(t, laterTx.PayToAddress("1BUBHgpCBWCsD2Ajbeu7ZgCgvMHsoYHm3W", 1))
					laterTx.LockTime = 2

					var before []*utxo.Spend

					if shape == "wrong hash claim first" {
						wrongHash := chainhash.Hash{99}
						owner := chainhash.Hash{7}
						before = []*utxo.Spend{{
							TxID: parent.TxIDChainHash(), Vout: 0, UTXOHash: &wrongHash,
							SpendingData: spendpkg.NewSpendingData(&owner, 0),
						}}
					}

					callerCtx, cancel := context.WithCancel(storeCtx)
					defer cancel()

					inFlight := teranode_aerospike.SpendAbortInFlightForTest(t)
					entered, release := make(chan struct{}), make(chan struct{})

					result, done, err := store.SpendHeldForTest(callerCtx, laterTx, 2, before, entered, release)
					require.NoError(t, err)

					select {
					case <-entered:
					case <-time.After(5 * time.Second):
						t.Fatal("claim part did not start")
					}

					if cancelCaller {
						cancel()
					}

					select {
					case err := <-result:
						t.Fatalf("caller returned %v while its admitted claim was still running", err)
					case <-time.After(800 * time.Millisecond):
					}

					close(release)

					select {
					case err := <-result:
						require.ErrorIs(t, err, wantClass)
					case <-time.After(10 * time.Second):
						t.Fatal("caller did not return after the claim settled")
					}

					select {
					case <-done:
					case <-time.After(5 * time.Second):
						t.Fatal("batch did not complete")
					}

					laterSpends, err := utxo.GetSpends(laterTx)
					require.NoError(t, err)

					stored, err := store.GetSpend(storeCtx, laterSpends[0])
					require.NoError(t, err)
					require.Equal(t, int(utxo.Status_OK), stored.Status, "the late claim's spend was left on the outpoint")
					require.Nil(t, stored.SpendingData)
					require.Equal(t, inFlight, teranode_aerospike.SpendAbortInFlightForTest(t), "a claim that settled inside the bound must not count as in flight")
				})
			}
		}
	}
}

func prepareHeldSpend(t *testing.T, name string, wait, bound time.Duration) (store *teranode_aerospike.Store, storeCtx context.Context, laterTx *bt.Tx, before []*utxo.Spend) {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.SpendWaitTimeout = wait
	tSettings.UtxoStore.SpendRollbackTimeout = bound

	client, store, storeCtx, deferFn := initAerospike(t, tSettings, ulogger.NewErrorTestLogger(t))
	t.Cleanup(deferFn)
	cleanDB(t, client)
	require.NoError(t, store.SetBlockHeight(2))

	parent := bt.NewTx()
	prev := chainhash.DoubleHashH([]byte(name))
	require.NoError(t, parent.From(prev.String(), 0, "76a91471d7dd96d9edda09180fe9d57a477b5acc9cad1188ac", 1e8))
	parent.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{bscript.OpTRUE})
	require.NoError(t, parent.PayToAddress("1BUBHgpCBWCsD2Ajbeu7ZgCgvMHsoYHm3W", 1000))
	_, err := store.Create(storeCtx, parent, 1)
	require.NoError(t, err)

	laterTx = bt.NewTx()
	require.NoError(t, laterTx.From(parent.TxID(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
	require.NoError(t, laterTx.PayToAddress("1BUBHgpCBWCsD2Ajbeu7ZgCgvMHsoYHm3W", 1))
	laterTx.LockTime = 2

	wrongHash := chainhash.Hash{99}
	owner := chainhash.Hash{7}
	before = []*utxo.Spend{{
		TxID: parent.TxIDChainHash(), Vout: 0, UTXOHash: &wrongHash,
		SpendingData: spendpkg.NewSpendingData(&owner, 0),
	}}

	return store, storeCtx, laterTx, before
}

func outpointIsUnspent(ctx context.Context, store *teranode_aerospike.Store, laterTx *bt.Tx) bool {
	laterSpends, err := utxo.GetSpends(laterTx)
	if err != nil {
		return false
	}

	stored, err := store.GetSpend(ctx, laterSpends[0])

	return err == nil && stored.Status == int(utxo.Status_OK) && stored.SpendingData == nil
}

// A claim that started before its caller gave up and finishes after the
// courtesy wait ends must be rolled back by whoever completes it, with no time
// bound, because the caller got an error and no spender record is ever created.
func TestSpendRollsBackClaimThatFinishesAfterAwaitBound(t *testing.T) {
	store, storeCtx, laterTx, before := prepareHeldSpend(t, "orphan-after-bound", 300*time.Millisecond, 4*time.Second)
	inFlight := teranode_aerospike.SpendAbortInFlightForTest(t)
	entered, release := make(chan struct{}), make(chan struct{})

	result, done, err := store.SpendHeldForTest(storeCtx, laterTx, 2, before, entered, release)
	require.NoError(t, err)

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("claim part did not start")
	}

	select {
	case err := <-result:
		require.ErrorIs(t, err, errors.ErrServiceUnavailable)
	case <-time.After(5 * time.Second):
		t.Fatal("caller did not return while its started claim was still held")
	}

	select {
	case <-done:
		t.Fatal("the claim finished before the caller returned, so the late path was not exercised")
	default:
	}

	require.Equal(t, inFlight+1, teranode_aerospike.SpendAbortInFlightForTest(t), "the claim the caller gave up on must count as in flight")

	close(release)

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("batch did not complete")
	}

	require.Eventually(t, func() bool { return outpointIsUnspent(storeCtx, store, laterTx) }, 10*time.Second, 50*time.Millisecond,
		"the late claim's spend was left on the outpoint")
}

// A claim whose success was published before its caller gave up on it must be
// rolled back by the caller's side of the hand-off.
func TestSpendRollsBackClaimPublishedBeforeItsCallerGaveUp(t *testing.T) {
	store, storeCtx, laterTx, _ := prepareHeldSpend(t, "orphan-after-publish", 300*time.Millisecond, 30*time.Second)

	laterSpends, err := utxo.GetSpends(laterTx)
	require.NoError(t, err)

	orphan := store.SpendPublishedForTest(t, laterSpends[0], 2)
	require.False(t, outpointIsUnspent(storeCtx, store, laterTx), "the claim must have spent the outpoint")

	orphan()

	require.Eventually(t, func() bool { return outpointIsUnspent(storeCtx, store, laterTx) }, 10*time.Second, 50*time.Millisecond,
		"a claim published before its caller gave up was left on the outpoint")
}

// The courtesy wait for a started claim and the rollback share one budget, so a
// failed Spend returns within SpendWaitTimeout plus SpendRollbackTimeout. The
// courtesy wait is only a quarter of that budget, so with a claim held past it
// the caller returns well before the whole budget is gone.
func TestSpendCallerBoundHoldsWhileStartedClaimIsHeld(t *testing.T) {
	const (
		wait  = 300 * time.Millisecond
		bound = 2 * time.Second
	)

	store, storeCtx, laterTx, before := prepareHeldSpend(t, "caller-bound", wait, bound)
	entered, release := make(chan struct{}), make(chan struct{})
	started := time.Now()

	result, done, err := store.SpendHeldForTest(storeCtx, laterTx, 2, before, entered, release)
	require.NoError(t, err)

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("claim part did not start")
	}

	select {
	case err := <-result:
		require.ErrorIs(t, err, errors.ErrServiceUnavailable)
	case <-time.After(wait + bound + 2*time.Second):
		t.Fatal("caller did not return within the documented bound")
	}

	elapsed := time.Since(started)
	require.LessOrEqual(t, elapsed, wait+bound+500*time.Millisecond, "caller held longer than SpendWaitTimeout plus SpendRollbackTimeout")
	require.Less(t, elapsed, wait+bound/2, "the courtesy wait must take a quarter of the budget, not all of it")

	close(release)
	<-done
}

// A blocking enqueue on a full batcher channel must not use up the wait budget:
// the budget starts when the enqueue returns.
func TestSpendWaitBudgetStartsAfterEnqueueOnSaturatedBatcher(t *testing.T) {
	store, storeCtx, laterTx, _ := prepareHeldSpend(t, "saturated", 300*time.Millisecond, 30*time.Second)
	release := make(chan struct{})

	result := store.SpendSaturatedForTest(storeCtx, laterTx, 2, release)

	time.Sleep(800 * time.Millisecond)
	close(release)

	select {
	case err := <-result:
		require.NoError(t, err, "a spend that waited for room in the batcher longer than SpendWaitTimeout must still succeed")
	case <-time.After(10 * time.Second):
		t.Fatal("Spend did not return after the batcher drained")
	}

	require.False(t, outpointIsUnspent(storeCtx, store, laterTx), "the spend must have been applied")
}

// When a completed spend of the same tx failed with ErrTxLocked and nothing
// proves the tx unwinnable, a concurrent attempt at the same txid may own the
// spends, so the batch rollback is suppressed. The late claim must then be left
// as the batch's own spends are, not unspent.
func TestSpendDoesNotRollBackLateClaimWhenTransientLockSuppressesRollback(t *testing.T) {
	store, storeCtx, laterTx, _ := prepareHeldSpend(t, "orphan-suppressed", 300*time.Millisecond, 4*time.Second)

	lockedParent := bt.NewTx()
	prev := chainhash.DoubleHashH([]byte("orphan-suppressed-locked"))
	require.NoError(t, lockedParent.From(prev.String(), 0, "76a91471d7dd96d9edda09180fe9d57a477b5acc9cad1188ac", 1e8))
	lockedParent.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{bscript.OpTRUE})
	require.NoError(t, lockedParent.PayToAddress("1BUBHgpCBWCsD2Ajbeu7ZgCgvMHsoYHm3W", 1000))
	_, err := store.Create(storeCtx, lockedParent, 1, utxo.WithLocked(true))
	require.NoError(t, err)

	twoInputs := bt.NewTx()
	require.NoError(t, twoInputs.From(lockedParent.TxID(), 0, lockedParent.Outputs[0].LockingScript.String(), lockedParent.Outputs[0].Satoshis))
	require.NoError(t, twoInputs.From(laterTx.Inputs[0].PreviousTxIDStr(), laterTx.Inputs[0].PreviousTxOutIndex, laterTx.Inputs[0].PreviousTxScript.String(), laterTx.Inputs[0].PreviousTxSatoshis))
	require.NoError(t, twoInputs.PayToAddress("1BUBHgpCBWCsD2Ajbeu7ZgCgvMHsoYHm3W", 1))
	twoInputs.LockTime = 2

	inFlight := teranode_aerospike.SpendAbortInFlightForTest(t)
	entered, release := make(chan struct{}), make(chan struct{})

	result, done, err := store.SpendHeldAfterFailureForTest(storeCtx, twoInputs, 2, entered, release)
	require.NoError(t, err)

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("claim part did not start")
	}

	select {
	case err := <-result:
		require.ErrorIs(t, err, errors.ErrServiceUnavailable)
	case <-time.After(5 * time.Second):
		t.Fatal("caller did not return while its started claim was still held")
	}

	require.Equal(t, inFlight+1, teranode_aerospike.SpendAbortInFlightForTest(t), "a suppressed late claim still counts as in flight")

	close(release)
	<-done

	require.Never(t, func() bool { return outpointIsUnspent(storeCtx, store, laterTx) }, 3*time.Second, 50*time.Millisecond,
		"the late claim was rolled back although a transient lock suppressed the batch rollback")
}
