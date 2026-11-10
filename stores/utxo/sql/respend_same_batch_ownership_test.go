package sql

import (
	"context"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-batcher/v2"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

// setupSharedBatch builds a store whose spend batcher actually accumulates more
// than one claim per batch, which the respend tests need.
//
// The package's setup() forces BatcherDrainMode=true so batch callbacks fire
// immediately and every batch holds one item. Here drain mode is off and the
// window is wide, so a Spend issued just before another lands in the SAME batch.
func setupSharedBatch(ctx context.Context, t *testing.T, dbName string) *Store {
	t.Helper()
	initPrometheusMetrics()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.DBTimeout = 30 * time.Second
	tSettings.UtxoStore.SpendBatcherSize = 256
	tSettings.UtxoStore.SpendBatcherDurationMillis = 200
	tSettings.BatcherDrainMode = false

	utxoStoreURL, err := url.Parse("sqlitememory:///" + dbName)
	require.NoError(t, err)

	store, err := New(ctx, ulogger.TestLogger{}, tSettings, utxoStoreURL)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close(context.Background()) })

	return store
}

// twoOutParent creates a parent with two outputs and returns it.
func twoOutParent(ctx context.Context, t *testing.T, store *Store, seed string) *bt.Tx {
	t.Helper()

	parent := bt.NewTx()
	prev := chainhash.DoubleHashH([]byte(seed))
	require.NoError(t, parent.From(prev.String(), 0,
		"76a91471d7dd96d9edda09180fe9d57a477b5acc9cad1188ac", 1e8))
	parent.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{bscript.OpTRUE})
	require.NoError(t, parent.PayToAddress("1BUBHgpCBWCsD2Ajbeu7ZgCgvMHsoYHm3W", 1000))
	require.NoError(t, parent.PayToAddress("1BUBHgpCBWCsD2Ajbeu7ZgCgvMHsoYHm3W", 2000))

	_, err := store.Create(ctx, parent, 1)
	require.NoError(t, err)

	return parent
}

// spendOf builds a spend of the given vouts of parent, distinguished by lockTime
// so each variant carries different spending data.
func spendOf(t *testing.T, parent *bt.Tx, lockTime uint32, vouts ...uint32) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()
	for _, v := range vouts {
		require.NoError(t, tx.From(parent.TxIDChainHash().String(), v,
			parent.Outputs[v].LockingScript.String(), parent.Outputs[v].Satoshis))
	}
	require.NoError(t, tx.PayToAddress("1BUBHgpCBWCsD2Ajbeu7ZgCgvMHsoYHm3W", 1))
	tx.LockTime = lockTime

	return tx
}

// TestRespendBehindLoserInSameBatchKeepsLegitimateSpends reproduces the
// respend-behind-loser failure.
//
// S0 legitimately spends both of the parent's outputs. A later batch then
// carries a double-spend attempt S1 on output 0 AHEAD of an idempotent re-spend
// of S0. The former duplicate filter awarded the outpoint to the first
// claim in the batch without consulting the persisted spender, so S0's
// idempotent re-spend is rejected with ErrSpent naming S1 -- a transaction that
// never spent anything. Store.Spend then treats that ErrSpent as an unwinnable
// failure (hasUnwinnableFailure) and un-spends output 1, which S0 had already
// re-spent successfully in the same batch.
//
// The test checks both the result and stored state:
//   - the re-spend must not be rejected (it matches the persisted spender);
//   - output 1 must stay spent by S0 even if some other input is refused.
func TestRespendBehindLoserInSameBatchKeepsLegitimateSpends(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := setupSharedBatch(ctx, t, "4934respend")
	require.NoError(t, store.SetBlockHeight(2))

	parent := twoOutParent(ctx, t, store, "4934-parent")

	s0 := spendOf(t, parent, 1, 0, 1) // legitimate spender of both outputs
	s1 := spendOf(t, parent, 2, 0)    // double-spend attempt on output 0

	// S0 spends both outputs up front.
	_, err := store.Spend(ctx, s0, 2)
	require.NoError(t, err, "initial S0 spend must succeed")

	// Capture the public Spend calls at the dispatch boundary. This fixes
	// batch membership and order without relying on scheduler timing.
	store.spendBatcher.Close()
	queued := make(chan *batchSpend, 3)
	store.spendBatcher = batcher.NewWithPool(1, time.Millisecond, func(items []*batchSpend) {
		queued <- items[0]
	}, false)
	next := func() *batchSpend {
		t.Helper()
		select {
		case item := <-queued:
			return item
		case <-time.After(5 * time.Second):
			t.Fatal("Spend did not enqueue its input")
			return nil
		}
	}
	errS1 := make(chan error, 1)
	go func() {
		_, e := store.Spend(ctx, s1, 2)
		errS1 <- e
	}()
	loserItem := next()
	errS0Ch := make(chan error, 1)
	go func() {
		_, e := store.Spend(ctx, s0, 2)
		errS0Ch <- e
	}()
	first, second := next(), next()
	if first.spend.Vout != 0 {
		first, second = second, first
	}
	store.sendSpendBatch([]*batchSpend{loserItem, first, second})
	errS0 := <-errS0Ch
	s1Err := <-errS1
	t.Logf("S1 err         : %v", s1Err)
	t.Logf("S0 re-spend err: %v", errS0)

	// Defect 1: an idempotent re-spend is a supported path and must succeed.
	require.NoError(t, errS0,
		"idempotent re-spend of S0 was rejected -- the in-batch filter handed the outpoint to S1")

	// The #825 guarantee must survive: the genuine double-spend loser is still
	// refused in the same batch, and is now blamed on S0, the tx that really
	// holds the outpoint, rather than on itself.
	require.Error(t, s1Err, "the double-spend attempt on output 0 must still be refused")
	require.True(t, errors.Is(s1Err, errors.ErrSpent), "expected ErrSpent for the loser, got: %v", s1Err)
	require.Contains(t, s1Err.Error(), s0.TxID(),
		"the loser must be blamed on S0, the persisted spender")

	// Defect 2: both outputs must remain spent by S0. Output 1 is the one the
	// rollback clears, and it is a real consensus-state loss.
	spends, err := utxo.GetSpends(s0)
	require.NoError(t, err)
	require.Len(t, spends, 2)

	for _, sp := range spends {
		resp, getErr := store.GetSpend(ctx, sp)
		require.NoError(t, getErr)
		t.Logf("output %d: status=%v spendingData=%v", sp.Vout, resp.Status, resp.SpendingData)

		require.Equal(t, int(utxo.Status_SPENT), resp.Status,
			"output %d that S0 legitimately spent went back to unspent", sp.Vout)
		require.NotNil(t, resp.SpendingData, "output %d must name its spender", sp.Vout)
		require.Equal(t, s0.TxID(), resp.SpendingData.TxID.String(),
			"output %d is owned by %s, not by S0", sp.Vout, resp.SpendingData.TxID)
	}
}

// Hold the first part in a real SQLite connection checkout until the competing
// caller has returned. The invalid first claim leaves the output spendable, so
// dispatching the abandoned second claim would create a dangling spent output.
func TestSpendBatchDoesNotCommitAbandonedLaterClaim(t *testing.T) {
	for _, cancelCaller := range []bool{false, true} {
		name, wantClass := "timeout", errors.ErrServiceUnavailable
		if cancelCaller {
			name, wantClass = "cancellation", errors.ErrContextCanceled
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			store := setupSharedBatch(ctx, t, "abandoned-"+name)
			require.NoError(t, store.SetBlockHeight(2))
			parent := twoOutParent(ctx, t, store, name)
			first, err := utxo.GetSpends(spendOf(t, parent, 1, 0))
			require.NoError(t, err)
			wrongHash := chainhash.Hash{99}
			first[0].UTXOHash = &wrongHash
			firstItem := &batchSpend{spend: first[0], blockHeight: 2, errCh: make(chan error, 1)}

			store.spendBatcher.Close()
			queued := make(chan *batchSpend, 1)
			store.spendBatcher = batcher.NewWithPool(1, time.Millisecond, func(items []*batchSpend) { queued <- items[0] }, false)
			store.settings.UtxoStore.SpendWaitTimeout = 200 * time.Millisecond
			callerCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			result := make(chan error, 1)
			laterTx := spendOf(t, parent, 2, 0)
			go func() { _, err := store.Spend(callerCtx, laterTx, 2); result <- err }()
			var later *batchSpend
			select {
			case later = <-queued:
			case <-time.After(3 * time.Second):
				t.Fatal("claim not queued")
			}

			store.db.SetMaxOpenConns(1)
			held, err := store.db.Conn(ctx)
			require.NoError(t, err)
			t.Cleanup(func() { _ = held.Close() })
			waits := store.db.Stats().WaitCount
			done := make(chan struct{})
			go func() { defer close(done); store.sendSpendBatch([]*batchSpend{firstItem, later}) }()
			require.Eventually(t, func() bool { return store.db.Stats().WaitCount > waits }, time.Second, time.Millisecond)
			if cancelCaller {
				cancel()
			}
			select {
			case err := <-result:
				require.ErrorIs(t, err, wantClass)
			case <-time.After(3 * time.Second):
				t.Fatal("caller did not stop waiting")
			}
			require.NoError(t, held.Close())
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Fatal("batch did not complete")
			}

			require.ErrorIs(t, firstItem.err, errors.ErrUtxoHashMismatch)
			stored, err := store.GetSpend(ctx, later.spend)
			require.NoError(t, err)
			require.Equal(t, int(utxo.Status_OK), stored.Status)
			require.Nil(t, stored.SpendingData, "no transaction record exists for the abandoned caller")
			require.ErrorIs(t, later.err, wantClass)
			_, err = store.Get(ctx, later.spend.SpendingData.TxID)
			require.ErrorIs(t, err, errors.ErrTxNotFound)
		})
	}
}

// A claim that started before its caller's deadline and finishes after it must
// settle before the caller returns, and its spend must be rolled back, because
// no spender record is ever created for a caller that got an error. The second
// shape is a claim with the wrong UTXO hash ahead of a claim with the right
// hash, so the right claim runs in a later backend part.
func TestSpendAwaitsAdmittedClaimThatFinishesAfterCallerDeadline(t *testing.T) {
	for _, shape := range []string{"single claim", "wrong hash claim first"} {
		for _, cancelCaller := range []bool{false, true} {
			name, wantClass := shape+" timeout", errors.ErrServiceUnavailable
			if cancelCaller {
				name, wantClass = shape+" cancellation", errors.ErrContextCanceled
			}

			t.Run(name, func(t *testing.T) {
				ctx := context.Background()
				store := setupSharedBatch(ctx, t, "late-"+name)
				require.NoError(t, store.SetBlockHeight(2))
				parent := twoOutParent(ctx, t, store, name)

				store.spendBatcher.Close()
				queued := make(chan *batchSpend, 1)
				store.spendBatcher = batcher.NewWithPool(1, time.Millisecond, func(items []*batchSpend) { queued <- items[0] }, false)
				store.settings.UtxoStore.SpendWaitTimeout = 200 * time.Millisecond

				callerCtx, cancel := context.WithCancel(ctx)
				defer cancel()

				result := make(chan error, 1)
				laterTx := spendOf(t, parent, 2, 0)
				go func() { _, err := store.Spend(callerCtx, laterTx, 2); result <- err }()

				var later *batchSpend
				select {
				case later = <-queued:
				case <-time.After(3 * time.Second):
					t.Fatal("claim not queued")
				}

				batch := []*batchSpend{later}
				var firstItem *batchSpend
				if shape == "wrong hash claim first" {
					first, err := utxo.GetSpends(spendOf(t, parent, 1, 0))
					require.NoError(t, err)
					wrongHash := chainhash.Hash{99}
					first[0].UTXOHash = &wrongHash
					firstItem = &batchSpend{spend: first[0], blockHeight: 2, errCh: make(chan error, 1)}
					batch = []*batchSpend{firstItem, later}
				}

				entered := make(chan struct{})
				release := make(chan struct{})
				store.beforeSpendPart = func(part []*batchSpend) {
					for _, item := range part {
						if item == later {
							close(entered)
							<-release

							return
						}
					}
				}

				done := make(chan struct{})
				go func() { defer close(done); store.sendSpendBatch(batch) }()

				select {
				case <-entered:
				case <-time.After(3 * time.Second):
					t.Fatal("claim part did not start")
				}

				require.True(t, later.admission.Started())

				if cancelCaller {
					cancel()
				}

				select {
				case err := <-result:
					t.Fatalf("caller returned %v while its admitted claim was still running", err)
				case <-time.After(500 * time.Millisecond):
				}

				close(release)

				select {
				case err := <-result:
					require.ErrorIs(t, err, wantClass)
				case <-time.After(5 * time.Second):
					t.Fatal("caller did not return after the claim settled")
				}

				select {
				case <-done:
				case <-time.After(3 * time.Second):
					t.Fatal("batch did not complete")
				}

				if firstItem != nil {
					require.ErrorIs(t, firstItem.err, errors.ErrUtxoHashMismatch)
				}

				require.NoError(t, later.err, "the claim must have run to completion for this test to exercise the late path")

				stored, err := store.GetSpend(ctx, later.spend)
				require.NoError(t, err)
				require.Equal(t, int(utxo.Status_OK), stored.Status, "the late claim's spend was left on the outpoint")
				require.Nil(t, stored.SpendingData)

				store.beforeSpendPart = nil
				store.spendBatcher.Close()
				store.spendBatcher = batcher.NewWithPool(1, time.Millisecond, store.sendSpendBatch, false)
				store.settings.UtxoStore.SpendWaitTimeout = 5 * time.Second

				_, err = store.Spend(ctx, spendOf(t, parent, 3, 0), 2)
				require.NoError(t, err, "a fresh spend of the outpoint must not see the late claimant as its owner")
			})
		}
	}
}

// The second claim differs from the first only in ignoreLocked, so the first
// claim's locked verdict says nothing about it.
func TestSpendBatchClaimIgnoringLockedIsNotRefusedForAnotherClaimsLockedVerdict(t *testing.T) {
	ctx := context.Background()
	store := setupSharedBatch(ctx, t, "ignore-locked-verdict")
	require.NoError(t, store.SetBlockHeight(2))

	parent := bt.NewTx()
	prev := chainhash.DoubleHashH([]byte("ignore-locked-verdict"))
	require.NoError(t, parent.From(prev.String(), 0, "76a91471d7dd96d9edda09180fe9d57a477b5acc9cad1188ac", 1e8))
	parent.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{bscript.OpTRUE})
	require.NoError(t, parent.PayToAddress("1BUBHgpCBWCsD2Ajbeu7ZgCgvMHsoYHm3W", 1000))

	_, err := store.Create(ctx, parent, 1, utxo.WithLocked(true))
	require.NoError(t, err)

	firstSpends, err := utxo.GetSpends(spendOf(t, parent, 1, 0))
	require.NoError(t, err)
	secondSpends, err := utxo.GetSpends(spendOf(t, parent, 2, 0))
	require.NoError(t, err)

	first := &batchSpend{spend: firstSpends[0], blockHeight: 2, errCh: make(chan error, 1)}
	second := &batchSpend{spend: secondSpends[0], blockHeight: 2, ignoreLocked: true, errCh: make(chan error, 1)}

	store.sendSpendBatch([]*batchSpend{first, second})

	require.ErrorIs(t, first.err, errors.ErrTxLocked)
	require.NoError(t, second.err, "a claim that ignores the locked flag was refused with another claim's locked verdict")

	stored, err := store.GetSpend(ctx, second.spend)
	require.NoError(t, err)
	require.NotNil(t, stored.SpendingData)
	require.Equal(t, second.spend.SpendingData.TxID.String(), stored.SpendingData.TxID.String())
}

// The second claim differs from the first only in block height. The first is
// below coinbase maturity and the second is at it, so the first claim's
// immature verdict says nothing about the second.
func TestSpendBatchClaimAtMatureHeightIsNotRefusedForAnotherClaimsImmatureVerdict(t *testing.T) {
	ctx := context.Background()
	store := setupSharedBatch(ctx, t, "height-verdict")
	require.NoError(t, store.SetBlockHeight(2))

	maturity := uint32(store.settings.ChainCfgParams.CoinbaseMaturity)
	require.Positive(t, maturity)

	const createHeight = uint32(100)

	coinbaseTx, err := bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff580320a107152f5669614254432f48656c6c6f20576f726c64212f2cfabe6d6dbcbb1b0222e1aeebaca2a9c905bb23a3ad0302898ec600a9033a87ec1645a446010000000000000010f829ba0b13a84def80c389cde9840000ffffffff0174fdaf4a000000001976a914f1c075a01882ae0972f95d3a4177c86c852b7d9188ac00000000")
	require.NoError(t, err)

	_, err = store.Create(ctx, coinbaseTx, createHeight)
	require.NoError(t, err)

	firstSpends, err := utxo.GetSpends(spendOf(t, coinbaseTx, 1, 0))
	require.NoError(t, err)
	secondSpends, err := utxo.GetSpends(spendOf(t, coinbaseTx, 2, 0))
	require.NoError(t, err)

	first := &batchSpend{spend: firstSpends[0], blockHeight: createHeight + maturity - 1, ignoreLocked: true, errCh: make(chan error, 1)}
	second := &batchSpend{spend: secondSpends[0], blockHeight: createHeight + maturity, ignoreLocked: true, errCh: make(chan error, 1)}

	store.sendSpendBatch([]*batchSpend{first, second})

	require.ErrorIs(t, first.err, errors.ErrTxCoinbaseImmature)
	require.NoError(t, second.err, "a claim at a mature height was refused with another claim's immature verdict")
}

type heldClaim struct {
	store   *Store
	parent  *bt.Tx
	later   *batchSpend
	queued  chan *batchSpend
	result  chan error
	release chan struct{}
	done    chan struct{}
}

// startHeldClaim runs Spend for a claim that sits behind a wrong hash claim and
// whose backend part is held until release is closed. It returns once that part
// has started.
func startHeldClaim(t *testing.T, name string, wait, bound time.Duration) *heldClaim {
	t.Helper()

	ctx := context.Background()
	store := setupSharedBatch(ctx, t, name)
	require.NoError(t, store.SetBlockHeight(2))

	parent := twoOutParent(ctx, t, store, name)

	store.spendBatcher.Close()
	queued := make(chan *batchSpend, 1)
	store.spendBatcher = batcher.NewWithPool(1, time.Millisecond, func(items []*batchSpend) { queued <- items[0] }, false)
	store.settings.UtxoStore.SpendWaitTimeout = wait
	store.settings.UtxoStore.SpendRollbackTimeout = bound

	held := &heldClaim{store: store, parent: parent, queued: queued, result: make(chan error, 1), release: make(chan struct{}), done: make(chan struct{})}

	go func() { _, err := store.Spend(ctx, spendOf(t, parent, 2, 0), 2); held.result <- err }()

	select {
	case held.later = <-queued:
	case <-time.After(3 * time.Second):
		t.Fatal("claim not queued")
	}

	first, err := utxo.GetSpends(spendOf(t, parent, 1, 0))
	require.NoError(t, err)

	wrongHash := chainhash.Hash{99}
	first[0].UTXOHash = &wrongHash
	firstItem := &batchSpend{spend: first[0], blockHeight: 2, errCh: make(chan error, 1)}

	entered := make(chan struct{})
	store.beforeSpendPart = func(part []*batchSpend) {
		for _, item := range part {
			if item == held.later {
				close(entered)
				<-held.release

				return
			}
		}
	}

	go func() { defer close(held.done); store.sendSpendBatch([]*batchSpend{firstItem, held.later}) }()

	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("claim part did not start")
	}

	return held
}

func abortInFlightCount(t *testing.T) float64 {
	t.Helper()

	var metric dto.Metric
	require.NoError(t, prometheusUtxoSpendAbortInFlight.Write(&metric))

	return metric.GetCounter().GetValue()
}

func outputIsUnspent(ctx context.Context, store *Store, spend *utxo.Spend) bool {
	stored, err := store.GetSpend(ctx, spend)

	return err == nil && stored.Status == int(utxo.Status_OK) && stored.SpendingData == nil
}

// A claim that started before its caller gave up and finishes after the
// courtesy wait ends must be rolled back by whoever completes it, with no time
// bound, because the caller got an error and no spender record is ever created.
func TestSpendRollsBackClaimThatFinishesAfterAwaitBound(t *testing.T) {
	initPrometheusMetrics()
	inFlight := abortInFlightCount(t)
	held := startHeldClaim(t, "orphan-after-bound", 300*time.Millisecond, 4*time.Second)

	select {
	case err := <-held.result:
		require.ErrorIs(t, err, errors.ErrServiceUnavailable)
	case <-time.After(5 * time.Second):
		t.Fatal("caller did not return while its started claim was still held")
	}

	select {
	case <-held.done:
		t.Fatal("the claim finished before the caller returned, so the late path was not exercised")
	default:
	}

	require.Equal(t, inFlight+1, abortInFlightCount(t), "the claim the caller gave up on must count as in flight")

	close(held.release)

	select {
	case <-held.done:
	case <-time.After(5 * time.Second):
		t.Fatal("batch did not complete")
	}

	require.NoError(t, held.later.err, "the claim must have run to completion for this test to exercise the late path")
	require.Eventually(t, func() bool { return outputIsUnspent(context.Background(), held.store, held.later.spend) }, 10*time.Second, 20*time.Millisecond,
		"the late claim's spend was left on the outpoint")
}

// A claim whose success was published before its caller gave up on it must be
// rolled back by the caller's side of the hand-off.
func TestSpendRollsBackClaimPublishedBeforeItsCallerGaveUp(t *testing.T) {
	ctx := context.Background()
	store := setupSharedBatch(ctx, t, "orphan-after-publish")
	require.NoError(t, store.SetBlockHeight(2))
	parent := twoOutParent(ctx, t, store, "orphan-after-publish")

	spends, err := utxo.GetSpends(spendOf(t, parent, 2, 0))
	require.NoError(t, err)

	item := &batchSpend{spend: spends[0], blockHeight: 2, errCh: make(chan error, 1), rollbackOrphan: store.rollbackOrphanedSpend}
	store.sendSpendBatch([]*batchSpend{item})
	require.NoError(t, item.err, "the claim must have succeeded for this test to exercise the orphan path")
	require.False(t, outputIsUnspent(ctx, store, spends[0]), "the claim must have spent the output")

	item.markOrphaned()

	require.Eventually(t, func() bool { return outputIsUnspent(ctx, store, spends[0]) }, 10*time.Second, 20*time.Millisecond,
		"a claim published before its caller gave up was left on the output")
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

	started := time.Now()
	held := startHeldClaim(t, "caller-bound", wait, bound)

	select {
	case err := <-held.result:
		require.ErrorIs(t, err, errors.ErrServiceUnavailable)
	case <-time.After(wait + bound + 2*time.Second):
		t.Fatal("caller did not return within the documented bound")
	}

	elapsed := time.Since(started)
	require.LessOrEqual(t, elapsed, wait+bound+500*time.Millisecond, "caller held longer than SpendWaitTimeout plus SpendRollbackTimeout")
	require.Less(t, elapsed, wait+bound/2, "the courtesy wait must take a quarter of the budget, not all of it")

	close(held.release)
	<-held.done
}

// A blocking enqueue on a full batcher channel must not use up the wait budget:
// the budget starts when the enqueue returns.
func TestSpendWaitBudgetStartsAfterEnqueueOnSaturatedBatcher(t *testing.T) {
	ctx := context.Background()
	store := setupSharedBatch(ctx, t, "saturated-batcher")
	require.NoError(t, store.SetBlockHeight(2))
	parent := twoOutParent(ctx, t, store, "saturated")

	store.spendBatcher.Close()

	release := make(chan struct{})
	store.spendBatcher = batcher.NewWithPool(1, time.Millisecond, func(items []*batchSpend) {
		<-release

		real := make([]*batchSpend, 0, len(items))

		for _, item := range items {
			if item.errCh != nil && item.spend.TxID != nil {
				real = append(real, item)
			}
		}

		if len(real) > 0 {
			store.sendSpendBatch(real)
		}
	}, false)
	store.settings.UtxoStore.SpendWaitTimeout = 300 * time.Millisecond

	var puts sync.WaitGroup

	for i := 0; i < 1000; i++ {
		puts.Add(1)

		go func() {
			defer puts.Done()
			store.spendBatcher.Put(&batchSpend{spend: &utxo.Spend{}})
		}()
	}

	time.Sleep(300 * time.Millisecond)

	laterTx := spendOf(t, parent, 2, 0)
	result := make(chan error, 1)
	go func() { _, err := store.Spend(ctx, laterTx, 2); result <- err }()

	time.Sleep(800 * time.Millisecond)
	close(release)

	select {
	case err := <-result:
		require.NoError(t, err, "a spend that waited for room in the batcher longer than SpendWaitTimeout must still succeed")
	case <-time.After(10 * time.Second):
		t.Fatal("Spend did not return after the batcher drained")
	}

	puts.Wait()

	spends, err := utxo.GetSpends(laterTx)
	require.NoError(t, err)
	require.False(t, outputIsUnspent(ctx, store, spends[0]), "the spend must have been applied")
}

// When a completed spend of the same tx failed with ErrTxLocked and nothing
// proves the tx unwinnable, a concurrent attempt at the same txid may own the
// spends, so the batch rollback is suppressed. The late claim must then be left
// as the batch's own spends are, not unspent.
func TestSpendDoesNotRollBackLateClaimWhenTransientLockSuppressesRollback(t *testing.T) {
	initPrometheusMetrics()
	ctx := context.Background()
	store := setupSharedBatch(ctx, t, "orphan-suppressed")
	require.NoError(t, store.SetBlockHeight(2))

	parent := twoOutParent(ctx, t, store, "orphan-suppressed")

	lockedParent := bt.NewTx()
	prev := chainhash.DoubleHashH([]byte("orphan-suppressed-locked"))
	require.NoError(t, lockedParent.From(prev.String(), 0, "76a91471d7dd96d9edda09180fe9d57a477b5acc9cad1188ac", 1e8))
	lockedParent.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{bscript.OpTRUE})
	require.NoError(t, lockedParent.PayToAddress("1BUBHgpCBWCsD2Ajbeu7ZgCgvMHsoYHm3W", 1000))
	_, err := store.Create(ctx, lockedParent, 1, utxo.WithLocked(true))
	require.NoError(t, err)

	twoInputs := bt.NewTx()
	require.NoError(t, twoInputs.From(lockedParent.TxIDChainHash().String(), 0, lockedParent.Outputs[0].LockingScript.String(), lockedParent.Outputs[0].Satoshis))
	require.NoError(t, twoInputs.From(parent.TxIDChainHash().String(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
	require.NoError(t, twoInputs.PayToAddress("1BUBHgpCBWCsD2Ajbeu7ZgCgvMHsoYHm3W", 1))
	twoInputs.LockTime = 2

	store.spendBatcher.Close()
	queued := make(chan *batchSpend, 2)
	store.spendBatcher = batcher.NewWithPool(1, time.Millisecond, func(items []*batchSpend) { queued <- items[0] }, false)
	store.settings.UtxoStore.SpendWaitTimeout = 300 * time.Millisecond
	store.settings.UtxoStore.SpendRollbackTimeout = 4 * time.Second

	inFlight := abortInFlightCount(t)
	result := make(chan error, 1)
	go func() { _, err := store.Spend(ctx, twoInputs, 2); result <- err }()

	var lockedItem, claim *batchSpend

	for i := 0; i < 2; i++ {
		select {
		case item := <-queued:
			if item.spend.TxID.IsEqual(lockedParent.TxIDChainHash()) {
				lockedItem = item
			} else {
				claim = item
			}
		case <-time.After(3 * time.Second):
			t.Fatal("claims not queued")
		}
	}

	require.NotNil(t, lockedItem)
	require.NotNil(t, claim)

	store.sendSpendBatch([]*batchSpend{lockedItem})
	require.ErrorIs(t, lockedItem.err, errors.ErrTxLocked)

	entered, release := make(chan struct{}), make(chan struct{})
	store.beforeSpendPart = func(part []*batchSpend) {
		for _, item := range part {
			if item == claim {
				close(entered)
				<-release

				return
			}
		}
	}

	done := make(chan struct{})
	go func() { defer close(done); store.sendSpendBatch([]*batchSpend{claim}) }()

	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("claim part did not start")
	}

	select {
	case err := <-result:
		require.Error(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("caller did not return while its started claim was still held")
	}

	require.Equal(t, inFlight+1, abortInFlightCount(t), "a suppressed late claim still counts as in flight")

	close(release)
	<-done
	require.NoError(t, claim.err)

	require.Never(t, func() bool { return outputIsUnspent(ctx, store, claim.spend) }, 2*time.Second, 20*time.Millisecond,
		"the late claim was rolled back although a transient lock suppressed the batch rollback")
}

// A retry of the same transaction must not have its spend cleared by the
// orphan rollback of an earlier attempt. Both attempts store identical
// spending data, so the rollback cannot tell them apart in storage: it must not
// run while a retry owns the input.
func TestOrphanRollbackDoesNotClearRetryOfSameTransaction(t *testing.T) {
	ctx := context.Background()
	held := startHeldClaim(t, "orphan-vs-retry", 300*time.Millisecond, 4*time.Second)

	select {
	case err := <-held.result:
		require.ErrorIs(t, err, errors.ErrServiceUnavailable)
	case <-time.After(5 * time.Second):
		t.Fatal("caller did not return while its started claim was held")
	}

	held.store.settings.UtxoStore.SpendWaitTimeout = 10 * time.Second

	retryResult := make(chan error, 1)

	go func() { _, err := held.store.Spend(ctx, spendOf(t, held.parent, 2, 0), 2); retryResult <- err }()

	process := func(item *batchSpend) { held.store.sendSpendBatch([]*batchSpend{item}) }

	var retryItem *batchSpend

	select {
	case retryItem = <-held.queued:
		process(retryItem)
	case <-time.After(500 * time.Millisecond):
	}

	close(held.release)

	select {
	case <-held.done:
	case <-time.After(5 * time.Second):
		t.Fatal("held batch did not complete")
	}

	if retryItem == nil {
		select {
		case retryItem = <-held.queued:
			process(retryItem)
		case <-time.After(10 * time.Second):
			t.Fatal("retry was never queued")
		}
	}

	select {
	case err := <-retryResult:
		require.NoError(t, err, "the retry must succeed")
	case <-time.After(10 * time.Second):
		t.Fatal("retry did not return")
	}

	require.Never(t, func() bool { return outputIsUnspent(ctx, held.store, held.later.spend) }, time.Second, 20*time.Millisecond,
		"the orphan rollback of an earlier attempt cleared the spend a retry of the same transaction owns")
}

// A retry that cannot wait out an earlier attempt's unresolved late spend fails
// at the wait budget, without queueing a claim that could be rolled back by it.
func TestRetryFailsAtWaitBudgetWhileEarlierLateSpendIsUnresolved(t *testing.T) {
	ctx := context.Background()
	held := startHeldClaim(t, "orphan-retry-budget", 300*time.Millisecond, 4*time.Second)

	select {
	case err := <-held.result:
		require.ErrorIs(t, err, errors.ErrServiceUnavailable)
	case <-time.After(5 * time.Second):
		t.Fatal("caller did not return while its started claim was held")
	}

	_, err := held.store.Spend(ctx, spendOf(t, held.parent, 2, 0), 2)
	require.ErrorIs(t, err, errors.ErrServiceUnavailable)

	select {
	case <-held.queued:
		t.Fatal("the retry queued a claim while the earlier late spend was unresolved")
	default:
	}

	close(held.release)
	<-held.done
}
