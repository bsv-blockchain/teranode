package tests

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/stretchr/testify/require"
)

// These are the WithBelowCheckpoint contract tests: the list is written in two
// waves and ends in the same store state as the default order; a transaction
// with no parent in the list takes one combined call, and one with a parent in
// the list a create and then a spend; a write that fails stops its
// descendants' spends; a record that already exists has its spends made again;
// a parent output that exists nowhere fails a second-wave spend instead of
// passing on the strength of the record the first wave just wrote, while a
// record that existed before the list, with its spends made, is Existed even
// when its parent has been pruned; and a cancel never lands inside a call.

// waveCall is one SpendAndCreate call DefaultSpendAndCreateMulti made, with the
// half it asked for.
type waveCall struct {
	tx                    *bt.Tx
	createOnly, spendOnly bool
	start, end            time.Time
}

// waveRecorder wraps a real store and records every SpendAndCreate call. delay
// holds each call before it reaches the store, so calls that can overlap
// visibly do. failCreate, when set, fails that transaction's first-wave call
// (create-only or combined) before it reaches the store.
type waveRecorder struct {
	utxostore.Store
	delay      time.Duration
	failCreate *bt.Tx

	mu    sync.Mutex
	calls []waveCall
}

func (w *waveRecorder) SpendAndCreate(ctx context.Context, tx *bt.Tx, h uint32, opts ...utxostore.CreateOption) (*meta.Data, []*utxostore.Spend, error) {
	options, err := utxostore.ParseCreateOptions(opts...)
	if err != nil {
		return nil, nil, err
	}

	start := time.Now()

	if w.delay > 0 {
		time.Sleep(w.delay)
	}

	var (
		md     *meta.Data
		spends []*utxostore.Spend
	)

	if w.failCreate != nil && !options.SpendOnly && *tx.TxIDChainHash() == *w.failCreate.TxIDChainHash() {
		err = errors.NewStorageError("injected create failure")
	} else {
		md, spends, err = w.Store.SpendAndCreate(ctx, tx, h, opts...)
	}

	w.mu.Lock()
	w.calls = append(w.calls, waveCall{tx: tx, createOnly: options.CreateOnly, spendOnly: options.SpendOnly, start: start, end: time.Now()})
	w.mu.Unlock()

	return md, spends, err
}

func (w *waveRecorder) recorded() []waveCall {
	w.mu.Lock()
	defer w.mu.Unlock()

	return append([]waveCall(nil), w.calls...)
}

// SpendAndCreateMultiBelowCheckpointMatchesLoop: a WithBelowCheckpoint list
// leaves every record and every spend exactly as a loop of SpendAndCreate
// does, before and after the record-mined write.
func SpendAndCreateMultiBelowCheckpointMatchesLoop(t *testing.T, db utxostore.Store) {
	ctx := context.Background()

	const height = 800

	loop := BuildMultiWorkload(t, 0xa0, 5, 7)
	multi := BuildMultiWorkload(t, 0xa1, 5, 7)

	loop.StoreRoots(t, db, height-1)
	multi.StoreRoots(t, db, height-1)

	loopSpendAndCreate(t, db, loop.Txs, height, utxostore.WithIgnoreLocked(true))

	results, err := db.SpendAndCreateMulti(ctx, multi.Txs, height, utxostore.WithBelowCheckpoint(), utxostore.WithIgnoreLocked(true))
	require.NoError(t, err)
	require.Len(t, results, len(multi.Txs))
	requireAllStatus(t, results, utxostore.MultiTxCreated)

	for i, r := range results {
		require.NotNil(t, r.Meta, "tx %d", i)
		require.NotEmpty(t, r.Spends, "tx %d", i)
	}

	require.Equal(t, loop.Records(t, db), multi.Records(t, db), "records before the record-mined write")
	require.Equal(t, loop.RootSpends(t, db), multi.RootSpends(t, db))

	loop.MarkMined(t, db, 902, height)
	multi.MarkMined(t, db, 902, height)

	require.Equal(t, loop.Records(t, db), multi.Records(t, db), "records after the record-mined write")
}

// SpendAndCreateMultiBelowCheckpointTwoWaves: the first level, whose parents
// are outside the list, gets one combined call per transaction; every deeper
// transaction gets one create-only call and one spend-only call; every
// first-wave call returns before any spend starts; and the first-wave calls of
// all five levels run together, as do the spends, so the depth of the list
// does not add rounds.
func SpendAndCreateMultiBelowCheckpointTwoWaves(t *testing.T, db utxostore.Store) {
	const (
		height = 810
		levels = 5
		width  = 3
	)

	w := BuildMultiWorkload(t, 0xa2, levels, width)
	w.StoreRoots(t, db, height-1)

	rec := &waveRecorder{Store: db, delay: 20 * time.Millisecond}

	results, err := utxostore.DefaultSpendAndCreateMulti(context.Background(), rec, utxostore.UniformSpendAndCreateMultiWidths(len(w.Txs)),
		w.Txs, height, utxostore.WithBelowCheckpoint(), utxostore.WithIgnoreLocked(true))
	require.NoError(t, err)
	requireAllStatus(t, results, utxostore.MultiTxCreated)

	firstLevel := make(map[chainhash.Hash]bool, width)
	for _, tx := range w.Txs[:width] {
		firstLevel[*tx.TxIDChainHash()] = true
	}

	calls := rec.recorded()
	require.Len(t, calls, width+2*(len(w.Txs)-width), "one combined call per first-level transaction, a create and a spend per deeper one")

	var firstWave, spends []waveCall

	for _, c := range calls {
		if firstLevel[*c.tx.TxIDChainHash()] {
			require.False(t, c.createOnly || c.spendOnly, "a transaction with no parent in the list takes one combined call")

			firstWave = append(firstWave, c)

			continue
		}

		require.True(t, c.createOnly != c.spendOnly, "a deeper call is one half: createOnly=%v spendOnly=%v", c.createOnly, c.spendOnly)

		if c.createOnly {
			firstWave = append(firstWave, c)
		} else {
			spends = append(spends, c)
		}
	}

	require.Len(t, firstWave, len(w.Txs))
	require.Len(t, spends, len(w.Txs)-width)

	for _, s := range spends {
		for _, c := range firstWave {
			require.False(t, s.start.Before(c.end), "a spend started before every first-wave call had returned")
		}
	}

	requireOneRound(t, "first", firstWave)
	requireOneRound(t, "spend", spends)

	for i, r := range w.Records(t, db) {
		require.True(t, r.ExistsInStore, "tx %d", i)
	}

	// A first-level transaction is spent by its child, and the deepest link of
	// the first chain by its child.
	requireSpentBy(t, db, w.Txs[0], 0, w.Txs[width], 0)
	requireSpentBy(t, db, w.Txs[(levels-2)*width], 0, w.Txs[(levels-1)*width], 0)
}

// requireOneRound checks that every call overlapped every other, so they ran as
// one round rather than one after another.
func requireOneRound(t *testing.T, wave string, calls []waveCall) {
	t.Helper()

	var latestStart, earliestEnd time.Time

	for i, c := range calls {
		if i == 0 || c.start.After(latestStart) {
			latestStart = c.start
		}

		if i == 0 || c.end.Before(earliestEnd) {
			earliestEnd = c.end
		}
	}

	require.True(t, latestStart.Before(earliestEnd), "the %s wave did not run as one round", wave)
}

// SpendAndCreateMultiBelowCheckpointCreateFails: a transaction whose
// first-wave call fails is Failed, its child and grandchild are ParentFailed
// and never spend, and an unrelated transaction is written and spent.
func SpendAndCreateMultiBelowCheckpointCreateFails(t *testing.T, db utxostore.Store) {
	const height = 820

	w := BuildMultiWorkload(t, 0xa3, 0, 2)
	w.StoreRoots(t, db, height-1)

	a := multiSpend(t, 1, []*bt.Tx{w.Roots[0]}, []uint32{0}, 1)
	b := multiSpend(t, 2, []*bt.Tx{a}, []uint32{0}, 1)
	c := multiSpend(t, 3, []*bt.Tx{b}, []uint32{0}, 1)
	d := multiSpend(t, 4, []*bt.Tx{w.Roots[1]}, []uint32{0}, 1)

	rec := &waveRecorder{Store: db, failCreate: a}

	results, err := utxostore.DefaultSpendAndCreateMulti(context.Background(), rec, utxostore.UniformSpendAndCreateMultiWidths(4),
		[]*bt.Tx{a, b, c, d}, height, utxostore.WithBelowCheckpoint(), utxostore.WithIgnoreLocked(true))
	require.NoError(t, err)
	require.Len(t, results, 4)

	require.Equal(t, utxostore.MultiTxFailed, results[0].Status)
	require.Error(t, results[0].Err)
	require.Equal(t, utxostore.MultiTxParentFailed, results[1].Status)
	require.Error(t, results[1].Err)
	require.Equal(t, utxostore.MultiTxParentFailed, results[2].Status)
	require.Error(t, results[2].Err)
	require.Equal(t, utxostore.MultiTxCreated, results[3].Status, "%v", results[3].Err)

	for _, call := range rec.recorded() {
		if *call.tx.TxIDChainHash() == *b.TxIDChainHash() || *call.tx.TxIDChainHash() == *c.TxIDChainHash() {
			require.True(t, call.createOnly, "a descendant of a failed write only creates")
		}
	}

	requireUnspent(t, db, a, 0)
	requireUnspent(t, db, b, 0)

	requireNotStored(t, db, a)
	requireUnspent(t, db, w.Roots[0], 0)
	requireSpentBy(t, db, w.Roots[1], 0, d, 0)
}

// SpendAndCreateMultiBelowCheckpointExistingRespent: a record created by an
// earlier attempt that never reached its spends, as a crash between the waves
// leaves it, is Existed, and its inputs are spent by it afterwards; its child
// in the list is written and spends it.
func SpendAndCreateMultiBelowCheckpointExistingRespent(t *testing.T, db utxostore.Store) {
	ctx := context.Background()

	const height = 830

	w := BuildMultiWorkload(t, 0xa4, 0, 1)
	w.StoreRoots(t, db, height-1)

	a := multiSpend(t, 1, []*bt.Tx{w.Roots[0]}, []uint32{0}, 1)
	b := multiSpend(t, 2, []*bt.Tx{a}, []uint32{0}, 1)

	_, _, err := db.SpendAndCreate(ctx, a, height, utxostore.WithCreateOnly())
	require.NoError(t, err)
	requireUnspent(t, db, w.Roots[0], 0)

	results, err := db.SpendAndCreateMulti(ctx, []*bt.Tx{a, b}, height, utxostore.WithBelowCheckpoint(), utxostore.WithIgnoreLocked(true))
	require.NoError(t, err)
	require.Len(t, results, 2)
	require.Equal(t, utxostore.MultiTxExisted, results[0].Status, "%v", results[0].Err)
	require.NotEmpty(t, results[0].Spends, "an existing record's spends are made")
	require.Equal(t, utxostore.MultiTxCreated, results[1].Status, "%v", results[1].Err)

	requireSpentBy(t, db, w.Roots[0], 0, a, 0)
	requireSpentBy(t, db, a, 0, b, 0)
}

// SpendAndCreateMultiBelowCheckpointMissingParent: a transaction whose parent
// is in neither the list nor the store is Failed with ErrTxNotFound, and a
// child in the list that spends it is ParentFailed. The orphan's combined call
// spends before it creates, so its record is never written. The child's record
// is, by the first wave, and the store must not take a record as proof the
// spend was made in an earlier attempt.
func SpendAndCreateMultiBelowCheckpointMissingParent(t *testing.T, db utxostore.Store) {
	ctx := context.Background()

	const height = 840

	w := BuildMultiWorkload(t, 0xa5, 0, 1) // roots never stored

	orphan := multiSpend(t, 1, []*bt.Tx{w.Roots[0]}, []uint32{0}, 1)
	child := multiSpend(t, 2, []*bt.Tx{orphan}, []uint32{0}, 1)

	results, err := db.SpendAndCreateMulti(ctx, []*bt.Tx{orphan, child}, height, utxostore.WithBelowCheckpoint(), utxostore.WithIgnoreLocked(true))
	require.NoError(t, err)
	require.Len(t, results, 2)
	require.Equal(t, utxostore.MultiTxFailed, results[0].Status, "a spend of an output that exists nowhere must fail")
	require.ErrorIs(t, results[0].Err, errors.ErrTxNotFound)
	require.Equal(t, utxostore.MultiTxParentFailed, results[1].Status)

	_, err = db.Get(ctx, orphan.TxIDChainHash(), fields.Fee)
	require.ErrorIs(t, err, errors.ErrTxNotFound, "the orphan's combined call failed its spend before it created")
}

// SpendAndCreateMultiBelowCheckpointExistingParentPruned: a transaction with
// no parent in the list whose record already exists, with its spends made, and
// whose parent is no longer in the store, as on a store left ahead of the chain
// by a reset, is Existed, not Failed. Its combined call spends before it
// creates, so the record it finds was written after its spends, and the store
// may read that record as proof the spend was made, as the per-transaction path
// does.
func SpendAndCreateMultiBelowCheckpointExistingParentPruned(t *testing.T, db utxostore.Store) {
	ctx := context.Background()

	const height = 845

	w := BuildMultiWorkload(t, 0xa8, 0, 1)
	w.StoreRoots(t, db, height-1)

	a := multiSpend(t, 1, []*bt.Tx{w.Roots[0]}, []uint32{0}, 1)

	_, _, err := db.SpendAndCreate(ctx, a, height)
	require.NoError(t, err)
	require.NoError(t, db.Delete(ctx, w.Roots[0].TxIDChainHash()))

	results, err := db.SpendAndCreateMulti(ctx, []*bt.Tx{a}, height, utxostore.WithBelowCheckpoint(), utxostore.WithIgnoreLocked(true))
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, utxostore.MultiTxExisted, results[0].Status, "a record whose spends were made must not fail on a pruned parent: %v", results[0].Err)
}

// SpendAndCreateMultiBelowCheckpointSecondWaveMissingParent: a transaction
// with a parent in the list that also spends an output existing nowhere fails
// that spend with ErrTxNotFound. The first wave has just written its record,
// and the store must not take that record as proof the spend was made in an
// earlier attempt.
func SpendAndCreateMultiBelowCheckpointSecondWaveMissingParent(t *testing.T, db utxostore.Store) {
	ctx := context.Background()

	const height = 846

	w := BuildMultiWorkload(t, 0xa9, 0, 2)

	_, _, err := db.SpendAndCreate(ctx, w.Roots[0], height-1, utxostore.WithCreateOnly()) // Roots[1] never stored
	require.NoError(t, err)

	a := multiSpend(t, 1, []*bt.Tx{w.Roots[0]}, []uint32{0}, 1)
	b := multiSpend(t, 2, []*bt.Tx{a, w.Roots[1]}, []uint32{0, 0}, 1)

	results, err := db.SpendAndCreateMulti(ctx, []*bt.Tx{a, b}, height, utxostore.WithBelowCheckpoint(), utxostore.WithIgnoreLocked(true))
	require.NoError(t, err)
	require.Len(t, results, 2)
	require.Equal(t, utxostore.MultiTxCreated, results[0].Status, "%v", results[0].Err)
	require.Equal(t, utxostore.MultiTxFailed, results[1].Status, "a spend of an output that exists nowhere must fail")
	require.ErrorIs(t, results[1].Err, errors.ErrTxNotFound)
}

// SpendAndCreateMultiBelowCheckpointCancelMidCall: a cancel that arrives while
// a call is in the store does not reach that call, and a call that has not
// started never starts. The in-flight transaction is written with its input
// spent; the queued one is absent with its input unspent; the error is the
// context's.
func SpendAndCreateMultiBelowCheckpointCancelMidCall(t *testing.T, db utxostore.Store) {
	const height = 850

	w := BuildMultiWorkload(t, 0xa6, 0, 2)
	w.StoreRoots(t, db, height-1)

	inFlight := multiSpend(t, 1, []*bt.Tx{w.Roots[0]}, []uint32{0}, 1)
	queued := multiSpend(t, 2, []*bt.Tx{w.Roots[1]}, []uint32{0}, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := &cancelInsideCall{Store: db, hold: *inFlight.TxIDChainHash(), cancel: cancel, seen: make(chan error, 1)}

	// Width 1: the queued transaction cannot start until the in-flight call
	// returns, by which time the cancel has landed.
	results, err := utxostore.DefaultSpendAndCreateMulti(ctx, store, utxostore.UniformSpendAndCreateMultiWidths(1),
		[]*bt.Tx{inFlight, queued}, height, utxostore.WithBelowCheckpoint(), utxostore.WithIgnoreLocked(true))

	require.NoError(t, <-store.seen, "the in-flight call must not see the caller's cancel")
	require.ErrorIs(t, err, context.Canceled)
	require.Len(t, results, 2)
	require.Equal(t, utxostore.MultiTxCreated, results[0].Status, "%v", results[0].Err)
	require.Equal(t, utxostore.MultiTxNotAttempted, results[1].Status)

	requireSpentBy(t, db, w.Roots[0], 0, inFlight, 0)
	requireNotStored(t, db, queued)
	requireUnspent(t, db, w.Roots[1], 0)
}

// SpendAndCreateMultiBelowCheckpointRollbackOutlivesDeadline: a combined call
// whose context ends during its create. When the create then definitely fails,
// the spends already made are rolled back all the same: the rollback runs on a
// context of its own, so the parent output ends unspent and the child has no
// record, and no parent names a spender that does not exist. This holds for the
// call's own bound in a list and for a caller's cancel of a direct
// SequentialSpendAndCreate. When the create fails with the context's error its
// outcome is unknown, since a batched create can still commit; the spends stay
// in place and a repeat of the list finishes the transaction.
//
// db must implement utxostore.SequentialStore, as the SQL and Aerospike stores
// do: the test runs utxostore.SequentialSpendAndCreate over db with a create
// that does not return until its context is done.
func SpendAndCreateMultiBelowCheckpointRollbackOutlivesDeadline(t *testing.T, db utxostore.Store) {
	const height = 860

	seq, ok := db.(utxostore.SequentialStore)
	require.True(t, ok, "the store must implement utxostore.SequentialStore")

	w := BuildMultiWorkload(t, 0xa7, 0, 3)
	w.StoreRoots(t, db, height-1)

	rejected := func(error) error { return errors.NewStorageError("create rejected") }
	unknown := func(ctxErr error) error { return errors.NewContextCanceledError("create did not finish", ctxErr) }

	bounded := utxostore.UniformSpendAndCreateMultiWidths(1)
	bounded.CallBound = 200 * time.Millisecond

	t.Run("call bound expires during a create that then fails", func(t *testing.T) {
		child := multiSpend(t, 1, []*bt.Tx{w.Roots[0]}, []uint32{0}, 1)
		store := &createUntilDone{SequentialStore: seq, fail: rejected}

		results, err := utxostore.DefaultSpendAndCreateMulti(context.Background(), store, bounded,
			[]*bt.Tx{child}, height, utxostore.WithBelowCheckpoint(), utxostore.WithIgnoreLocked(true))
		require.NoError(t, err)
		require.Len(t, results, 1)
		require.Equal(t, utxostore.MultiTxFailed, results[0].Status)
		require.True(t, store.created.Load(), "the create must have been reached, after the spend")

		// End state first: these are what a rollback cut short breaks.
		requireUnspent(t, db, w.Roots[0], 0)
		requireNotStored(t, db, child)

		require.NotContains(t, results[0].Err.Error(), "reversing", "the rollback must not fail")
		require.Empty(t, results[0].Spends, "a completed rollback leaves no spends in effect")
	})

	t.Run("caller cancels during a create that then fails", func(t *testing.T) {
		child := multiSpend(t, 2, []*bt.Tx{w.Roots[1]}, []uint32{0}, 1)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		store := &createUntilDone{SequentialStore: seq, fail: rejected, onCreate: cancel}

		_, spends, err := store.SpendAndCreate(ctx, child, height, utxostore.WithIgnoreLocked(true))
		require.True(t, store.created.Load(), "the create must have been reached, after the spend")

		// End state first: these are what a rollback cut short breaks.
		requireUnspent(t, db, w.Roots[1], 0)
		requireNotStored(t, db, child)

		require.ErrorIs(t, err, errors.ErrStorageError)
		require.NotContains(t, err.Error(), "reversing", "the rollback must not fail")
		require.Nil(t, spends, "a completed rollback leaves no spends in effect")
	})

	t.Run("call bound expires with the create's outcome unknown", func(t *testing.T) {
		child := multiSpend(t, 3, []*bt.Tx{w.Roots[2]}, []uint32{0}, 1)
		store := &createUntilDone{SequentialStore: seq, fail: unknown}

		results, err := utxostore.DefaultSpendAndCreateMulti(context.Background(), store, bounded,
			[]*bt.Tx{child}, height, utxostore.WithBelowCheckpoint(), utxostore.WithIgnoreLocked(true))
		require.NoError(t, err)
		require.Len(t, results, 1)
		require.Equal(t, utxostore.MultiTxFailed, results[0].Status)
		require.True(t, errors.IsContextError(results[0].Err), "%v", results[0].Err)
		require.Len(t, results[0].Spends, 1, "the spends of a create that may commit stay in effect")

		// The create may still commit, so the spend stays; here it never does.
		requireSpentBy(t, db, w.Roots[2], 0, child, 0)
		requireNotStored(t, db, child)

		// A repeat of the list finishes the transaction.
		results, err = db.SpendAndCreateMulti(context.Background(), []*bt.Tx{child}, height,
			utxostore.WithBelowCheckpoint(), utxostore.WithIgnoreLocked(true))
		require.NoError(t, err)
		require.Equal(t, utxostore.MultiTxCreated, results[0].Status, "%v", results[0].Err)

		requireSpentBy(t, db, w.Roots[2], 0, child, 0)

		_, err = db.Get(context.Background(), child.TxIDChainHash(), fields.Fee)
		require.NoError(t, err)
	})
}

// createUntilDone runs utxostore.SequentialSpendAndCreate over a real store
// whose create does not return until its context is done, and then fails with
// fail(ctx.Err()): a definite rejection, or the context's own error, as a create
// still waiting on its store's batch would return. onCreate, when set, runs as
// the create starts.
type createUntilDone struct {
	utxostore.SequentialStore
	fail     func(ctxErr error) error
	onCreate func()
	created  atomic.Bool
}

func (c *createUntilDone) Create(ctx context.Context, _ *bt.Tx, _ uint32, _ ...utxostore.CreateOption) (*meta.Data, error) {
	c.created.Store(true)

	if c.onCreate != nil {
		c.onCreate()
	}

	<-ctx.Done()

	return nil, c.fail(ctx.Err())
}

func (c *createUntilDone) SpendAndCreate(ctx context.Context, tx *bt.Tx, h uint32, opts ...utxostore.CreateOption) (*meta.Data, []*utxostore.Spend, error) {
	return utxostore.SequentialSpendAndCreate(ctx, ulogger.TestLogger{}, c, tx, h, opts...)
}

// cancelInsideCall cancels the caller's context from inside the call for hold,
// before that call reaches the store, and reports the context error the call
// then sees.
type cancelInsideCall struct {
	utxostore.Store
	hold   chainhash.Hash
	cancel context.CancelFunc
	seen   chan error
}

func (c *cancelInsideCall) SpendAndCreate(ctx context.Context, tx *bt.Tx, h uint32, opts ...utxostore.CreateOption) (*meta.Data, []*utxostore.Spend, error) {
	if *tx.TxIDChainHash() == c.hold {
		c.cancel()
		c.seen <- ctx.Err()
	}

	return c.Store.SpendAndCreate(ctx, tx, h, opts...)
}

// requireUnspent checks that output vout of parent has no spender.
func requireUnspent(t testing.TB, db utxostore.Store, parent *bt.Tx, vout uint32) {
	t.Helper()

	utxoHash, err := util.UTXOHashFromOutput(parent.TxIDChainHash(), parent.Outputs[vout], vout)
	require.NoError(t, err)

	resp, err := db.GetSpend(context.Background(), &utxostore.Spend{TxID: parent.TxIDChainHash(), Vout: vout, UTXOHash: utxoHash})
	require.NoError(t, err)
	require.Nil(t, resp.SpendingData, "%s:%d is spent", parent.TxIDChainHash(), vout)
}
