package tests

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/stretchr/testify/require"
)

// These are DefaultSpendAndCreateMulti's contract tests on a real store: the
// refusals, the level schedule, the result mapping, option pass-through,
// cancellation and parent deduplication, each checked against what the store
// holds afterwards, not only against what the call returned.

// recordingStore wraps a real store for DefaultSpendAndCreateMulti and records
// when every SpendAndCreate call starts and ends. delay holds each call before
// it reaches the store, so calls that can overlap visibly do.
type recordingStore struct {
	utxostore.Store
	delay time.Duration

	mu    sync.Mutex
	calls map[chainhash.Hash]recordedCall
}

type recordedCall struct {
	start, end time.Time
}

func newRecordingStore(db utxostore.Store, delay time.Duration) *recordingStore {
	return &recordingStore{Store: db, delay: delay, calls: map[chainhash.Hash]recordedCall{}}
}

func (r *recordingStore) SpendAndCreate(ctx context.Context, tx *bt.Tx, h uint32, opts ...utxostore.CreateOption) (*meta.Data, []*utxostore.Spend, error) {
	start := time.Now()

	if r.delay > 0 {
		time.Sleep(r.delay)
	}

	md, spends, err := r.Store.SpendAndCreate(ctx, tx, h, opts...)

	r.mu.Lock()
	r.calls[*tx.TxIDChainHash()] = recordedCall{start: start, end: time.Now()}
	r.mu.Unlock()

	return md, spends, err
}

func (r *recordingStore) call(t testing.TB, tx *bt.Tx) recordedCall {
	t.Helper()

	r.mu.Lock()
	defer r.mu.Unlock()

	c, ok := r.calls[*tx.TxIDChainHash()]
	require.True(t, ok, "no SpendAndCreate call for %s", tx.TxIDChainHash())

	return c
}

// multiSpend builds a transaction spending the given outputs of parents, each
// input extended from its parent, with nOutputs outputs sharing everything but a
// 200-satoshi fee. lockTime makes two otherwise identical spends distinct.
func multiSpend(t testing.TB, lockTime uint32, parents []*bt.Tx, vouts []uint32, nOutputs int) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()
	tx.LockTime = lockTime

	var in uint64

	for n, p := range parents {
		addMultiInput(t, tx, p, vouts[n])
		in += p.Outputs[vouts[n]].Satoshis
	}

	each := (in - 200) / uint64(nOutputs) //nolint:gosec // test data
	for o := 0; o < nOutputs; o++ {
		require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", each))
	}

	return tx
}

// requireNotStored checks that no record exists for any of txs.
func requireNotStored(t testing.TB, db utxostore.Store, txs ...*bt.Tx) {
	t.Helper()

	for i, tx := range txs {
		if tx == nil {
			continue
		}

		_, err := db.Get(context.Background(), tx.TxIDChainHash(), fields.Fee)
		require.ErrorIs(t, err, errors.ErrTxNotFound, "tx %d must not be written", i)
	}
}

// requireSpentBy checks that output vout of parent is spent by input vin of child.
func requireSpentBy(t testing.TB, db utxostore.Store, parent *bt.Tx, vout uint32, child *bt.Tx, vin int) {
	t.Helper()

	utxoHash, err := util.UTXOHashFromOutput(parent.TxIDChainHash(), parent.Outputs[vout], vout)
	require.NoError(t, err)

	resp, err := db.GetSpend(context.Background(), &utxostore.Spend{TxID: parent.TxIDChainHash(), Vout: vout, UTXOHash: utxoHash})
	require.NoError(t, err)
	require.NotNil(t, resp.SpendingData, "%s:%d is unspent", parent.TxIDChainHash(), vout)
	require.Equal(t, *child.TxIDChainHash(), *resp.SpendingData.TxID, "%s:%d", parent.TxIDChainHash(), vout)
	require.Equal(t, vin, resp.SpendingData.Vin, "%s:%d", parent.TxIDChainHash(), vout)
}

// SpendAndCreateMultiRefusals: every list the contract refuses is refused before
// any write, with a fully formatted message, and leaves the store untouched.
func SpendAndCreateMultiRefusals(t *testing.T, db utxostore.Store) {
	ctx := context.Background()

	const height = 700

	coinbase := bt.NewTx()
	require.NoError(t, coinbase.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0))
	coinbase.Inputs[0].UnlockingScript = dummyUnlockingScript
	require.NoError(t, coinbase.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000))
	require.True(t, coinbase.IsCoinbase())

	cases := []struct {
		name  string
		build func(w *MultiWorkload) ([]*bt.Tx, []utxostore.CreateOption)
	}{
		{"child before parent", func(w *MultiWorkload) ([]*bt.Tx, []utxostore.CreateOption) {
			return []*bt.Tx{w.Txs[2], w.Txs[0], w.Txs[1]}, nil
		}},
		{"duplicate outpoint", func(w *MultiWorkload) ([]*bt.Tx, []utxostore.CreateOption) {
			return []*bt.Tx{
				multiSpend(t, 1, []*bt.Tx{w.Roots[0]}, []uint32{0}, 1),
				multiSpend(t, 2, []*bt.Tx{w.Roots[0]}, []uint32{0}, 1),
			}, nil
		}},
		{"output index past a parent in the list", func(w *MultiWorkload) ([]*bt.Tx, []utxostore.CreateOption) {
			child := multiSpend(t, 3, []*bt.Tx{w.Txs[0]}, []uint32{0}, 1)
			child.Inputs[0].PreviousTxOutIndex = uint32(len(w.Txs[0].Outputs)) //nolint:gosec // test data

			return []*bt.Tx{w.Txs[0], child}, nil
		}},
		{"the same transaction twice", func(w *MultiWorkload) ([]*bt.Tx, []utxostore.CreateOption) {
			return []*bt.Tx{w.Txs[0], w.Txs[0]}, nil
		}},
		{"a coinbase", func(_ *MultiWorkload) ([]*bt.Tx, []utxostore.CreateOption) {
			return []*bt.Tx{coinbase}, nil
		}},
		{"WithTXID", func(w *MultiWorkload) ([]*bt.Tx, []utxostore.CreateOption) {
			return w.Txs[:1], []utxostore.CreateOption{utxostore.WithTXID(w.Txs[0].TxIDChainHash())}
		}},
		{"WithSetCoinbase", func(w *MultiWorkload) ([]*bt.Tx, []utxostore.CreateOption) {
			return w.Txs[:1], []utxostore.CreateOption{utxostore.WithSetCoinbase(false)}
		}},
		{"WithTXIDs of the wrong length", func(w *MultiWorkload) ([]*bt.Tx, []utxostore.CreateOption) {
			return w.Txs[:2], []utxostore.CreateOption{utxostore.WithTXIDs([]chainhash.Hash{*w.Txs[0].TxIDChainHash()})}
		}},
		{"WithCreateOnly and WithSpendOnly", func(w *MultiWorkload) ([]*bt.Tx, []utxostore.CreateOption) {
			return w.Txs[:1], []utxostore.CreateOption{utxostore.WithCreateOnly(), utxostore.WithSpendOnly()}
		}},
		{"WithCreateOnly", func(w *MultiWorkload) ([]*bt.Tx, []utxostore.CreateOption) {
			return w.Txs[:1], []utxostore.CreateOption{utxostore.WithCreateOnly()}
		}},
		{"WithSpendOnly", func(w *MultiWorkload) ([]*bt.Tx, []utxostore.CreateOption) {
			return w.Txs[:1], []utxostore.CreateOption{utxostore.WithSpendOnly()}
		}},
		{"a nil transaction", func(w *MultiWorkload) ([]*bt.Tx, []utxostore.CreateOption) {
			return []*bt.Tx{w.Txs[0], nil}, nil
		}},
		{"a nil transaction with WithTXIDs", func(_ *MultiWorkload) ([]*bt.Tx, []utxostore.CreateOption) {
			return []*bt.Tx{nil}, []utxostore.CreateOption{utxostore.WithTXIDs([]chainhash.Hash{{0x01}})}
		}},
	}

	for n, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := BuildMultiWorkload(t, byte(0x70+n), 2, 2) //nolint:gosec // test data
			w.StoreRoots(t, db, height-1)

			txs, opts := tc.build(w)

			results, err := db.SpendAndCreateMulti(ctx, txs, height, opts...)
			require.Error(t, err)
			require.True(t, utxostore.IsSpendAndCreateMultiRefused(err), "want a refusal, got %v", err)
			require.NotContains(t, err.Error(), "%v", "the refusal message must be fully formatted")
			require.NotContains(t, err.Error(), "%!", "the refusal message must be fully formatted")
			require.Nil(t, results)

			requireNotStored(t, db, txs...)
			requireNotStored(t, db, w.Txs...)

			for i, s := range w.RootSpends(t, db) {
				require.Empty(t, s, "root output %d was spent by a refused list", i)
			}
		})
	}
}

// SpendAndCreateMultiEmptyList: an empty list is no work and no error.
func SpendAndCreateMultiEmptyList(t *testing.T, db utxostore.Store) {
	results, err := db.SpendAndCreateMulti(context.Background(), nil, 710)
	require.NoError(t, err)
	require.Empty(t, results)
}

// SpendAndCreateMultiLevelsParallelAndOrdered: the calls of one dependency level
// overlap, and no call starts before every call of the level above it has
// returned. Every record is in the store afterwards.
func SpendAndCreateMultiLevelsParallelAndOrdered(t *testing.T, db utxostore.Store) {
	const (
		height = 720
		levels = 4
		width  = 6
	)

	w := BuildMultiWorkload(t, 0x90, levels, width)
	w.StoreRoots(t, db, height-1)

	rec := newRecordingStore(db, 20*time.Millisecond)

	results, err := utxostore.DefaultSpendAndCreateMulti(context.Background(), rec, width, w.Txs, height, utxostore.WithIgnoreLocked(true))
	require.NoError(t, err)
	require.Len(t, results, len(w.Txs))
	requireAllStatus(t, results, utxostore.MultiTxCreated)

	for i, r := range w.Records(t, db) {
		require.True(t, r.ExistsInStore, "tx %d", i)
	}

	for level := 0; level < levels; level++ {
		row := w.Txs[level*width : (level+1)*width]

		var latestStart, earliestEnd time.Time

		for i, tx := range row {
			c := rec.call(t, tx)

			if i == 0 || c.start.After(latestStart) {
				latestStart = c.start
			}

			if i == 0 || c.end.Before(earliestEnd) {
				earliestEnd = c.end
			}

			if level == 0 {
				continue
			}

			for _, p := range w.Txs[(level-1)*width : level*width] {
				require.False(t, c.start.Before(rec.call(t, p).end), "level %d call started before a level %d call returned", level, level-1)
			}
		}

		require.True(t, latestStart.Before(earliestEnd), "level %d calls did not overlap: it ran as a loop", level)
	}
}

// SpendAndCreateMultiConcurrencyBound: within a level, no more calls are in
// flight than the concurrency allows, and every record is still written.
func SpendAndCreateMultiConcurrencyBound(t *testing.T, db utxostore.Store) {
	const (
		height = 730
		bound  = 3
	)

	w := BuildMultiWorkload(t, 0x91, 1, 12)
	w.StoreRoots(t, db, height-1)

	rec := newRecordingStore(db, 10*time.Millisecond)

	results, err := utxostore.DefaultSpendAndCreateMulti(context.Background(), rec, bound, w.Txs, height, utxostore.WithIgnoreLocked(true))
	require.NoError(t, err)
	requireAllStatus(t, results, utxostore.MultiTxCreated)

	for _, tx := range w.Txs {
		c := rec.call(t, tx)
		inFlight := 0

		for _, other := range w.Txs {
			o := rec.call(t, other)
			if !o.start.After(c.start) && o.end.After(c.start) {
				inFlight++
			}
		}

		require.LessOrEqual(t, inFlight, bound)
	}

	for i, r := range w.Records(t, db) {
		require.True(t, r.ExistsInStore, "tx %d", i)
	}
}

// SpendAndCreateMultiResultMapping: each outcome a real store produces maps to
// its status. A spend of an output someone else already spent is Failed with
// ErrSpent and its spends; its descendants are ParentFailed and never written;
// a record that already exists is Existed with its spends, and its child is
// still written; an unrelated transaction is Created.
func SpendAndCreateMultiResultMapping(t *testing.T, db utxostore.Store) {
	ctx := context.Background()

	const height = 740

	w := BuildMultiWorkload(t, 0x92, 1, 3)
	w.StoreRoots(t, db, height-1)

	thief := multiSpend(t, 1, []*bt.Tx{w.Roots[0]}, []uint32{0}, 1)
	_, _, err := db.SpendAndCreate(ctx, thief, height-1)
	require.NoError(t, err)

	a := multiSpend(t, 2, []*bt.Tx{w.Roots[0]}, []uint32{0}, 2) // its input is the thief's
	b := multiSpend(t, 3, []*bt.Tx{a}, []uint32{0}, 1)          // child of a
	c := multiSpend(t, 4, []*bt.Tx{b}, []uint32{0}, 1)          // grandchild of a
	d := multiSpend(t, 5, []*bt.Tx{w.Roots[1]}, []uint32{0}, 2) // created beforehand
	e := multiSpend(t, 6, []*bt.Tx{d}, []uint32{0}, 1)          // child of the existing d
	f := multiSpend(t, 7, []*bt.Tx{w.Roots[2]}, []uint32{0}, 1) // unrelated

	_, _, err = db.SpendAndCreate(ctx, d, height-1)
	require.NoError(t, err)

	results, err := db.SpendAndCreateMulti(ctx, []*bt.Tx{a, b, c, d, e, f}, height, utxostore.WithIgnoreLocked(true))
	require.NoError(t, err)
	require.Len(t, results, 6)

	require.Equal(t, utxostore.MultiTxFailed, results[0].Status)
	require.ErrorIs(t, results[0].Err, errors.ErrSpent)
	require.NotEmpty(t, results[0].Spends, "a failed spend reports its spends")

	require.Equal(t, utxostore.MultiTxParentFailed, results[1].Status)
	require.Error(t, results[1].Err)
	require.Equal(t, utxostore.MultiTxParentFailed, results[2].Status)
	require.Error(t, results[2].Err)

	require.Equal(t, utxostore.MultiTxExisted, results[3].Status)
	require.NoError(t, results[3].Err)
	require.NotEmpty(t, results[3].Spends, "an existing record's spends stay in place")

	require.Equal(t, utxostore.MultiTxCreated, results[4].Status, "%v", results[4].Err)
	require.Equal(t, utxostore.MultiTxCreated, results[5].Status, "%v", results[5].Err)

	requireNotStored(t, db, a, b, c)
	requireSpentBy(t, db, w.Roots[0], 0, thief, 0)
	requireSpentBy(t, db, d, 0, e, 0)
	requireSpentBy(t, db, w.Roots[2], 0, f, 0)
}

// SpendAndCreateMultiOptionsPassThrough: options apply to every transaction as
// SpendAndCreate applies them. WithTXIDs becomes each transaction's WithTXID, so
// deliberately wrong txids put the records under those ids; the record flags and
// mined-block info land on every record.
func SpendAndCreateMultiOptionsPassThrough(t *testing.T, db utxostore.Store) {
	ctx := context.Background()

	const height = 750

	t.Run("WithTXIDs", func(t *testing.T) {
		w := BuildMultiWorkload(t, 0x93, 1, 2)
		w.StoreRoots(t, db, height-1)

		ids := []chainhash.Hash{{0x93, 0x0a}, {0x93, 0x0b}}

		results, err := db.SpendAndCreateMulti(ctx, w.Txs, height, utxostore.WithTXIDs(ids), utxostore.WithIgnoreLocked(true))
		require.NoError(t, err)
		requireAllStatus(t, results, utxostore.MultiTxCreated)

		for i := range ids {
			_, err := db.Get(ctx, &ids[i], fields.Fee)
			require.NoError(t, err, "record %d must be under the supplied txid", i)
		}

		requireNotStored(t, db, w.Txs...)
	})

	t.Run("record flags and mined-block info", func(t *testing.T) {
		w := BuildMultiWorkload(t, 0x94, 2, 2)
		w.StoreRoots(t, db, height-1)

		results, err := db.SpendAndCreateMulti(ctx, w.Txs, height, utxostore.WithLocked(true), utxostore.WithIgnoreLocked(true),
			utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{BlockID: 7, BlockHeight: height, SubtreeIdx: 1}))
		require.NoError(t, err)
		requireAllStatus(t, results, utxostore.MultiTxCreated)

		for i, r := range w.Records(t, db) {
			require.True(t, r.Locked, "tx %d", i)
			require.Equal(t, []uint32{7}, r.BlockIDs, "tx %d", i)
			require.Equal(t, []uint32{height}, r.BlockHeights, "tx %d", i)
		}
	})

	t.Run("WithConflicting", func(t *testing.T) {
		w := BuildMultiWorkload(t, 0x95, 1, 2)
		w.StoreRoots(t, db, height-1)

		results, err := db.SpendAndCreateMulti(ctx, w.Txs, height, utxostore.WithConflicting(true), utxostore.WithIgnoreLocked(true))
		require.NoError(t, err)
		requireAllStatus(t, results, utxostore.MultiTxCreated)

		for i, r := range w.Records(t, db) {
			require.True(t, r.Conflicting, "tx %d", i)
		}
	})
}

// cancellingStore cancels the list's context once its first SpendAndCreate call
// has written its record.
type cancellingStore struct {
	utxostore.Store
	cancel context.CancelFunc
}

func (c *cancellingStore) SpendAndCreate(ctx context.Context, tx *bt.Tx, h uint32, opts ...utxostore.CreateOption) (*meta.Data, []*utxostore.Spend, error) {
	defer c.cancel()

	return c.Store.SpendAndCreate(ctx, tx, h, opts...)
}

// SpendAndCreateMultiCancelledBetweenLevels: a cancelled context stops the list
// between levels. The level that ran is written, the next is NotAttempted and
// absent from the store, and the call reports the context error.
func SpendAndCreateMultiCancelledBetweenLevels(t *testing.T, db utxostore.Store) {
	const height = 760

	w := BuildMultiWorkload(t, 0x96, 2, 1)
	w.StoreRoots(t, db, height-1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	results, err := utxostore.DefaultSpendAndCreateMulti(ctx, &cancellingStore{Store: db, cancel: cancel}, 4, w.Txs, height, utxostore.WithIgnoreLocked(true))
	require.ErrorIs(t, err, context.Canceled)
	require.Len(t, results, 2)
	require.Equal(t, utxostore.MultiTxCreated, results[0].Status)
	require.Equal(t, utxostore.MultiTxNotAttempted, results[1].Status)

	records := w.Records(t, db)
	require.True(t, records[0].ExistsInStore)
	require.False(t, records[1].ExistsInStore, "a transaction never attempted must not be written")
}

// SpendAndCreateMultiParentsDeduplicated: a child spending several outputs of
// the same parents in the list, in an interleaved order, is written after all of
// them and spends every one of those outputs with the right input; its own child
// follows.
func SpendAndCreateMultiParentsDeduplicated(t *testing.T, db utxostore.Store) {
	const height = 770

	w := BuildMultiWorkload(t, 0x97, 0, 3)
	w.StoreRoots(t, db, height-1)

	a := multiSpend(t, 1, []*bt.Tx{w.Roots[0]}, []uint32{0}, 3)
	b := multiSpend(t, 2, []*bt.Tx{w.Roots[1]}, []uint32{0}, 2)
	c := multiSpend(t, 3, []*bt.Tx{w.Roots[2]}, []uint32{0}, 1)
	child := multiSpend(t, 4, []*bt.Tx{b, a, b, a, c, a}, []uint32{0, 0, 1, 2, 0, 1}, 1)
	grandchild := multiSpend(t, 5, []*bt.Tx{child}, []uint32{0}, 1)

	results, err := db.SpendAndCreateMulti(context.Background(), []*bt.Tx{a, b, c, child, grandchild}, height, utxostore.WithIgnoreLocked(true))
	require.NoError(t, err)
	requireAllStatus(t, results, utxostore.MultiTxCreated)

	requireSpentBy(t, db, b, 0, child, 0)
	requireSpentBy(t, db, a, 0, child, 1)
	requireSpentBy(t, db, b, 1, child, 2)
	requireSpentBy(t, db, a, 2, child, 3)
	requireSpentBy(t, db, c, 0, child, 4)
	requireSpentBy(t, db, a, 1, child, 5)
	requireSpentBy(t, db, child, 0, grandchild, 0)
}
