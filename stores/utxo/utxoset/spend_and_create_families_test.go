package utxoset

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// setChunkTxs bounds a chunk to n transactions for the test.
func setChunkTxs(t *testing.T, n int) {
	t.Helper()

	old := multiCreateChunkTxs
	multiCreateChunkTxs = n

	t.Cleanup(func() { multiCreateChunkTxs = old })
}

// A family cut across chunks: the cut edge's parent output is a real coin until its child's chunk
// spends it, every other in-chunk edge is netted, and both halves end up stored, with the records
// a per-transaction write leaves.
func TestSpendAndCreateMultiACutEdgeWritesTheParentsOutputAsACoin(t *testing.T) {
	const (
		height = 1600
		n      = 6
	)

	setChunkTxs(t, 3)

	s, ctx := newUncheckpointedStore(t)

	wantRecs, wantRoots := perTxReference(t, s, buildChain(t, 0x11, n, nil), height)

	w := buildChain(t, 0x12, n, nil)
	w.StoreRoots(t, s, height-1)

	var (
		mu       sync.Mutex
		observed bool
	)

	// Chunk 1 holds the second half and has not opened its transaction yet at "locked" of its own
	// round, so what chunk 0 committed is exactly what is visible.
	multiChunkFault = func(chunk int, stage string) error {
		if chunk != 1 || stage != "locked" {
			return nil
		}

		mu.Lock()
		defer mu.Unlock()

		observed = true

		// Chunk 0 is [0 1 2]. Edges 0 to 1 and 1 to 2 are inside it, so tx0's and tx1's outputs
		// were netted: no coin row, a journal row naming the child.
		for i := 0; i < 2; i++ {
			require.Equal(t, 0, utxoCount(t, s, ctx, w.Txs[i]), "tx %d output was netted", i)
			require.Equal(t, 1, journalRowsFor(t, s, ctx, w.Txs[i], 0), "tx %d output is journaled for its child", i)
		}

		// Edge 2 to 3 is the cut edge: tx2's output is a real coin, with no journal row, until
		// tx3's chunk spends it.
		require.Equal(t, 1, utxoCount(t, s, ctx, w.Txs[2]), "the cut edge's parent output is a coin")
		require.Equal(t, 0, journalRowsFor(t, s, ctx, w.Txs[2], 0))
		require.Equal(t, 1, identityCount(t, s, ctx, w.Txs[2]), "the parent chunk committed before the child's began")
		require.Equal(t, 0, identityCount(t, s, ctx, w.Txs[3]))

		return nil
	}

	t.Cleanup(func() { multiChunkFault = nil })

	results, err := s.SpendAndCreateMulti(ctx, w.Txs, height, utxo.WithIgnoreLocked(true))
	multiChunkFault = nil
	require.NoError(t, err)
	requireAllCreated(t, results)
	require.True(t, observed, "chunk 1 never ran")

	// The child's chunk spent the coin for real: gone, journaled, naming tx3.
	require.Equal(t, 0, utxoCount(t, s, ctx, w.Txs[2]))
	require.Equal(t, 1, journalRowsFor(t, s, ctx, w.Txs[2], 0))
	require.Equal(t, 1, identityCount(t, s, ctx, w.Txs[3]))

	require.Equal(t, wantRecs, recordsOf(t, s, w))
	require.Equal(t, wantRoots, w.RootSpends(t, s))
}

func requireAllCreated(t *testing.T, results []utxo.SpendAndCreateMultiResult) {
	t.Helper()

	for i, r := range results {
		require.Equal(t, utxo.MultiTxCreated, r.Status, "tx %d: %v", i, r.Err)
	}
}

// A transaction that fails in one chunk gives ParentFailed to its descendants in that chunk and
// in the chunks that depend on it, and the chunks that committed before it stay committed. The
// dependent chunk opens no database transaction.
func TestSpendAndCreateMultiAFailedChunkGivesParentFailedToItsDependants(t *testing.T) {
	const height = 1700

	setChunkTxs(t, 2)

	s, ctx := newUncheckpointedStore(t)

	w := tests.BuildMultiWorkload(t, 0xa5, 1, 2)
	w.StoreRoots(t, s, height-1)
	rootA, rootB := w.Roots[0], w.Roots[1]

	thief := payFrom(t, 1, []*bt.Tx{rootB}, []uint32{0})
	_, _, err := s.SpendAndCreate(ctx, thief, height)
	require.NoError(t, err)

	ok := payFrom(t, 2, []*bt.Tx{rootA}, []uint32{0})      // independent, written
	failing := payFrom(t, 3, []*bt.Tx{rootB}, []uint32{0}) // rootB:0 is taken, so it fails
	c1 := payFrom(t, 4, []*bt.Tx{failing}, []uint32{0})    // same chunk as its failed parent
	c2 := payFrom(t, 5, []*bt.Tx{c1}, []uint32{0})         // in the next chunk, across a cut edge

	// Chunk 0 is [ok], chunk 1 is [failing c1], chunk 2 is [c2].
	var (
		mu   sync.Mutex
		seen = map[int]bool{}
	)

	multiChunkFault = func(chunk int, _ string) error {
		mu.Lock()
		seen[chunk] = true
		mu.Unlock()

		return nil
	}

	t.Cleanup(func() { multiChunkFault = nil })

	results, err := s.SpendAndCreateMulti(ctx, []*bt.Tx{ok, failing, c1, c2}, height, utxo.WithIgnoreLocked(true))
	multiChunkFault = nil
	require.NoError(t, err)

	require.Equal(t, utxo.MultiTxCreated, results[0].Status, "%v", results[0].Err)
	require.Equal(t, utxo.MultiTxFailed, results[1].Status)
	require.ErrorIs(t, results[1].Err, errors.ErrSpent)
	require.Equal(t, utxo.MultiTxParentFailed, results[2].Status)
	require.Equal(t, utxo.MultiTxParentFailed, results[3].Status, "the dependent chunk's transaction")

	require.Equal(t, 1, identityCount(t, s, ctx, ok), "the chunk that committed before the failure stays committed")
	require.Equal(t, 0, identityCount(t, s, ctx, failing))
	require.Equal(t, 0, identityCount(t, s, ctx, c1))
	require.Equal(t, 0, identityCount(t, s, ctx, c2))

	mu.Lock()
	require.True(t, seen[0] && seen[1], "chunks 0 and 1 ran")
	require.False(t, seen[2], "chunk 2 had nothing live and opened no database transaction")
	mu.Unlock()
}

// An output a child in the chunk would have netted is a real coin when the child fails: the
// parent is written, the child is not, and the output is spendable by anyone.
func TestSpendAndCreateMultiAFailedChildLeavesItsParentsOutputACoin(t *testing.T) {
	const height = 1800

	s, ctx := newUncheckpointedStore(t)

	w := tests.BuildMultiWorkload(t, 0xa6, 1, 2)
	w.StoreRoots(t, s, height-1)
	rootA, rootB := w.Roots[0], w.Roots[1]

	thief := payFrom(t, 1, []*bt.Tx{rootB}, []uint32{0})
	_, _, err := s.SpendAndCreate(ctx, thief, height)
	require.NoError(t, err)

	parent := payFrom(t, 2, []*bt.Tx{rootA}, []uint32{0})
	child := payFrom(t, 3, []*bt.Tx{parent, rootB}, []uint32{0, 0}) // rootB:0 is taken: child fails

	results, err := s.SpendAndCreateMulti(ctx, []*bt.Tx{parent, child}, height, utxo.WithIgnoreLocked(true))
	require.NoError(t, err)
	require.Equal(t, utxo.MultiTxCreated, results[0].Status, "%v", results[0].Err)
	require.Equal(t, utxo.MultiTxFailed, results[1].Status)

	require.Equal(t, 1, identityCount(t, s, ctx, parent))
	require.Equal(t, 0, identityCount(t, s, ctx, child))
	require.Equal(t, 1, utxoCount(t, s, ctx, parent), "the output the child would have netted is a coin")
	require.Equal(t, 0, journalRowsFor(t, s, ctx, parent, 0), "no journal row names a child that was not written")

	other := payFrom(t, 4, []*bt.Tx{parent}, []uint32{0})
	_, _, err = s.SpendAndCreate(ctx, other, height)
	require.NoError(t, err, "a different transaction spends the parent's output")
}

// A chunk waits for the commit of the chunk that holds the parent of one of its cut edges.
func TestSpendAndCreateMultiAChunkWaitsForItsCutEdgeParents(t *testing.T) {
	const height = 1900

	setChunkTxs(t, 1)

	s, ctx := newUncheckpointedStore(t)

	w := buildChain(t, 0x21, 3, nil)
	w.StoreRoots(t, s, height-1)

	var (
		mu     sync.Mutex
		events []string
	)

	note := func(e string) {
		mu.Lock()
		events = append(events, e)
		mu.Unlock()
	}

	multiChunkFault = func(chunk int, stage string) error {
		if chunk == 0 && stage == "spent" {
			// Long enough for a child chunk that did not wait to start and finish.
			time.Sleep(300 * time.Millisecond)
		}

		if chunk >= 0 && (stage == "locked" || stage == "committed") {
			note("chunk " + string(rune('0'+chunk)) + " " + stage)
		}

		return nil
	}

	t.Cleanup(func() { multiChunkFault = nil })

	results, err := s.SpendAndCreateMulti(ctx, w.Txs, height, utxo.WithIgnoreLocked(true))
	multiChunkFault = nil
	require.NoError(t, err)
	requireAllCreated(t, results)

	index := map[string]int{}
	for i, e := range events {
		index[e] = i
	}

	require.Less(t, index["chunk 0 committed"], index["chunk 1 locked"], "chunk 1 began before its parent's chunk committed")
	require.Less(t, index["chunk 1 committed"], index["chunk 2 locked"], "chunk 2 began before its parent's chunk committed")
}

// Chunks with no edge between them run in parallel, up to the concurrency bound.
func TestSpendAndCreateMultiIndependentChunksRunInParallel(t *testing.T) {
	const (
		height = 2000
		chains = 4
	)

	setChunkTxs(t, 1)

	s, ctx := newUncheckpointedStore(t)

	// Four independent transactions, one per chunk.
	var txs []*bt.Tx

	for i := 0; i < chains; i++ {
		c := buildChain(t, byte(0x30+i), 1, nil)
		c.StoreRoots(t, s, height-1)
		txs = append(txs, c.Txs...)
	}

	var (
		mu      sync.Mutex
		running int
		peak    int
	)

	multiChunkFault = func(_ int, stage string) error {
		switch stage {
		case "locked":
			mu.Lock()
			running++
			peak = max(peak, running)
			mu.Unlock()

			time.Sleep(150 * time.Millisecond)
		case "created":
			mu.Lock()
			running--
			mu.Unlock()
		}

		return nil
	}

	t.Cleanup(func() { multiChunkFault = nil })

	results, err := s.SpendAndCreateMulti(ctx, txs, height, utxo.WithIgnoreLocked(true))
	multiChunkFault = nil
	require.NoError(t, err)
	requireAllCreated(t, results)

	mu.Lock()
	defer mu.Unlock()
	require.Greater(t, peak, 1, "independent chunks ran one at a time")
}

// A list whose families fit in chunks writes no coin row for any output spent inside its chunk:
// netting is intact. Three chains of 90 make two chunks with no edge cut, so the only coin rows
// are the last output of each chain, and every other output is a journal row naming its child.
func TestSpendAndCreateMultiFamiliesInOneChunkAreNetted(t *testing.T) {
	const (
		height = 2100
		chains = 3
		length = 90
	)

	s, ctx := newUncheckpointedStore(t)

	var txs []*bt.Tx

	for i := 0; i < chains; i++ {
		c := buildChain(t, byte(0x40+i), length, nil)
		c.StoreRoots(t, s, height-1)
		txs = append(txs, c.Txs...)
	}

	commits := countCreateCommits(t)

	results, err := s.SpendAndCreateMulti(ctx, txs, height, utxo.WithIgnoreLocked(true))
	require.NoError(t, err)
	requireAllCreated(t, results)
	require.ElementsMatch(t, []int{2 * length, length}, commits(), "two chunks of whole chains, no edge cut")

	coinRows := 0
	for _, tx := range txs {
		coinRows += utxoRowsOf(t, s, tx)
	}

	require.Equal(t, chains, coinRows, "only the last output of each chain is a coin row")

	var journaled int

	for i, tx := range txs {
		if i%length == length-1 {
			continue
		}

		journaled += journalRowsFor(t, s, ctx, tx, 0)
	}

	require.Equal(t, chains*(length-1), journaled, "every output spent inside the list is one journal row")
}

// A real deadlock, not an injected one: a rival holds the coin the chunk needs and then asks for
// the advisory lock the chunk holds on its own transaction. PostgreSQL stops the chunk, the first
// of the two to wait, with SQLSTATE 40P01. The chunk is run again, finds the coin free once the
// rival lets go, and the list is written whole.
func TestSpendAndCreateMultiAChunkDeadlockIsRetried(t *testing.T) {
	s, ctx := newUncheckpointedStore(t)

	parent := storedParent(t, s, ctx)
	child := spendOutput(t, parent, 0, 1)

	childID := child.TxIDChainHash()
	parentID := parent.TxIDChainHash()

	rival, err := s.pool.Begin(ctx)
	require.NoError(t, err)

	t.Cleanup(func() { _ = rival.Rollback(context.Background()) })

	// The rival takes the coin the chunk will spend.
	var n int
	require.NoError(t, rival.QueryRow(ctx,
		`SELECT count(*) FROM (SELECT 1 FROM utxo WHERE leaf = $1 AND ukey = $2 AND txid = $3 FOR UPDATE) c`,
		LeafFor(parentID[:]), Pack(parentID[:], 0), parentID[:]).Scan(&n))
	require.Equal(t, 1, n)

	before := testutil.ToFloat64(deadlockRetried.WithLabelValues("multi_chunk"))

	rivalDone := make(chan error, 1)
	fired := false

	multiChunkFault = func(_ int, stage string) error {
		if stage != "locked" || fired {
			return nil
		}

		fired = true

		go func() {
			// The chunk's spend has been waiting on the coin for a while by now. Ask for the lock
			// it holds, which closes the cycle, then let go of everything.
			time.Sleep(300 * time.Millisecond)

			_, err := rival.Exec(context.Background(),
				`SELECT pg_advisory_xact_lock($1)`, int64(binaryBigEndian(childID[:8]))) //nolint:gosec // a hash prefix as a lock key
			if err == nil {
				err = rival.Rollback(context.Background())
			}

			rivalDone <- err
		}()

		return nil
	}

	t.Cleanup(func() { multiChunkFault = nil })

	results, err := s.SpendAndCreateMulti(ctx, []*bt.Tx{child}, 1_100, utxo.WithIgnoreLocked(true))
	multiChunkFault = nil
	require.NoError(t, err, "a deadlock must never fail the list")
	require.Equal(t, utxo.MultiTxCreated, results[0].Status, "%v", results[0].Err)

	require.NoError(t, <-rivalDone)
	require.GreaterOrEqual(t, testutil.ToFloat64(deadlockRetried.WithLabelValues("multi_chunk"))-before, 1.0,
		"the chunk was not stopped as a deadlock, so the retry was not exercised")

	requireCoinStaysSpentByChild(t, s, ctx, parent, child)
}

// binaryBigEndian is the advisory lock key lockTxids derives from a transaction id.
func binaryBigEndian(b []byte) uint64 {
	var v uint64
	for _, c := range b[:8] {
		v = v<<8 | uint64(c)
	}

	return v
}

// The two writers meet at the checkpoint: a quick-validation-shaped list carrying a block at or
// below it is written by the below-checkpoint writer, and every other list by family chunks.
func TestSpendAndCreateMultiRoutesByTheCheckpoint(t *testing.T) {
	var chunks, below int

	hook := func() {
		multiChunkFault = func(int, string) error {
			chunks++
			return nil
		}
		nettedBelowFault = func([]int) error {
			below++
			return nil
		}
	}

	t.Cleanup(func() { multiChunkFault, nettedBelowFault = nil, nil })

	t.Run("a block at or below the checkpoint takes the below-checkpoint writer", func(t *testing.T) {
		s, ctx := newTestStore(t) // mainnet checkpoints: 800 is below them

		w := tests.BuildMultiWorkload(t, 0x61, 2, 3)
		w.StoreRoots(t, s, 799)

		chunks, below = 0, 0
		hook()

		results, err := s.SpendAndCreateMulti(ctx, outpointOnly(w.Txs), 800, belowCheckpointOptions(800)...)
		require.NoError(t, err)
		requireAllCreated(t, results)

		require.Positive(t, below, "the below-checkpoint writer ran")
		require.Zero(t, chunks, "family chunks did not")
	})

	t.Run("a block above the checkpoint takes family chunks", func(t *testing.T) {
		s, ctx := newTestStore(t)

		const height = testCheckpointHeight + 100

		reference := tests.BuildMultiWorkload(t, 0x62, 2, 3)
		reference.StoreRoots(t, s, height-1)

		options := func() []utxo.CreateOption {
			return []utxo.CreateOption{
				utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 9, BlockHeight: height, OnLongestChain: true}),
				utxo.WithIgnoreLocked(true),
			}
		}

		for _, tx := range reference.Txs {
			_, _, err := s.SpendAndCreate(ctx, tx, height, options()...)
			require.NoError(t, err)
		}

		w := tests.BuildMultiWorkload(t, 0x63, 2, 3)
		w.StoreRoots(t, s, height-1)

		chunks, below = 0, 0
		hook()

		results, err := s.SpendAndCreateMulti(ctx, w.Txs, height, options()...)
		require.NoError(t, err)
		requireAllCreated(t, results)

		require.Positive(t, chunks, "family chunks ran")
		require.Zero(t, below, "the below-checkpoint writer did not")
		require.Equal(t, reference.Records(t, s), w.Records(t, s), "a block above the checkpoint is carried like a per-transaction write carries it")
		require.Equal(t, reference.RootSpends(t, s), w.RootSpends(t, s))
	})

	t.Run("a block below the checkpoint with ordinary spends takes family chunks", func(t *testing.T) {
		s, ctx := newTestStore(t)

		w := tests.BuildMultiWorkload(t, 0x65, 2, 3)
		w.StoreRoots(t, s, 799)

		chunks, below = 0, 0
		hook()

		// The block is below the checkpoint but the spends are not outpoint-only, the shape
		// PR 1919's gate also refuses, so the list is not quick validation's.
		results, err := s.SpendAndCreateMulti(ctx, w.Txs, 800,
			utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 800}), utxo.WithIgnoreLocked(true))
		require.NoError(t, err)
		requireAllCreated(t, results)

		require.Positive(t, chunks)
		require.Zero(t, below)
	})

	t.Run("a list with no block takes family chunks", func(t *testing.T) {
		s, ctx := newTestStore(t)

		w := tests.BuildMultiWorkload(t, 0x64, 2, 3)
		w.StoreRoots(t, s, 799)

		chunks, below = 0, 0
		hook()

		results, err := s.SpendAndCreateMulti(ctx, w.Txs, 800, utxo.WithIgnoreLocked(true))
		require.NoError(t, err)
		requireAllCreated(t, results)

		require.Positive(t, chunks)
		require.Zero(t, below)
	})
}

// A transaction the store holds is Existed, whatever the state of the evidence that would let a
// repeat of its spends pass. Here the journal row that names it as the spender of its parent
// coin is gone, so a spend of that coin is refused as spent by nobody the store can name. The
// chunk must find the record rather than report the transaction failed, and must not touch
// anything.
func TestSpendAndCreateMultiAnExistingTransactionWithNoJournalRowIsExisted(t *testing.T) {
	s, ctx := newUncheckpointedStore(t)

	parent := storedParent(t, s, ctx)
	child := spendOutput(t, parent, 0, 1)

	_, _, err := s.SpendAndCreate(ctx, child, 1_100, utxo.WithIgnoreLocked(true))
	require.NoError(t, err)

	h := parent.TxIDChainHash()
	_, err = s.pool.Exec(ctx, `DELETE FROM spend_journal WHERE txid = $1`, h[:])
	require.NoError(t, err)

	results, err := s.SpendAndCreateMulti(ctx, []*bt.Tx{child}, 1_100, utxo.WithIgnoreLocked(true))
	require.NoError(t, err)
	require.Equal(t, utxo.MultiTxExisted, results[0].Status, "%v", results[0].Err)

	require.Equal(t, 1, identityCount(t, s, ctx, child))
	require.Equal(t, 1, utxoCount(t, s, ctx, parent), "the coin the child spent stays spent")
}

// A transaction stored without its spends, as a coinbase or a seed is, is Existed and its inputs
// are left alone: the write took nothing for it. Spending first and finding the record through
// the create claim must roll that spend back.
func TestSpendAndCreateMultiAnExistingTransactionKeepsItsInputsUntouched(t *testing.T) {
	s, ctx := newUncheckpointedStore(t)

	parent := storedParent(t, s, ctx)
	child := spendOutput(t, parent, 0, 1)

	_, _, err := s.SpendAndCreate(ctx, child, 1_100, utxo.WithCreateOnly())
	require.NoError(t, err)

	results, err := s.SpendAndCreateMulti(ctx, []*bt.Tx{child}, 1_100, utxo.WithIgnoreLocked(true))
	require.NoError(t, err)
	require.Equal(t, utxo.MultiTxExisted, results[0].Status, "%v", results[0].Err)

	require.Equal(t, 2, utxoCount(t, s, ctx, parent), "no spend was kept for a transaction that already existed")
	require.Equal(t, 0, journalRowsFor(t, s, ctx, parent, 0))
}

// A record is built from the transaction as the spends leave it, with every input carrying the
// value and script of the coin it took, as the per-transaction write builds it. A list that
// arrives with no previous-output data on its inputs must therefore leave the record, fee
// included, that a loop of SpendAndCreate leaves. Building the record before the spends would
// leave no fee, because a fee needs extended inputs.
func TestSpendAndCreateMultiBuildsTheRecordFromTheDecoratedInputs(t *testing.T) {
	const height = 2200

	s, ctx := newUncheckpointedStore(t)

	reference := tests.BuildMultiWorkload(t, 0x71, 3, 4)
	reference.StoreRoots(t, s, height-1)

	for _, tx := range outpointOnly(reference.Txs) {
		_, _, err := s.SpendAndCreate(ctx, tx, height, utxo.WithIgnoreLocked(true))
		require.NoError(t, err)
	}

	w := tests.BuildMultiWorkload(t, 0x72, 3, 4)
	w.StoreRoots(t, s, height-1)

	results, err := s.SpendAndCreateMulti(ctx, outpointOnly(w.Txs), height, utxo.WithIgnoreLocked(true))
	require.NoError(t, err)
	requireAllCreated(t, results)

	want := reference.Records(t, s)
	for i, r := range want {
		require.NotZero(t, r.Fee, "tx %d: the reference has a fee, so the comparison means something", i)
	}

	require.Equal(t, want, w.Records(t, s))
	require.Equal(t, reference.RootSpends(t, s), w.RootSpends(t, s))
}
