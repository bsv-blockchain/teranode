package utxoset

import (
	"context"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/stretchr/testify/require"
)

func TestParentOutputsForValidationUtxoset(t *testing.T) {
	t.Run("contract", func(t *testing.T) {
		s, _ := newUncheckpointedStore(t)
		tests.ParentOutputsForValidation(t, s)
	})

	t.Run("reads outputs, never inputs", func(t *testing.T) {
		s, _ := newUncheckpointedStore(t)
		tests.ParentOutputsReadsOutputsNotInputs(t, s)
	})
}

func TestSpendAndCreateMultiUtxoset(t *testing.T) {
	suite := func(t *testing.T) {
		t.Run("matches a loop of SpendAndCreate", func(t *testing.T) {
			s, _ := newUncheckpointedStore(t)
			tests.SpendAndCreateMultiMatchesLoop(t, s)
		})
		t.Run("a spend fails partway", func(t *testing.T) {
			s, _ := newUncheckpointedStore(t)
			tests.SpendAndCreateMultiSpendFailsPartway(t, s)
		})
		t.Run("a parent in the list exists", func(t *testing.T) {
			s, _ := newUncheckpointedStore(t)
			tests.SpendAndCreateMultiParentExists(t, s)
		})
		t.Run("a refusal writes nothing", func(t *testing.T) {
			s, _ := newUncheckpointedStore(t)
			tests.SpendAndCreateMultiRefusalWritesNothing(t, s)
		})
		t.Run("repeat at every cut point", func(t *testing.T) {
			s, _ := newUncheckpointedStore(t)
			tests.SpendAndCreateMultiRepeatAtCutPoints(t, s)
		})
		t.Run("subtree indexes", func(t *testing.T) {
			s, _ := newUncheckpointedStore(t)
			tests.SpendAndCreateMultiSubtreeIdxs(t, s)
		})
		t.Run("subtree indexes below the checkpoint", func(t *testing.T) {
			s, _ := newTestStore(t)
			tests.SpendAndCreateMultiSubtreeIdxs(t, s)
		})
	}

	t.Run("one spend chunk", suite)

	// One transaction per create chunk lets one chunk of a level commit while another finds its
	// parent already exists, so the rest of the list finishes per transaction and has to spend
	// outputs this write netted, by finding their journal rows.
	t.Run("a create chunk per transaction", func(t *testing.T) {
		old := multiCreateChunkTxs
		multiCreateChunkTxs = 1

		t.Cleanup(func() { multiCreateChunkTxs = old })

		suite(t)
	})

	// One input per spend chunk puts every transaction in its own chunk, run in parallel, so a
	// failed transaction's descendants have their spends committed before the failure is known
	// and must be restored.
	t.Run("a spend chunk per input", func(t *testing.T) {
		old := multiSpendChunkInputs
		multiSpendChunkInputs = 1

		t.Cleanup(func() { multiSpendChunkInputs = old })

		suite(t)
	})
}

// utxoRowsOf counts the UTXO rows the store holds for tx's outputs.
func utxoRowsOf(t *testing.T, s *Store, tx *bt.Tx) int {
	t.Helper()

	h := tx.TxIDChainHash()

	var n int
	require.NoError(t, s.pool.QueryRow(context.Background(),
		`SELECT count(*) FROM utxo WHERE leaf = $1 AND ukey >= $2 AND ukey <= $3 AND txid = $4`,
		LeafFor(h[:]), Pack(h[:], 0), Pack(h[:], ^uint32(0)), h[:]).Scan(&n))

	return n
}

// An output created and spent inside one list is never a UTXO row, and its spend is in the
// journal naming the child; an output nothing in the list spends is a UTXO row.
func TestSpendAndCreateMultiNetsOutputsSpentInTheList(t *testing.T) {
	s, ctx := newUncheckpointedStore(t)

	const height = 700

	w := tests.BuildMultiWorkload(t, 0x70, 3, 4)
	w.StoreRoots(t, s, height-1)

	results, err := s.SpendAndCreateMulti(ctx, w.Txs, height, utxo.WithIgnoreLocked(true))
	require.NoError(t, err)

	for i, r := range results {
		require.Equal(t, utxo.MultiTxCreated, r.Status, "tx %d: %v", i, r.Err)
	}

	spentBy := map[chainhash.Hash]map[uint32]chainhash.Hash{}

	for _, tx := range w.Txs {
		for _, in := range tx.Inputs {
			p := *in.PreviousTxIDChainHash()
			if spentBy[p] == nil {
				spentBy[p] = map[uint32]chainhash.Hash{}
			}

			spentBy[p][in.PreviousTxOutIndex] = *tx.TxIDChainHash()
		}
	}

	for i, tx := range w.Txs {
		h := *tx.TxIDChainHash()
		require.Equal(t, len(tx.Outputs)-len(spentBy[h]), utxoRowsOf(t, s, tx), "tx %d: only outputs nothing in the list spends are UTXO rows", i)

		for vout, child := range spentBy[h] {
			var spender []byte
			require.NoError(t, s.pool.QueryRow(ctx, `SELECT spending_txid FROM spend_journal WHERE ukey = $1 AND txid = $2`,
				Pack(h[:], vout), h[:]).Scan(&spender), "tx %d output %d", i, vout)
			require.Equal(t, child[:], spender)
		}
	}

	// A transaction outside the list that spends a netted output is refused as spent, naming
	// the child that spent it.
	parent := w.Txs[0]
	thief := bt.NewTx()
	require.NoError(t, thief.FromUTXOs(&bt.UTXO{TxIDHash: parent.TxIDChainHash(), Vout: 0,
		LockingScript: parent.Outputs[0].LockingScript, Satoshis: parent.Outputs[0].Satoshis}))
	thief.Inputs[0].UnlockingScript = tests.Tx.Inputs[0].UnlockingScript
	require.NoError(t, thief.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", parent.Outputs[0].Satoshis-200))

	_, spends, err := s.SpendAndCreate(ctx, thief, height+1)
	require.ErrorIs(t, err, errors.ErrSpent)
	require.Len(t, spends, 1)
	require.NotNil(t, spends[0].ConflictingTxID)
	require.Equal(t, spentBy[*parent.TxIDChainHash()][0], *spends[0].ConflictingTxID)
}

// A crash after step 1, or after any group of step 2, repeats the way the caller repeats: the
// pre-check drops every transaction that exists and the rest are sent again, as one list or
// two. The records must match a clean run. The chunk bound is one level wide, so no two levels
// share a group and every level is its own commit to crash after.
func TestSpendAndCreateMultiNettedRepeatAfterACrash(t *testing.T) {
	const (
		height = 800
		levels = 4
		width  = 5
	)

	old := multiCreateChunkTxs
	multiCreateChunkTxs = width

	t.Cleanup(func() { multiCreateChunkTxs = old })

	stages := []string{"spent", "created group 0", "created group 1", "created group 2"}

	for si, stage := range stages {
		for _, split := range []bool{false, true} {
			name := stage
			if split {
				name += ", repeated as two lists"
			}

			t.Run(name, func(t *testing.T) {
				s, ctx := newUncheckpointedStore(t)

				seed := byte(0x80 + si*2)
				if split {
					seed++
				}

				reference := tests.BuildMultiWorkload(t, 0x7f, levels, width)
				reference.StoreRoots(t, s, height-1)

				for _, tx := range reference.Txs {
					_, _, err := s.SpendAndCreate(ctx, tx, height, utxo.WithIgnoreLocked(true))
					require.NoError(t, err)
				}

				want := reference.Records(t, s)
				wantRoots := reference.RootSpends(t, s)

				w := tests.BuildMultiWorkload(t, seed, levels, width)
				w.StoreRoots(t, s, height-1)

				crash := errors.NewProcessingError("injected crash")
				multiFault = func(at string) error {
					if at == stage {
						return crash
					}

					return nil
				}

				_, err := s.SpendAndCreateMulti(ctx, w.Txs, height, utxo.WithIgnoreLocked(true))
				multiFault = nil
				require.ErrorIs(t, err, crash)

				var remaining []*bt.Tx

				for _, tx := range w.Txs {
					if _, err := s.Get(ctx, tx.TxIDChainHash(), fields.Fee); errors.Is(err, errors.ErrTxNotFound) {
						remaining = append(remaining, tx)
					} else {
						require.NoError(t, err)
					}
				}

				lists := [][]*bt.Tx{remaining}
				if split && len(remaining) > 1 {
					lists = [][]*bt.Tx{remaining[:len(remaining)/2], remaining[len(remaining)/2:]}
				}

				for _, list := range lists {
					results, err := s.SpendAndCreateMulti(ctx, list, height, utxo.WithIgnoreLocked(true))
					require.NoError(t, err)

					for i, r := range results {
						require.Equal(t, utxo.MultiTxCreated, r.Status, "tx %d of the repeat: %v", i, r.Err)
					}
				}

				require.Equal(t, want, w.Records(t, s))
				require.Equal(t, wantRoots, w.RootSpends(t, s))
			})
		}
	}
}

// A parent whose body the store no longer keeps is answered from the UTXO table while the
// output is unspent, and from the spend journal once it is spent, including an output the list
// netted.
func TestParentOutputsForValidationWithoutABody(t *testing.T) {
	s, ctx := newUncheckpointedStore(t)

	const height = 900

	w := tests.BuildMultiWorkload(t, 0x90, 2, 2)
	w.StoreRoots(t, s, height-1)

	results, err := s.SpendAndCreateMulti(ctx, w.Txs, height, utxo.WithIgnoreLocked(true))
	require.NoError(t, err)

	for i, r := range results {
		require.Equal(t, utxo.MultiTxCreated, r.Status, "tx %d: %v", i, r.Err)
	}

	parent := w.Txs[0] // level 0 tx 0: output 0 netted by level 1 tx 0, output 1 unspent
	h := parent.TxIDChainHash()

	_, err = s.pool.Exec(ctx, `DELETE FROM tx_body WHERE txid = $1`, h[:])
	require.NoError(t, err)

	answers, err := s.ParentOutputsForValidation(ctx, []utxo.Outpoint{{TxID: *h, Vout: 0}, {TxID: *h, Vout: 1}, {TxID: *h, Vout: 7}})
	require.NoError(t, err)

	for vout := 0; vout < 2; vout++ {
		require.NoError(t, answers[vout].Err)
		require.Equal(t, utxo.ParentOutputNotMined, answers[vout].Status, "output %d", vout)
		require.Equal(t, parent.Outputs[vout].Satoshis, answers[vout].Satoshis, "output %d", vout)
		require.Equal(t, []byte(*parent.Outputs[vout].LockingScript), []byte(*answers[vout].LockingScript), "output %d", vout)
	}

	// A known parent with no coin at the index, in neither table, is a verdict, as on the other
	// stores.
	require.NoError(t, answers[2].Err)
	require.Equal(t, utxo.ParentOutputNoSuchIndex, answers[2].Status)
}

// payFrom builds a transaction spending the given outputs, extended, paying their sum less 200.
func payFrom(t *testing.T, lockTime uint32, parents []*bt.Tx, vouts []uint32) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()
	tx.LockTime = lockTime

	var in uint64

	for k, p := range parents {
		out := p.Outputs[vouts[k]]
		require.NoError(t, tx.FromUTXOs(&bt.UTXO{TxIDHash: p.TxIDChainHash(), Vout: vouts[k], LockingScript: out.LockingScript, Satoshis: out.Satoshis}))
		tx.Inputs[len(tx.Inputs)-1].UnlockingScript = tests.Tx.Inputs[0].UnlockingScript
		in += out.Satoshis
	}

	require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", in-200))

	return tx
}

// A transaction whose parent in the list fails has its own outside spends undone, even when a
// parallel chunk committed them before the parent's failure was known.
func TestSpendAndCreateMultiRestoresTheOutsideSpendsOfAFailedParentsChild(t *testing.T) {
	old := multiSpendChunkInputs
	multiSpendChunkInputs = 1

	t.Cleanup(func() { multiSpendChunkInputs = old })

	s, ctx := newUncheckpointedStore(t)

	const height = 1000

	w := tests.BuildMultiWorkload(t, 0xa0, 1, 2) // two roots to spend from
	w.StoreRoots(t, s, height-1)
	rootA, rootB := w.Roots[0], w.Roots[1]

	thief := payFrom(t, 1, []*bt.Tx{rootA}, []uint32{0})
	_, _, err := s.SpendAndCreate(ctx, thief, height)
	require.NoError(t, err)

	parent := payFrom(t, 2, []*bt.Tx{rootA}, []uint32{0})           // fails: rootA:0 is taken
	child := payFrom(t, 3, []*bt.Tx{parent, rootB}, []uint32{0, 0}) // rootB:0 is an outside spend

	results, err := s.SpendAndCreateMulti(ctx, []*bt.Tx{parent, child}, height, utxo.WithIgnoreLocked(true))
	require.NoError(t, err)
	require.Equal(t, utxo.MultiTxFailed, results[0].Status)
	require.Equal(t, utxo.MultiTxParentFailed, results[1].Status)

	// rootB:0 is spendable again: another transaction can take it.
	other := payFrom(t, 4, []*bt.Tx{rootB}, []uint32{0})
	_, _, err = s.SpendAndCreate(ctx, other, height)
	require.NoError(t, err, "the child's spend of rootB:0 was left in place")
}

// buildChain builds one root and a single chain of n transactions after it, each spending
// output 0 of the one before, so every transaction is its own dependency level. Transaction i
// carries extra[i] additional 1,000-satoshi outputs, which is how a test makes one of them big.
func buildChain(t *testing.T, seed byte, n int, extra map[int]int) *tests.MultiWorkload {
	t.Helper()

	var prev chainhash.Hash
	prev[0], prev[1], prev[3] = seed, 0xc4, 0x5c

	root := bt.NewTx()
	require.NoError(t, root.FromUTXOs(&bt.UTXO{TxIDHash: &prev, Vout: 0,
		LockingScript: tests.Tx.Inputs[0].PreviousTxScript, Satoshis: 100_000_000}))
	root.Inputs[0].UnlockingScript = tests.Tx.Inputs[0].UnlockingScript
	require.NoError(t, root.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 50_000_000))

	w := &tests.MultiWorkload{Roots: []*bt.Tx{root}}
	parent := root

	for i := 0; i < n; i++ {
		tx := bt.NewTx()
		tx.LockTime = uint32(seed)<<24 | uint32(i) //nolint:gosec // test data

		out := parent.Outputs[0]
		require.NoError(t, tx.FromUTXOs(&bt.UTXO{TxIDHash: parent.TxIDChainHash(), Vout: 0,
			LockingScript: out.LockingScript, Satoshis: out.Satoshis}))
		tx.Inputs[0].UnlockingScript = tests.Tx.Inputs[0].UnlockingScript

		change := out.Satoshis - 200 - uint64(extra[i])*1_000 //nolint:gosec // test data
		require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", change))

		for e := 0; e < extra[i]; e++ {
			require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1_000))
		}

		w.Txs = append(w.Txs, tx)
		parent = tx
	}

	return w
}

// countCreateCommits records the size of every step-2 chunk transaction that commits, in order.
func countCreateCommits(t *testing.T) func() []int {
	t.Helper()

	var (
		mu    sync.Mutex
		sizes []int
	)

	multiCreateCommitted = func(n int) {
		mu.Lock()
		sizes = append(sizes, n)
		mu.Unlock()
	}

	t.Cleanup(func() { multiCreateCommitted = nil })

	return func() []int {
		mu.Lock()
		defer mu.Unlock()

		return append([]int(nil), sizes...)
	}
}

// perTxReference writes a workload one SpendAndCreate at a time and returns its records.
func perTxReference(t *testing.T, s *Store, w *tests.MultiWorkload, height uint32) ([]any, []string) {
	t.Helper()

	w.StoreRoots(t, s, height-1)

	for _, tx := range w.Txs {
		_, _, err := s.SpendAndCreate(context.Background(), tx, height, utxo.WithIgnoreLocked(true))
		require.NoError(t, err)
	}

	return recordsOf(t, s, w), w.RootSpends(t, s)
}

func recordsOf(t *testing.T, s *Store, w *tests.MultiWorkload) []any {
	t.Helper()

	recs := w.Records(t, s)
	out := make([]any, len(recs))

	for i, r := range recs {
		out[i] = r
	}

	return out
}

// A deep chain is the shape that made step 2 slow: one level per transaction, each its own
// database transaction. Consecutive narrow levels share a chunk transaction up to the chunk
// bounds, so a 600-level chain commits in ceil(600/256) step-2 transactions, and the records
// are the ones a per-transaction write leaves.
func TestSpendAndCreateMultiMergesNarrowLevels(t *testing.T) {
	const (
		height = 1100
		n      = 600
	)

	s, ctx := newUncheckpointedStore(t)

	wantRecs, wantRoots := perTxReference(t, s, buildChain(t, 0xb0, n, nil), height)

	w := buildChain(t, 0xb1, n, nil)
	w.StoreRoots(t, s, height-1)

	commits := countCreateCommits(t)

	results, err := s.SpendAndCreateMulti(ctx, w.Txs, height, utxo.WithIgnoreLocked(true))
	require.NoError(t, err)

	for i, r := range results {
		require.Equal(t, utxo.MultiTxCreated, r.Status, "tx %d: %v", i, r.Err)
	}

	require.Len(t, commits(), (n+multiCreateChunkTxs-1)/multiCreateChunkTxs)
	require.Equal(t, []int{256, 256, 88}, commits())
	require.Equal(t, wantRecs, recordsOf(t, s, w))
	require.Equal(t, wantRoots, w.RootSpends(t, s))
}

// A crash between two merged groups at the default bound repeats through the caller's
// pre-check to the records of a clean run: the first group committed every parent's journal
// rows with the children that spend them, so nothing it left needs the second.
func TestSpendAndCreateMultiRepeatAfterACrashBetweenMergedGroups(t *testing.T) {
	const (
		height = 1200
		n      = 600
	)

	for _, stage := range []string{"spent", "created group 0", "created group 1"} {
		t.Run(stage, func(t *testing.T) {
			s, ctx := newUncheckpointedStore(t)

			wantRecs, wantRoots := perTxReference(t, s, buildChain(t, 0xc0, n, nil), height)

			w := buildChain(t, 0xc1, n, nil)
			w.StoreRoots(t, s, height-1)

			crash := errors.NewProcessingError("injected crash")
			multiFault = func(at string) error {
				if at == stage {
					return crash
				}

				return nil
			}

			_, err := s.SpendAndCreateMulti(ctx, w.Txs, height, utxo.WithIgnoreLocked(true))
			multiFault = nil
			require.ErrorIs(t, err, crash)

			remaining := missingTxs(t, s, w.Txs)
			require.NotEmpty(t, remaining)
			require.Less(t, len(remaining), n+1)

			results, err := s.SpendAndCreateMulti(ctx, remaining, height, utxo.WithIgnoreLocked(true))
			require.NoError(t, err)

			for i, r := range results {
				require.Equal(t, utxo.MultiTxCreated, r.Status, "tx %d of the repeat: %v", i, r.Err)
			}

			require.Equal(t, wantRecs, recordsOf(t, s, w))
			require.Equal(t, wantRoots, w.RootSpends(t, s))
		})
	}
}

// missingTxs is the caller's pre-check: the transactions of txs the store does not hold.
func missingTxs(t *testing.T, s *Store, txs []*bt.Tx) []*bt.Tx {
	t.Helper()

	var remaining []*bt.Tx

	for _, tx := range txs {
		if _, err := s.Get(context.Background(), tx.TxIDChainHash(), fields.Fee); errors.Is(err, errors.ErrTxNotFound) {
			remaining = append(remaining, tx)
		} else {
			require.NoError(t, err)
		}
	}

	return remaining
}

// A chain whose middle transaction already exists, with an output its child in the list spends,
// stops the merged group holding it. The whole group rolls back, so nothing of it is half
// written, and the rest of the list goes through the per-transaction default. The records are
// the ones the per-transaction default leaves from the same start.
func TestSpendAndCreateMultiMergedGroupWithAnExistingParent(t *testing.T) {
	const (
		height = 1300
		n      = 10
		middle = 5
	)

	setup := func(t *testing.T, s *Store, seed byte) *tests.MultiWorkload {
		w := buildChain(t, seed, n, nil)
		w.StoreRoots(t, s, height-1)

		_, _, err := s.SpendAndCreate(context.Background(), w.Txs[middle], height-1, utxo.WithCreateOnly())
		require.NoError(t, err)

		return w
	}

	s, ctx := newUncheckpointedStore(t)

	ref := setup(t, s, 0xd0)
	refResults, err := utxo.DefaultSpendAndCreateMulti(ctx, s, 1, ref.Txs, height, utxo.WithIgnoreLocked(true))
	require.NoError(t, err)

	w := setup(t, s, 0xd1)
	commits := countCreateCommits(t)

	results, err := s.SpendAndCreateMulti(ctx, w.Txs, height, utxo.WithIgnoreLocked(true))
	require.NoError(t, err)
	require.Empty(t, commits(), "the group holding the existing parent committed something")

	for i := range results {
		require.Equal(t, refResults[i].Status, results[i].Status, "tx %d: %v", i, results[i].Err)
	}

	require.Equal(t, utxo.MultiTxExisted, results[middle].Status)
	require.Equal(t, recordsOf(t, s, ref), recordsOf(t, s, w))
	require.Equal(t, ref.RootSpends(t, s), w.RootSpends(t, s))
}

// A transaction too big for what is left of the byte budget closes the group before it and
// starts its own, and the next one that does not fit beside it closes that one in turn.
func TestSpendAndCreateMultiByteBudgetClosesAGroup(t *testing.T) {
	const (
		height = 1400
		n      = 5
		big    = 2
	)

	extra := map[int]int{big: 200}

	s, ctx := newUncheckpointedStore(t)

	wantRecs, wantRoots := perTxReference(t, s, buildChain(t, 0xe0, n, extra), height)

	w := buildChain(t, 0xe1, n, extra)
	w.StoreRoots(t, s, height-1)

	small, large := w.Txs[0].Size(), w.Txs[big].Size()
	require.Greater(t, large, 4*small)

	old := spendAndCreateBatchByteBudget
	spendAndCreateBatchByteBudget = large + small/2

	t.Cleanup(func() { spendAndCreateBatchByteBudget = old })

	commits := countCreateCommits(t)

	results, err := s.SpendAndCreateMulti(ctx, w.Txs, height, utxo.WithIgnoreLocked(true))
	require.NoError(t, err)

	for i, r := range results {
		require.Equal(t, utxo.MultiTxCreated, r.Status, "tx %d: %v", i, r.Err)
	}

	require.Equal(t, []int{2, 1, 2}, commits())
	require.Equal(t, wantRecs, recordsOf(t, s, w))
	require.Equal(t, wantRoots, w.RootSpends(t, s))
}

// buildNarrowThenWide builds the shape a mainnet block has: a chain of narrow levels, then one
// level of wide transactions, then one narrow level after it. The last chain transaction carries
// wide extra outputs; wide child j spends its output j+1, and a tail transaction spends output 0
// of the first wide child.
func buildNarrowThenWide(t *testing.T, seed byte, narrow, wide int) *tests.MultiWorkload {
	t.Helper()

	w := buildChain(t, seed, narrow, map[int]int{narrow - 1: wide})
	last := w.Txs[narrow-1]

	children := make([]*bt.Tx, wide)

	for j := 0; j < wide; j++ {
		children[j] = payFrom(t, uint32(seed)<<24|0x10000|uint32(j), []*bt.Tx{last}, []uint32{uint32(j + 1)}) //nolint:gosec // test data
	}

	w.Txs = append(w.Txs, children...)
	w.Txs = append(w.Txs, payFrom(t, uint32(seed)<<24|0x20000, []*bt.Tx{children[0]}, []uint32{0})) //nolint:gosec // test data

	return w
}

// Narrow levels gathered into a pending group are flushed before a level wider than the chunk
// bound is split, so the parents commit before the children that spend them. With a chunk bound
// of 2, three narrow levels, a level of five and one narrow level after it, the commits are the
// group of the first two levels, the group of the third, the three chunks of the wide level in
// any order, and the tail. A crash after any of those commit units repeats through the caller's
// pre-check to the records of a clean run.
func TestSpendAndCreateMultiFlushesPendingBeforeASplitLevel(t *testing.T) {
	const (
		height = 1500
		narrow = 3
		wide   = 5
	)

	old := multiCreateChunkTxs
	multiCreateChunkTxs = 2

	t.Cleanup(func() { multiCreateChunkTxs = old })

	t.Run("commit order", func(t *testing.T) {
		s, ctx := newUncheckpointedStore(t)

		wantRecs, wantRoots := perTxReference(t, s, buildNarrowThenWide(t, 0xf0, narrow, wide), height)

		w := buildNarrowThenWide(t, 0xf1, narrow, wide)
		w.StoreRoots(t, s, height-1)

		commits := countCreateCommits(t)

		results, err := s.SpendAndCreateMulti(ctx, w.Txs, height, utxo.WithIgnoreLocked(true))
		require.NoError(t, err)

		for i, r := range results {
			require.Equal(t, utxo.MultiTxCreated, r.Status, "tx %d: %v", i, r.Err)
		}

		got := commits()
		require.Len(t, got, 6)
		require.Equal(t, []int{2, 1}, got[:2], "the pending narrow levels commit before the split level")
		require.ElementsMatch(t, []int{2, 2, 1}, got[2:5])
		require.Equal(t, 1, got[5])
		require.Equal(t, wantRecs, recordsOf(t, s, w))
		require.Equal(t, wantRoots, w.RootSpends(t, s))
	})

	for gi, stage := range []string{"spent", "created group 0", "created group 1", "created group 2", "created group 3"} {
		t.Run("crash after "+stage, func(t *testing.T) {
			s, ctx := newUncheckpointedStore(t)

			wantRecs, wantRoots := perTxReference(t, s, buildNarrowThenWide(t, byte(0xf2+2*gi), narrow, wide), height) //nolint:gosec // test data

			w := buildNarrowThenWide(t, byte(0xf3+2*gi), narrow, wide) //nolint:gosec // test data
			w.StoreRoots(t, s, height-1)

			crash := errors.NewProcessingError("injected crash")
			multiFault = func(at string) error {
				if at == stage {
					return crash
				}

				return nil
			}

			_, err := s.SpendAndCreateMulti(ctx, w.Txs, height, utxo.WithIgnoreLocked(true))
			multiFault = nil
			require.ErrorIs(t, err, crash)

			remaining := missingTxs(t, s, w.Txs)

			if len(remaining) > 0 {
				results, err := s.SpendAndCreateMulti(ctx, remaining, height, utxo.WithIgnoreLocked(true))
				require.NoError(t, err)

				for i, r := range results {
					require.Equal(t, utxo.MultiTxCreated, r.Status, "tx %d of the repeat: %v", i, r.Err)
				}
			}

			require.Equal(t, wantRecs, recordsOf(t, s, w))
			require.Equal(t, wantRoots, w.RootSpends(t, s))
		})
	}
}

// belowCheckpointOptions are the options quick validation applies a block with below the
// checkpoint: the block carried from birth, and outpoint-only spends.
func belowCheckpointOptions(height uint32) []utxo.CreateOption {
	return belowCheckpointOptionsAt(height, 0)
}

// belowCheckpointOptionsAt is belowCheckpointOptions for a transaction in subtree idx.
func belowCheckpointOptionsAt(height uint32, idx int) []utxo.CreateOption {
	return []utxo.CreateOption{
		utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 7, BlockHeight: height, SubtreeIdx: idx}),
		utxo.WithIgnoreLocked(true),
		utxo.WithSkipExtendedInputs(true),
		utxo.WithSkipUTXOHashCheck(true),
	}
}

// outpointOnly copies txs with every input stripped to its outpoint, as a block below the
// checkpoint arrives: no previous script and no previous satoshis.
func outpointOnly(txs []*bt.Tx) []*bt.Tx {
	out := make([]*bt.Tx, len(txs))

	for i, tx := range txs {
		c := tx.Clone()
		for _, in := range c.Inputs {
			in.PreviousTxScript = nil
			in.PreviousTxSatoshis = 0
		}

		out[i] = c
	}

	return out
}

// When a parent in the list already exists, as on a repeat after a crash, the netted write hands
// the rest of the list to the per-transaction default. That hand-off must carry each remaining
// transaction's own subtree index, not refuse the shorter list or shift the indexes.
func TestSpendAndCreateMultiParentExistsKeepsEachSubtreeIdx(t *testing.T) {
	old := multiCreateChunkTxs
	multiCreateChunkTxs = 1

	t.Cleanup(func() { multiCreateChunkTxs = old })

	s, ctx := newTestStore(t)

	const height = 950

	w := tests.BuildMultiWorkload(t, 0x52, 2, 3)
	w.StoreRoots(t, s, height-1)

	list := outpointOnly(w.Txs)

	idxs := make([]int, len(list))
	for i := range idxs {
		idxs[i] = 10 + i
	}

	// A crash left level 0 transaction 1 written for this block.
	_, _, err := s.SpendAndCreate(ctx, list[1], height, belowCheckpointOptionsAt(height, idxs[1])...)
	require.NoError(t, err)

	results, err := s.SpendAndCreateMulti(ctx, list, height, append(belowCheckpointOptions(height), utxo.WithSubtreeIdxs(idxs))...)
	require.NoError(t, err, "the hand-off must not be refused")

	for i, r := range results {
		if i == 1 {
			require.Equal(t, utxo.MultiTxExisted, r.Status)
			continue
		}

		require.Equal(t, utxo.MultiTxCreated, r.Status, "tx %d: %v", i, r.Err)
	}

	for i, tx := range list {
		md, err := s.Get(ctx, tx.TxIDChainHash(), fields.SubtreeIdxs)
		require.NoError(t, err, "tx %d", i)
		require.Equal(t, []int{idxs[i]}, md.SubtreeIdxs, "tx %d keeps its own subtree index", i)
	}
}
