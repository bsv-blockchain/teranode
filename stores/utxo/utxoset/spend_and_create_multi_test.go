package utxoset

import (
	"context"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

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
		// Both of these assert, through GetSpend, which INPUT of the spender took each coin,
		// one for spends of stored UTXOs and one for outputs netted inside the list.
		t.Run("result mapping", func(t *testing.T) {
			s, _ := newUncheckpointedStore(t)
			tests.SpendAndCreateMultiResultMapping(t, s)
		})
		t.Run("parents deduplicated", func(t *testing.T) {
			s, _ := newUncheckpointedStore(t)
			tests.SpendAndCreateMultiParentsDeduplicated(t, s)
		})
	}

	t.Run("whole families in one chunk", suite)

	// One transaction per chunk makes every in-list edge a cut edge: each parent's outputs are
	// written as coins and each child's chunk waits for its parent's commit and spends them for
	// real. The same contract must hold when nothing is netted.
	t.Run("a chunk per transaction", func(t *testing.T) {
		old := multiCreateChunkTxs
		multiCreateChunkTxs = 1

		t.Cleanup(func() { multiCreateChunkTxs = old })

		suite(t)
	})

	// Two transactions per chunk cuts families in the middle, so some edges are netted and some
	// are cut within one list.
	t.Run("two transactions per chunk", func(t *testing.T) {
		old := multiCreateChunkTxs
		multiCreateChunkTxs = 2

		t.Cleanup(func() { multiCreateChunkTxs = old })

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

// A crash inside a chunk, or between chunks, repeats the way the caller repeats: the pre-check
// drops every transaction that exists and the rest are sent again, as one list or two. The open
// chunk is rolled back whole and the committed ones stay, so the records must match a clean run.
// The chunk bound is one level wide, so the list is cut into chunks to crash in and between.
func TestSpendAndCreateMultiNettedRepeatAfterACrash(t *testing.T) {
	const (
		height = 800
		levels = 4
		width  = 5
	)

	old := multiCreateChunkTxs
	multiCreateChunkTxs = width

	t.Cleanup(func() { multiCreateChunkTxs = old })

	crashes := []struct {
		chunk int
		stage string
	}{
		{0, "locked"}, {0, "spent"}, {0, "created"}, {0, "committed"},
		{1, "spent"}, {1, "created"}, {1, "committed"}, {2, "committed"},
	}

	for si, c := range crashes {
		for _, split := range []bool{false, true} {
			name := "chunk " + strconv.Itoa(c.chunk) + " " + c.stage
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

				crash := errMultiCrash
				multiChunkFault = func(chunk int, at string) error {
					if chunk == c.chunk && at == c.stage {
						return crash
					}

					return nil
				}

				t.Cleanup(func() { multiChunkFault = nil })

				_, err := s.SpendAndCreateMulti(ctx, w.Txs, height, utxo.WithIgnoreLocked(true))
				multiChunkFault = nil
				require.ErrorIs(t, err, crash)

				// Chunks that did not depend on the crashed one may have committed too, so the
				// list left may be shorter, or empty.
				remaining := missingTxs(t, s, w.Txs)

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

// A transaction whose parent in the list fails leaves no spend of its own behind, whether the two
// share a chunk or sit in different ones. In one chunk the failed parent rolls the round back and
// the child is dropped before any spend is kept. In two chunks the child's chunk is never
// started, because its parent's chunk finished with the parent failed. Neither needs anything
// restored, because no spend was ever committed for the child.
func TestSpendAndCreateMultiAFailedParentsChildLeavesNoSpend(t *testing.T) {
	for _, tc := range []struct {
		name  string
		chunk int
	}{
		{"one chunk", 256},
		{"a chunk per transaction", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			old := multiCreateChunkTxs
			multiCreateChunkTxs = tc.chunk

			t.Cleanup(func() { multiCreateChunkTxs = old })

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

			// rootB:0 is spendable: another transaction can take it.
			other := payFrom(t, 4, []*bt.Tx{rootB}, []uint32{0})
			_, _, err = s.SpendAndCreate(ctx, other, height)
			require.NoError(t, err, "the child's spend of rootB:0 was left in place")
		})
	}
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

// countCreateCommits records the size of every chunk transaction that commits, in order.
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

// A deep chain is one family. It is cut into slices of up to the chunk bound, each one database
// transaction, so a 600-deep chain commits in ceil(600/256) transactions. The cut edges write
// their parent's output as a real coin and the next chunk spends it, and the records are still
// the ones a per-transaction write leaves.
func TestSpendAndCreateMultiDeepChainIsCutIntoChunks(t *testing.T) {
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

// A crash between two chunks of a deep chain at the default bound, or inside one, repeats through
// the caller's pre-check to the records of a clean run. A chunk that committed holds its
// transactions with their spends, and its last output is a real coin the next chunk spends, so
// nothing the first chunk left needs the second to make it true.
func TestSpendAndCreateMultiRepeatAfterACrashBetweenChunksOfAChain(t *testing.T) {
	const (
		height = 1200
		n      = 600
	)

	for _, c := range []struct {
		chunk int
		stage string
	}{{0, "spent"}, {0, "committed"}, {1, "created"}} {
		t.Run("chunk "+strconv.Itoa(c.chunk)+" "+c.stage, func(t *testing.T) {
			s, ctx := newUncheckpointedStore(t)

			wantRecs, wantRoots := perTxReference(t, s, buildChain(t, 0xc0, n, nil), height)

			w := buildChain(t, 0xc1, n, nil)
			w.StoreRoots(t, s, height-1)

			crash := errMultiCrash
			multiChunkFault = func(chunk int, at string) error {
				if chunk == c.chunk && at == c.stage {
					return crash
				}

				return nil
			}

			t.Cleanup(func() { multiChunkFault = nil })

			_, err := s.SpendAndCreateMulti(ctx, w.Txs, height, utxo.WithIgnoreLocked(true))
			multiChunkFault = nil
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

// A chain that begins with transactions the store already holds, with an output their child in
// the list spends, is written in one chunk. The chunk finds the stored ones under their locks
// before it nets anything, so they are Existed and not written, and the first new transaction
// spends the last stored one's coin for real. The records are the ones the per-transaction
// default leaves from the same start, with no hand-off to it.
func TestSpendAndCreateMultiAChunkWithAnExistingParent(t *testing.T) {
	const (
		height = 1300
		n      = 10
	)

	for _, stored := range []int{1, 6} {
		t.Run(strconv.Itoa(stored)+" stored", func(t *testing.T) {
			// The stored ones are written the way the validator writes them, spends and create
			// together, so each holds the spends of its own inputs.
			setup := func(t *testing.T, s *Store, seed byte) *tests.MultiWorkload {
				w := buildChain(t, seed, n, nil)
				w.StoreRoots(t, s, height-1)

				for _, tx := range w.Txs[:stored] {
					_, _, err := s.SpendAndCreate(context.Background(), tx, height-1)
					require.NoError(t, err)
				}

				return w
			}

			s, ctx := newUncheckpointedStore(t)

			ref := setup(t, s, 0xd0)
			refResults, err := utxo.DefaultSpendAndCreateMulti(ctx, s, utxo.UniformSpendAndCreateMultiWidths(1), ref.Txs, height, utxo.WithIgnoreLocked(true))
			require.NoError(t, err)

			w := setup(t, s, 0xd1)
			commits := countCreateCommits(t)

			results, err := s.SpendAndCreateMulti(ctx, w.Txs, height, utxo.WithIgnoreLocked(true))
			require.NoError(t, err)
			require.Equal(t, []int{n - stored}, commits(), "one chunk, holding every transaction that was not stored")

			for i := range results {
				require.Equal(t, refResults[i].Status, results[i].Status, "tx %d: %v", i, results[i].Err)
			}

			require.Equal(t, utxo.MultiTxExisted, results[stored-1].Status)
			require.Equal(t, utxo.MultiTxCreated, results[stored].Status)
			require.Equal(t, recordsOf(t, s, ref), recordsOf(t, s, w))
			require.Equal(t, ref.RootSpends(t, s), w.RootSpends(t, s))
		})
	}
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

// The shape of a mainnet block, a chain of narrow levels, then a wide level, then a tail, is one
// family. With a chunk bound of 2 it is cut into chunks, and a chunk commits only after every
// chunk holding the parent of one of its cut edges. A crash after any chunk, or inside one,
// repeats through the caller's pre-check to the records of a clean run.
func TestSpendAndCreateMultiACutFamilyCommitsInDependencyOrder(t *testing.T) {
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

		var (
			mu        sync.Mutex
			committed []int
		)

		multiChunkFault = func(chunk int, at string) error {
			if at == "committed" {
				mu.Lock()
				committed = append(committed, chunk)
				mu.Unlock()
			}

			return nil
		}

		t.Cleanup(func() { multiChunkFault = nil })

		results, err := s.SpendAndCreateMulti(ctx, w.Txs, height, utxo.WithIgnoreLocked(true))
		multiChunkFault = nil
		require.NoError(t, err)

		for i, r := range results {
			require.Equal(t, utxo.MultiTxCreated, r.Status, "tx %d: %v", i, r.Err)
		}

		sizes := make([]int, len(w.Txs))
		for i, tx := range w.Txs {
			sizes[i] = tx.Size()
		}

		parents := make([][]int, len(w.Txs))
		position := map[chainhash.Hash]int{}

		for i, tx := range w.Txs {
			position[*tx.TxIDChainHash()] = i

			for _, in := range tx.Inputs {
				if p, ok := position[*in.PreviousTxIDChainHash()]; ok && !slices.Contains(parents[i], p) {
					parents[i] = append(parents[i], p)
				}
			}
		}

		plan := planFamilyChunks(parents, sizes, multiCreateChunkTxs, spendAndCreateBatchByteBudget)
		require.Greater(t, plan.cutEdges, 0, "the shape must cut edges for this test to mean anything")
		require.Len(t, committed, len(plan.chunks))

		at := map[int]int{}
		for order, chunk := range committed {
			at[chunk] = order
		}

		for _, ch := range plan.chunks {
			for _, d := range ch.deps {
				require.Less(t, at[d], at[ch.idx], "chunk %d committed before chunk %d, which holds a parent of one of its transactions", ch.idx, d)
			}
		}

		require.Equal(t, wantRecs, recordsOf(t, s, w))
		require.Equal(t, wantRoots, w.RootSpends(t, s))
	})

	for gi, c := range []struct {
		chunk int
		stage string
	}{{0, "spent"}, {0, "committed"}, {1, "created"}, {1, "committed"}, {2, "locked"}, {3, "committed"}} {
		t.Run("crash at chunk "+strconv.Itoa(c.chunk)+" "+c.stage, func(t *testing.T) {
			s, ctx := newUncheckpointedStore(t)

			wantRecs, wantRoots := perTxReference(t, s, buildNarrowThenWide(t, byte(0xf2+2*gi), narrow, wide), height) //nolint:gosec // test data

			w := buildNarrowThenWide(t, byte(0xf3+2*gi), narrow, wide) //nolint:gosec // test data
			w.StoreRoots(t, s, height-1)

			crash := errMultiCrash
			multiChunkFault = func(chunk int, at string) error {
				if chunk == c.chunk && at == c.stage {
					return crash
				}

				return nil
			}

			t.Cleanup(func() { multiChunkFault = nil })

			_, err := s.SpendAndCreateMulti(ctx, w.Txs, height, utxo.WithIgnoreLocked(true))
			multiChunkFault = nil
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

// When a transaction of the list already exists, as on a repeat after a crash, the write skips it
// and writes the rest. Each transaction must keep its own subtree index: the skipped one must not
// refuse the list or shift the indexes of the others.
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
	require.NoError(t, err, "the existing transaction must not refuse the list")

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

// When a list is cut into parallel chunks and another chunk finds a transaction that already
// exists, a chunk caught at its commit must still commit. A context the other chunk's outcome
// cancelled would abandon it or, when the server had already committed it, leave it without a
// result, and a later look would find the record this call had just written and report it
// MultiTxExisted. This holds transaction 0's chunk at its commit until either its context is
// cancelled or a second passes, which makes such a race certain.
func TestSpendAndCreateMultiCommittingChunkIsNotCancelledByAnExistedSibling(t *testing.T) {
	old := multiCreateChunkTxs
	multiCreateChunkTxs = 1

	t.Cleanup(func() {
		multiCreateChunkTxs = old
		multiCreateBeforeCommit = nil
	})

	// A store with checkpoints sends a list below the checkpoint to the below-checkpoint writer
	// (netBelow), which has no chunk commit for the hook to hold, so this store has none.
	s, ctx := newUncheckpointedStore(t)

	const height = 950

	w := tests.BuildMultiWorkload(t, 0x53, 2, 3)
	w.StoreRoots(t, s, height-1)

	list := outpointOnly(w.Txs)

	idxs := make([]int, len(list))
	for i := range idxs {
		idxs[i] = 10 + i
	}

	// Transaction 1 already exists, and a child in the list spends it.
	_, _, err := s.SpendAndCreate(ctx, list[1], height, belowCheckpointOptionsAt(height, idxs[1])...)
	require.NoError(t, err)

	var (
		mu        sync.Mutex
		reached   bool
		cancelled error
	)

	tx0 := *list[0].TxIDChainHash()

	multiCreateBeforeCommit = func(commitCtx context.Context, txids []chainhash.Hash) {
		for _, h := range txids {
			if h != tx0 {
				continue
			}

			select {
			case <-commitCtx.Done():
			case <-time.After(time.Second):
			}

			mu.Lock()
			reached, cancelled = true, commitCtx.Err()
			mu.Unlock()
		}
	}

	results, err := s.SpendAndCreateMulti(ctx, list, height, append(belowCheckpointOptions(height), utxo.WithSubtreeIdxs(idxs))...)
	require.NoError(t, err)

	mu.Lock()
	require.True(t, reached, "transaction 0's chunk must reach its commit")
	require.NoError(t, cancelled, "a sibling chunk's failure must not cancel transaction 0's commit")
	mu.Unlock()

	require.Equal(t, utxo.MultiTxCreated, results[0].Status, "transaction 0 was created by this call: %v", results[0].Err)
	require.Equal(t, utxo.MultiTxExisted, results[1].Status)

	for i, r := range results[2:] {
		require.Equal(t, utxo.MultiTxCreated, r.Status, "tx %d: %v", i+2, r.Err)
	}
}
