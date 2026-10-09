package utxoset

import (
	"fmt"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/stretchr/testify/require"
)

// writeNothingList builds, on a stored root, a list in which two transactions write nothing at
// all below the checkpoint when bodies are skipped:
//
//	0: spends the root, one output, spent by 1          (an outside delete: keeps its record)
//	1: spends 0:0, two outputs, spent by 2 and 3         (every input and output netted: W)
//	2: spends 1:0, one data output                       (netted input, no spendable output: W)
//	3: spends 1:1, one output nobody in the list spends  (writes a coin: keeps its record)
func writeNothingList(t *testing.T, w *tests.MultiWorkload) []*bt.Tx {
	t.Helper()

	lock := w.Roots[0].Outputs[0].LockingScript

	spend := func(parent *bt.Tx, vout uint32) *bt.Tx {
		tx := bt.NewTx()
		require.NoError(t, tx.FromUTXOs(&bt.UTXO{
			TxIDHash:      parent.TxIDChainHash(),
			Vout:          vout,
			LockingScript: parent.Outputs[vout].LockingScript,
			Satoshis:      parent.Outputs[vout].Satoshis,
		}))

		return tx
	}

	t0 := spend(w.Roots[0], 0)
	t0.AddOutput(&bt.Output{Satoshis: w.Roots[0].Outputs[0].Satoshis - 1_000, LockingScript: lock})

	t1 := spend(t0, 0)
	half := (t0.Outputs[0].Satoshis - 1_000) / 2
	t1.AddOutput(&bt.Output{Satoshis: half, LockingScript: lock})
	t1.AddOutput(&bt.Output{Satoshis: half, LockingScript: lock})

	data, err := bscript.NewFromASM("OP_FALSE OP_RETURN 74657374")
	require.NoError(t, err)

	t2 := spend(t1, 0)
	t2.AddOutput(&bt.Output{Satoshis: 0, LockingScript: data})

	t3 := spend(t1, 1)
	t3.AddOutput(&bt.Output{Satoshis: half - 1_000, LockingScript: lock})

	return outpointOnly([]*bt.Tx{t0, t1, t2, t3})
}

// minedRecords reports, per transaction of the list, whether it has a mined record at height.
func minedRecords(t *testing.T, s *Store, list []*bt.Tx, height uint32) []bool {
	t.Helper()

	out := make([]bool, len(list))

	for i, tx := range list {
		var n int
		require.NoError(t, s.pool.QueryRow(t.Context(), `SELECT count(*) FROM tx_mined WHERE txid = $1 AND mined_height = $2`,
			tx.TxIDChainHash()[:], int32(height)).Scan(&n)) //nolint:gosec // test height

		out[i] = n == 1
	}

	return out
}

func newBodySkipStore(t *testing.T, skip bool) *Store {
	t.Helper()

	s, _ := newTestStoreWith(t, func(ts *settings.Settings) {
		ts.UtxoStore.SkipTxBodyBelowCheckpoint = skip
	})

	return s
}

// Below the checkpoint, with bodies skipped, a transaction whose inputs are all netted and whose
// outputs are all spent in the list or unspendable writes nothing, not even its mined record. It
// is still reported created, with no metadata, and the coins are the list's net effect.
func TestNettedBelowWriteNothingTransactionHasNoRecord(t *testing.T) {
	s := newBodySkipStore(t, true)
	ctx := t.Context()

	const height = 830

	w := tests.BuildMultiWorkload(t, 0x7a, 1, 1)
	w.StoreRoots(t, s, height-1)

	list := writeNothingList(t, w)

	results, err := s.SpendAndCreateMulti(ctx, list, height, belowCheckpointOptions(height)...)
	require.NoError(t, err)

	for i, r := range results {
		require.Equal(t, utxo.MultiTxCreated, r.Status, "tx %d: %v", i, r.Err)
	}

	require.Nil(t, results[1].Meta, "a transaction that wrote nothing has no record to report")
	require.Nil(t, results[2].Meta)
	require.NotNil(t, results[0].Meta)
	require.NotNil(t, results[3].Meta)

	require.Equal(t, []bool{true, false, false, true}, minedRecords(t, s, list, height))

	want := expectedNetState(w.Roots, list)
	got := readNetState(t, s, ctx, w.Roots, list, height)
	require.Equal(t, want.coins, got.coins)
	require.Zero(t, got.journal)

	// Block-ID recovery on a repeat reads the first transaction of the block; it keeps its
	// record because position 0 of a list has no earlier transaction to net against.
	m, err := s.Get(ctx, list[0].TxIDChainHash())
	require.NoError(t, err)
	require.Equal(t, []uint32{7}, m.BlockIDs)
}

// A repeat of the list, whole or after any subset of chunks committed, leaves the same state.
// A transaction that writes nothing is created again, as nothing of it exists; the others are
// found existing.
func TestNettedBelowWriteNothingRepeatAfterAnySubsetOfChunks(t *testing.T) {
	const height = 840

	withNettedBelowChunkTxs(t, 1)

	for mask := uint64(0); mask < 16; mask++ {
		t.Run(fmt.Sprintf("mask %#x", mask), func(t *testing.T) {
			s := newBodySkipStore(t, true)
			ctx := t.Context()

			w := tests.BuildMultiWorkload(t, 0x7b, 1, 1)
			w.StoreRoots(t, s, height-1)

			list := writeNothingList(t, w)

			crash := errors.NewProcessingError("injected crash")
			nettedBelowFault = func(chunk []int) error {
				for _, pos := range chunk {
					if mask&(1<<uint(pos)) == 0 { //nolint:gosec // test positions
						return crash
					}
				}

				return nil
			}

			t.Cleanup(func() { nettedBelowFault = nil })

			_, _ = s.SpendAndCreateMulti(ctx, list, height, belowCheckpointOptions(height)...)

			nettedBelowFault = nil

			results, err := s.SpendAndCreateMulti(ctx, list, height, belowCheckpointOptions(height)...)
			require.NoError(t, err)

			require.Equal(t, utxo.MultiTxCreated, results[1].Status)
			require.Equal(t, utxo.MultiTxCreated, results[2].Status)

			// The first crash cancels the chunks still running, so a chunk the mask lets
			// commit may not have; either answer is right for the two that keep a record.
			for _, i := range []int{0, 3} {
				require.Contains(t, []utxo.SpendAndCreateMultiStatus{utxo.MultiTxCreated, utxo.MultiTxExisted}, results[i].Status, "tx %d", i)
			}

			require.Equal(t, []bool{true, false, false, true}, minedRecords(t, s, list, height))
			require.Equal(t, expectedNetState(w.Roots, list).coins, readNetState(t, s, ctx, w.Roots, list, height).coins)
		})
	}
}

// A repeat of a block whose window the stamp has already passed is judged by the fence: every
// transaction it claims must already have its record. A transaction that writes nothing never
// had one, so it must not be part of that count, or the repeat would be refused as a boundary
// error.
func TestNettedBelowWriteNothingRepeatBehindTheStampFence(t *testing.T) {
	s := newBodySkipStore(t, true)
	ctx := t.Context()

	const height = 850

	w := tests.BuildMultiWorkload(t, 0x7c, 1, 1)
	w.StoreRoots(t, s, height-1)

	list := writeNothingList(t, w)

	_, err := s.SpendAndCreateMulti(ctx, list, height, belowCheckpointOptions(height)...)
	require.NoError(t, err)

	stampThrough(t, s, ctx, height/TxMinedPartitionBlocks, map[uint32]uint32{height: 7})

	results, err := s.SpendAndCreateMulti(ctx, list, height, belowCheckpointOptions(height)...)
	require.NoError(t, err)

	require.Equal(t, []utxo.SpendAndCreateMultiStatus{utxo.MultiTxExisted, utxo.MultiTxCreated, utxo.MultiTxCreated, utxo.MultiTxExisted},
		[]utxo.SpendAndCreateMultiStatus{results[0].Status, results[1].Status, results[2].Status, results[3].Status})
	require.Equal(t, expectedNetState(w.Roots, list).coins, readNetState(t, s, ctx, w.Roots, list, height).coins)
}

// With bodies written, every transaction writes at least its body, so every one keeps its
// record.
func TestNettedBelowWriteNothingNeedsTheBodySkip(t *testing.T) {
	s := newBodySkipStore(t, false)
	ctx := t.Context()

	const height = 860

	w := tests.BuildMultiWorkload(t, 0x7d, 1, 1)
	w.StoreRoots(t, s, height-1)

	list := writeNothingList(t, w)

	_, err := s.SpendAndCreateMulti(ctx, list, height, belowCheckpointOptions(height)...)
	require.NoError(t, err)

	require.Equal(t, []bool{true, true, true, true}, minedRecords(t, s, list, height))
}
