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

// nettedFamily builds, on a stored root, a family in which two transactions have no coin to
// write and no coin to delete below the checkpoint when bodies are skipped:
//
//	0: spends the root, one output, spent by 1          (an outside delete)
//	1: spends 0:0, two outputs, spent by 2 and 3         (every input and output netted)
//	2: spends 1:0, one data output                       (netted input, no spendable output)
//	3: spends 1:1, one output nobody in the list spends  (writes a coin)
//
// The transactions keep their extended inputs, as a validator stores them. Pass them through
// outpointOnly for a list below the checkpoint.
func nettedFamily(t *testing.T, w *tests.MultiWorkload) []*bt.Tx {
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

	return []*bt.Tx{t0, t1, t2, t3}
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

func newBodySkipStore(t *testing.T) *Store {
	t.Helper()

	s, _ := newTestStoreWith(t, func(ts *settings.Settings) {
		ts.UtxoStore.SkipTxBodyBelowCheckpoint = true
	})

	return s
}

// withNettedBelowWorkers sets how many chunks of the below-checkpoint write run at once.
func withNettedBelowWorkers(t *testing.T, s *Store, n int) {
	t.Helper()

	old := s.settings.UtxoStore.NettedBelowWorkers
	s.settings.UtxoStore.NettedBelowWorkers = n

	t.Cleanup(func() { s.settings.UtxoStore.NettedBelowWorkers = old })
}

// Below the checkpoint, with bodies skipped, a transaction with no coin to write and no coin to
// delete still writes its mined record. The coins are the list's net effect.
func TestNettedBelowEveryTransactionWritesItsRecord(t *testing.T) {
	s := newBodySkipStore(t)
	ctx := t.Context()

	const height = 830

	w := tests.BuildMultiWorkload(t, 0x7a, 1, 1)
	w.StoreRoots(t, s, height-1)

	list := outpointOnly(nettedFamily(t, w))

	results, err := s.SpendAndCreateMulti(ctx, list, height, belowCheckpointOptions(height)...)
	require.NoError(t, err)

	for i, r := range results {
		require.Equal(t, utxo.MultiTxCreated, r.Status, "tx %d: %v", i, r.Err)
		require.NotNil(t, r.Meta, "tx %d", i)
	}

	require.Equal(t, []bool{true, true, true, true}, minedRecords(t, s, list, height))

	want := expectedNetState(w.Roots, list)
	got := readNetState(t, s, ctx, w.Roots, list, height)
	require.Equal(t, want.coins, got.coins)
	require.Zero(t, got.journal)

	m, err := s.Get(ctx, list[0].TxIDChainHash())
	require.NoError(t, err)
	require.Equal(t, []uint32{7}, m.BlockIDs)
}

// A repeat of the list, whole or after any subset of chunks committed, leaves the same state.
func TestNettedBelowFamilyRepeatAfterAnySubsetOfChunks(t *testing.T) {
	const height = 840

	withNettedBelowChunkTxs(t, 1)

	for mask := uint64(0); mask < 16; mask++ {
		t.Run(fmt.Sprintf("mask %#x", mask), func(t *testing.T) {
			s := newBodySkipStore(t)
			ctx := t.Context()

			w := tests.BuildMultiWorkload(t, 0x7b, 1, 1)
			w.StoreRoots(t, s, height-1)

			list := outpointOnly(nettedFamily(t, w))

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

			// The first crash cancels the chunks still running, so a chunk the mask lets
			// commit may not have. Either answer is right.
			for i, r := range results {
				require.Contains(t, []utxo.SpendAndCreateMultiStatus{utxo.MultiTxCreated, utxo.MultiTxExisted}, r.Status, "tx %d", i)
			}

			require.Equal(t, []bool{true, true, true, true}, minedRecords(t, s, list, height))
			require.Equal(t, expectedNetState(w.Roots, list).coins, readNetState(t, s, ctx, w.Roots, list, height).coins)
		})
	}
}

// A repeat of a block whose window the stamp has already passed is judged by the fence: every
// transaction it claims must already have its record. Every transaction wrote one.
func TestNettedBelowFamilyRepeatBehindTheStampFence(t *testing.T) {
	s := newBodySkipStore(t)
	ctx := t.Context()

	const height = 850

	w := tests.BuildMultiWorkload(t, 0x7c, 1, 1)
	w.StoreRoots(t, s, height-1)

	list := outpointOnly(nettedFamily(t, w))

	_, err := s.SpendAndCreateMulti(ctx, list, height, belowCheckpointOptions(height)...)
	require.NoError(t, err)

	stampThrough(t, s, ctx, height/TxMinedPartitionBlocks, map[uint32]uint32{height: 7})

	results, err := s.SpendAndCreateMulti(ctx, list, height, belowCheckpointOptions(height)...)
	require.NoError(t, err)

	for i, r := range results {
		require.Equal(t, utxo.MultiTxExisted, r.Status, "tx %d: %v", i, r.Err)
	}

	require.Equal(t, expectedNetState(w.Roots, list).coins, readNetState(t, s, ctx, w.Roots, list, height).coins)
}

// Another writer stores a transaction of the list unmined after the first identity probe and
// before the transaction's chunk. The chunk reads the identity rows again under its locks, takes
// the identity route, and deletes the outputs of that transaction that the list spends.
//
// The list is X, t0, t1, t2, t3. X spends the root's other output, and its chunk runs first. t0
// reached the store unmined before the list, so its output 0 is a coin. While X's chunk runs, a
// validator stores t1 unmined: it spends t0:0 and writes both outputs of t1 as coins. Both are
// spent in the list.
func TestNettedBelowTransactionStoredAfterTheProbe(t *testing.T) {
	s := newBodySkipStore(t)
	ctx := t.Context()

	const height = 870

	withNettedBelowChunkTxs(t, 1)
	withNettedBelowWorkers(t, s, 1)

	w := tests.BuildMultiWorkload(t, 0x7e, 1, 1)
	w.StoreRoots(t, s, height-1)

	family := nettedFamily(t, w)

	x := bt.NewTx()
	require.NoError(t, x.FromUTXOs(&bt.UTXO{
		TxIDHash:      w.Roots[0].TxIDChainHash(),
		Vout:          1,
		LockingScript: w.Roots[0].Outputs[1].LockingScript,
		Satoshis:      w.Roots[0].Outputs[1].Satoshis,
	}))
	x.AddOutput(&bt.Output{Satoshis: w.Roots[0].Outputs[1].Satoshis - 1_000, LockingScript: w.Roots[0].Outputs[1].LockingScript})

	_, _, err := s.SpendAndCreate(ctx, family[0], height-1)
	require.NoError(t, err)

	full := append([]*bt.Tx{x}, family...)
	list := outpointOnly(full)

	stored := false
	nettedBelowFault = func(chunk []int) error {
		if chunk[0] != 0 || stored {
			return nil
		}

		stored = true

		_, _, err := s.SpendAndCreate(ctx, full[2], height-1)

		return err
	}

	t.Cleanup(func() { nettedBelowFault = nil })

	_, err = s.SpendAndCreateMulti(ctx, list, height, belowCheckpointOptions(height)...)
	require.NoError(t, err)
	require.True(t, stored)

	require.Equal(t, expectedNetState(w.Roots, list).coins, readNetState(t, s, ctx, w.Roots, list, height).coins,
		"an output the list spends is not a coin")
}

// After a chunk commits, another writer offers a transaction of the chunk unmined. The chunk
// wrote the transaction's mined record, so the claim does not take and the store gets no second
// copy of the transaction with every output a coin. This is the create step of a validator that
// stores t1 while the chunk of t1's parent still runs.
func TestNettedBelowTransactionOfferedAfterItsChunk(t *testing.T) {
	s := newBodySkipStore(t)
	ctx := t.Context()

	const height = 880

	w := tests.BuildMultiWorkload(t, 0x7f, 1, 1)
	w.StoreRoots(t, s, height-1)

	family := nettedFamily(t, w)
	list := outpointOnly(family)

	_, err := s.SpendAndCreateMulti(ctx, list, height, belowCheckpointOptions(height)...)
	require.NoError(t, err)

	_, err = s.Create(ctx, family[1], height-1)
	require.ErrorIs(t, err, errors.ErrTxExists)

	require.Equal(t, expectedNetState(w.Roots, list).coins, readNetState(t, s, ctx, w.Roots, list, height).coins,
		"an output the list spends is not a coin")
}
