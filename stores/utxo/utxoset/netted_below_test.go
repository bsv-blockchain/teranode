package utxoset

import (
	"context"
	"fmt"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/stretchr/testify/require"
)

// netState is what the store holds for a workload after a write: how many coin rows each
// output of the roots and of the list has, how many spend-journal rows were written at the
// list's height, and how many transactions of the list have a mined record.
type netState struct {
	coins   map[string]int
	journal int
	mined   int
}

func coinLabel(txid *chainhash.Hash, vout uint32) string {
	return fmt.Sprintf("%s:%d", txid.String()[:12], vout)
}

// readNetState reads the store's state for the roots and transactions of a workload.
func readNetState(t *testing.T, s *Store, ctx context.Context, roots, txs []*bt.Tx, height uint32) netState {
	t.Helper()

	st := netState{coins: map[string]int{}}

	for _, tx := range append(append([]*bt.Tx{}, roots...), txs...) {
		id := tx.TxIDChainHash()

		for vout := range tx.Outputs {
			var n int
			require.NoError(t, s.pool.QueryRow(ctx, `SELECT count(*) FROM utxo WHERE ukey = $1 AND txid = $2`,
				Pack(id[:], uint32(vout)), id[:]).Scan(&n)) //nolint:gosec // test sizes

			st.coins[coinLabel(id, uint32(vout))] = n //nolint:gosec // test sizes
		}
	}

	require.NoError(t, s.pool.QueryRow(ctx, `SELECT count(*) FROM spend_journal WHERE spent_height = $1`, int32(height)).Scan(&st.journal)) //nolint:gosec // test height

	for _, tx := range txs {
		var n int
		require.NoError(t, s.pool.QueryRow(ctx, `SELECT count(*) FROM tx_mined WHERE txid = $1 AND mined_height = $2`,
			tx.TxIDChainHash()[:], int32(height)).Scan(&n)) //nolint:gosec // test height

		st.mined += n
	}

	return st
}

// expectedNetState is the net effect of applying txs below the checkpoint, from the list alone:
// every output a transaction of the list or a root has is a coin unless something in the list
// spends it, no journal row, and one mined record per transaction.
func expectedNetState(roots, txs []*bt.Tx) netState {
	spent := map[string]bool{}

	for _, tx := range txs {
		for _, in := range tx.Inputs {
			spent[coinLabel(in.PreviousTxIDChainHash(), in.PreviousTxOutIndex)] = true
		}
	}

	st := netState{coins: map[string]int{}, mined: len(txs)}

	for _, tx := range append(append([]*bt.Tx{}, roots...), txs...) {
		id := tx.TxIDChainHash()

		for vout, out := range tx.Outputs {
			k := coinLabel(id, uint32(vout)) //nolint:gosec // test sizes

			switch {
			case spent[k], out.LockingScript.IsData():
				st.coins[k] = 0
			default:
				st.coins[k] = 1
			}
		}
	}

	return st
}

func withNettedBelowChunkTxs(t *testing.T, n int) {
	t.Helper()

	old := nettedBelowChunkTxs
	nettedBelowChunkTxs = n

	t.Cleanup(func() { nettedBelowChunkTxs = old })
}

// Below the checkpoint the netted write writes the net effect of the list and nothing else:
// no coin for an output the list spends, no spend-journal row at all, and one mined record per
// transaction.
func TestNettedBelowWritesTheNetEffectAndNoJournal(t *testing.T) {
	s, ctx := newTestStore(t)

	const height = 800

	w := tests.BuildMultiWorkload(t, 0x71, 4, 5)
	w.StoreRoots(t, s, height-1)

	list := outpointOnly(w.Txs)

	results, err := s.SpendAndCreateMulti(ctx, list, height, belowCheckpointOptions(height)...)
	require.NoError(t, err)

	for i, r := range results {
		require.Equal(t, utxo.MultiTxCreated, r.Status, "tx %d: %v", i, r.Err)
	}

	require.Equal(t, expectedNetState(w.Roots, list), readNetState(t, s, ctx, w.Roots, list, height))
}

// A crash can leave any subset of the chunks committed, in any order: a child without its
// parent, a parent without its child. Repeating the whole list must then leave exactly the net
// effect: no coin the list spends comes back, no coin is written twice, nothing is missing.
func TestNettedBelowRepeatAfterAnySubsetOfChunks(t *testing.T) {
	const height = 900

	withNettedBelowChunkTxs(t, 1)

	// Each mask is the set of chunks (one transaction each, in list order) that commit before
	// the crash. The workload's first transactions are parents of the later ones, so masks
	// with only late bits set leave children without parents, and the reverse.
	masks := []uint64{0, 1, 0b1111, 1 << 7, 1<<15 | 1<<3, 0xAAAA, 0x5555, 0xFFFF}

	for _, mask := range masks {
		t.Run(fmt.Sprintf("mask %#x", mask), func(t *testing.T) {
			s, ctx := newTestStore(t)

			w := tests.BuildMultiWorkload(t, 0x72, 4, 4)
			w.StoreRoots(t, s, height-1)

			list := outpointOnly(w.Txs)

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

			_, err := s.SpendAndCreateMulti(ctx, list, height, belowCheckpointOptions(height)...)
			if mask != 0xFFFF {
				require.Error(t, err)
			}

			nettedBelowFault = nil

			results, err := s.SpendAndCreateMulti(ctx, outpointOnly(w.Txs), height, belowCheckpointOptions(height)...)
			require.NoError(t, err)

			for i, r := range results {
				require.Contains(t, []utxo.SpendAndCreateMultiStatus{utxo.MultiTxCreated, utxo.MultiTxExisted}, r.Status, "tx %d: %v", i, r.Err)
			}

			require.Equal(t, expectedNetState(w.Roots, list), readNetState(t, s, ctx, w.Roots, list, height))
		})
	}
}

// A transaction with no spendable output is in the list like any other: it gets a mined record,
// its inputs are spent, and it writes no coin.
func TestNettedBelowUnspendableTransactionInTheList(t *testing.T) {
	s, ctx := newTestStore(t)

	const height = 810

	w := tests.BuildMultiWorkload(t, 0x73, 1, 2)
	w.StoreRoots(t, s, height-1)

	data := bt.NewTx()
	require.NoError(t, data.From(w.Txs[0].TxIDChainHash().String(), 0, "76a914000000000000000000000000000000000000000088ac", 1000))

	script, err := bscript.NewFromASM("OP_FALSE OP_RETURN 74657374")
	require.NoError(t, err)

	data.AddOutput(&bt.Output{Satoshis: 0, LockingScript: script})

	list := outpointOnly(append(append([]*bt.Tx{}, w.Txs...), data))

	results, err := s.SpendAndCreateMulti(ctx, list, height, belowCheckpointOptions(height)...)
	require.NoError(t, err)

	for i, r := range results {
		require.Equal(t, utxo.MultiTxCreated, r.Status, "tx %d: %v", i, r.Err)
	}

	require.Equal(t, expectedNetState(w.Roots, list), readNetState(t, s, ctx, w.Roots, list, height))
}

// A transaction that reached the store unmined before its block holds real coins and has made
// its spends. In the list it is not written again, and its child in the list spends its coin
// for real rather than netting it.
func TestNettedBelowRebroadcastTransactionStoredBeforeItsBlock(t *testing.T) {
	s, ctx := newTestStore(t)

	const height = 820

	w := tests.BuildMultiWorkload(t, 0x74, 2, 2)
	w.StoreRoots(t, s, height-1)

	parent := w.Txs[0]

	// The parent arrives unmined first: identity route, its own spends journalled.
	_, _, err := s.SpendAndCreate(ctx, parent, height-1)
	require.NoError(t, err)

	list := outpointOnly(w.Txs)

	results, err := s.SpendAndCreateMulti(ctx, list, height, belowCheckpointOptions(height)...)
	require.NoError(t, err)
	require.Equal(t, utxo.MultiTxExisted, results[0].Status, "the parent was already stored")

	for i, r := range results[1:] {
		require.Equal(t, utxo.MultiTxCreated, r.Status, "tx %d: %v", i+1, r.Err)
	}

	got := readNetState(t, s, ctx, w.Roots, list, height)
	want := expectedNetState(w.Roots, list)

	require.Equal(t, want.coins, got.coins, "the parent's coins its children spend are gone, the rest live once")
}

// A coin the list spends from outside that is not in the store is a store fault below the
// checkpoint: the write fails and the failing chunk writes nothing.
func TestNettedBelowMissingOutsideCoinFails(t *testing.T) {
	s, ctx := newTestStore(t)

	const height = 830

	withNettedBelowChunkTxs(t, 256)

	w := tests.BuildMultiWorkload(t, 0x75, 2, 2) // roots never stored

	list := outpointOnly(w.Txs)

	_, err := s.SpendAndCreateMulti(ctx, list, height, belowCheckpointOptions(height)...)
	require.Error(t, err)
	require.False(t, utxo.IsSpendAndCreateMultiRefused(err), "a store fault, not a list the caller may apply another way")

	require.Zero(t, readNetState(t, s, ctx, nil, list, height).mined, "the failing chunk wrote nothing")
}

// Below the checkpoint a list whose creates would be locked cannot be netted, and the store says
// so rather than silently writing it another, slower way.
func TestNettedBelowRefusesLockedCreates(t *testing.T) {
	s, ctx := newTestStore(t)

	const height = 840

	w := tests.BuildMultiWorkload(t, 0x76, 1, 2)
	w.StoreRoots(t, s, height-1)

	_, err := s.SpendAndCreateMulti(ctx, outpointOnly(w.Txs), height, append(belowCheckpointOptions(height), utxo.WithLocked(true))...)
	require.Error(t, err)
	require.Contains(t, err.Error(), "below the checkpoint")
}

// A coinbase never goes through the list: it keeps the create path that refuses the historic
// duplicate coinbases.
func TestNettedBelowRefusesACoinbase(t *testing.T) {
	s, ctx := newTestStore(t)

	const height = 850

	cb := bt.NewTx()
	require.NoError(t, cb.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0))
	cb.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x03, 0x52, 0x03, 0x00})

	script, err := bscript.NewFromHexString("76a914000000000000000000000000000000000000000088ac")
	require.NoError(t, err)

	cb.AddOutput(&bt.Output{Satoshis: 5000000000, LockingScript: script})

	_, err = s.SpendAndCreateMulti(ctx, []*bt.Tx{cb}, height, belowCheckpointOptions(height)...)
	require.Error(t, err)
	require.Contains(t, err.Error(), "coinbase")
}

// Each transaction's mined record carries the subtree index it sits in, from WithSubtreeIdxs,
// however the list is cut into chunks.
func TestNettedBelowKeepsEachSubtreeIdx(t *testing.T) {
	s, ctx := newTestStore(t)

	const height = 860

	withNettedBelowChunkTxs(t, 3)

	w := tests.BuildMultiWorkload(t, 0x77, 3, 3)
	w.StoreRoots(t, s, height-1)

	list := outpointOnly(w.Txs)

	idxs := make([]int, len(list))
	for i := range idxs {
		idxs[i] = 5 + i
	}

	_, err := s.SpendAndCreateMulti(ctx, list, height, append(belowCheckpointOptions(height), utxo.WithSubtreeIdxs(idxs))...)
	require.NoError(t, err)

	for i, tx := range list {
		var idx int32
		require.NoError(t, s.pool.QueryRow(ctx, `SELECT subtree_idx FROM tx_mined WHERE txid = $1 AND mined_height = $2`,
			tx.TxIDChainHash()[:], int32(height)).Scan(&idx))
		require.Equal(t, int32(idxs[i]), idx, "tx %d", i) //nolint:gosec // test sizes
	}
}

// The build before version 2 spent every outside input of a list first, deleting the coin and
// writing a journal row that names the spender, and only then created the transactions. A
// restart onto version 2 between those steps leaves transactions with their outside spends
// made and no mined record. Version 2 must accept those spends as already made by the same
// transaction, not fail the list for a missing coin. Mainnet stopped at block 701,233 on
// 2026-10-07 on exactly this.
func TestNettedBelowAcceptsOutsideSpendsTheOldWriteAlreadyMade(t *testing.T) {
	s, ctx := newTestStore(t)

	const height = 950

	w := tests.BuildMultiWorkload(t, 0x7a, 4, 4)
	w.StoreRoots(t, s, height-1)

	list := outpointOnly(w.Txs)

	inList := map[chainhash.Hash]bool{}
	for _, tx := range list {
		inList[*tx.TxIDChainHash()] = true
	}

	// The old write's first step: every transaction with an outside input spends it, through
	// the per-transaction spend path, which writes the journal row naming it.
	spent := 0

	for _, tx := range list {
		outside := false

		for _, in := range tx.Inputs {
			if !inList[*in.PreviousTxIDChainHash()] {
				outside = true
			}
		}

		if !outside {
			continue
		}

		_, _, err := s.SpendAndCreate(ctx, tx, height, utxo.WithSpendOnly(), utxo.WithIgnoreLocked(true), utxo.WithSkipUTXOHashCheck(true))
		require.NoError(t, err)

		spent++
	}

	require.Positive(t, spent, "the workload must have transactions with outside inputs")

	results, err := s.SpendAndCreateMulti(ctx, outpointOnly(w.Txs), height, belowCheckpointOptions(height)...)
	require.NoError(t, err, "spends the old write already made for the same transaction are accepted")

	for i, r := range results {
		require.Equal(t, utxo.MultiTxCreated, r.Status, "tx %d: %v", i, r.Err)
	}

	got := readNetState(t, s, ctx, w.Roots, list, height)
	want := expectedNetState(w.Roots, list)
	want.journal = got.journal // the old write's journal rows stay; version 2 adds none

	require.Equal(t, want, got)
}

// A chunk can hold transactions that already exist and transactions that are new: a repeat whose
// chunks are cut differently from the first attempt's, or a block the build before version 2
// part-wrote. Only the new transactions' coins are written, and nothing else is.
func TestNettedBelowRepeatWithChunksMixingNewAndExisting(t *testing.T) {
	s, ctx := newTestStore(t)

	const height = 960

	w := tests.BuildMultiWorkload(t, 0x7b, 4, 4)
	w.StoreRoots(t, s, height-1)

	list := outpointOnly(w.Txs)

	// First attempt: one transaction per chunk, one chunk at a time, and the crash comes at the
	// second chunk, so exactly the first transaction is written. Its output 1 is spent by no
	// transaction of the list, so on the repeat it is an existing transaction with a coin.
	withNettedBelowChunkTxs(t, 1)

	workers := s.settings.UtxoStore.NettedBelowWorkers
	s.settings.UtxoStore.NettedBelowWorkers = 1

	t.Cleanup(func() { s.settings.UtxoStore.NettedBelowWorkers = workers })

	crash := errors.NewProcessingError("injected crash")
	nettedBelowFault = func(chunk []int) error {
		if chunk[0] >= 1 {
			return crash
		}

		return nil
	}

	t.Cleanup(func() { nettedBelowFault = nil })

	_, err := s.SpendAndCreateMulti(ctx, list, height, belowCheckpointOptions(height)...)
	require.Error(t, err)

	var mined int
	require.NoError(t, s.pool.QueryRow(ctx, `SELECT count(*) FROM tx_mined WHERE mined_height = $1`, int32(height)).Scan(&mined))
	require.Equal(t, 1, mined, "exactly the first transaction was written before the crash")

	// Repeat with the whole list in one chunk, so the chunk mixes existing and new.
	nettedBelowFault = nil
	nettedBelowChunkTxs = len(list)

	results, err := s.SpendAndCreateMulti(ctx, outpointOnly(w.Txs), height, belowCheckpointOptions(height)...)
	require.NoError(t, err)

	for i, r := range results {
		require.Contains(t, []utxo.SpendAndCreateMultiStatus{utxo.MultiTxCreated, utxo.MultiTxExisted}, r.Status, "tx %d: %v", i, r.Err)
	}

	require.Equal(t, expectedNetState(w.Roots, list), readNetState(t, s, ctx, w.Roots, list, height))
}
