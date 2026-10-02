package tests

import (
	"context"
	"fmt"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/stretchr/testify/require"
)

// MultiWorkload is a block-shaped list of chained transactions: Roots are
// outside parents (stored before the call), Txs is the list in parent-first
// order. Every input is extended from its parent's output, as a validated
// transaction is.
type MultiWorkload struct {
	Roots []*bt.Tx
	Txs   []*bt.Tx
}

const multiTxOutputValue = 100_000

// BuildMultiWorkload builds levels x width transactions. Level 0 transaction i
// spends root i's output 0 and, for even i, root i's output 1. Level k
// transaction i spends output 0 of level k-1 transaction i and, when i%3 == 0,
// output 1 of level k-1 transaction i+1, so some transactions have two parents
// in the list. seed makes the txids distinct between two workloads of the same
// shape.
func BuildMultiWorkload(t testing.TB, seed byte, levels, width int) *MultiWorkload {
	t.Helper()

	w := &MultiWorkload{}

	for i := 0; i < width; i++ {
		root := bt.NewTx()

		var prev chainhash.Hash
		prev[0] = seed
		prev[1] = byte(i)
		prev[2] = byte(i >> 8)
		prev[3] = 0x5c

		require.NoError(t, root.FromUTXOs(&bt.UTXO{
			TxIDHash:      &prev,
			Vout:          0,
			LockingScript: Tx.Inputs[0].PreviousTxScript,
			Satoshis:      4 * multiTxOutputValue,
		}))
		root.Inputs[0].UnlockingScript = dummyUnlockingScript

		for o := 0; o < 2; o++ {
			require.NoError(t, root.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", multiTxOutputValue))
		}

		w.Roots = append(w.Roots, root)
	}

	prevLevel := w.Roots

	for level := 0; level < levels; level++ {
		row := make([]*bt.Tx, width)

		for i := 0; i < width; i++ {
			tx := bt.NewTx()
			tx.LockTime = uint32(seed)<<24 | uint32(level)<<12 | uint32(i) //nolint:gosec // test data

			addMultiInput(t, tx, prevLevel[i], 0)

			if level == 0 && i%2 == 0 {
				addMultiInput(t, tx, prevLevel[i], 1)
			}

			if level > 0 && i%3 == 0 && i+1 < width {
				addMultiInput(t, tx, prevLevel[i+1], 1)
			}

			var in uint64
			for _, input := range tx.Inputs {
				in += input.PreviousTxSatoshis
			}

			// Two outputs and a fee of 200 satoshis.
			out := (in - 200) / 2
			for o := 0; o < 2; o++ {
				require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", out))
			}

			row[i] = tx
			w.Txs = append(w.Txs, tx)
		}

		prevLevel = row
	}

	return w
}

// spendOneOutput builds a transaction that spends one output of parent, paying
// it on less a 200-satoshi fee.
func spendOneOutput(t testing.TB, parent *bt.Tx, vout uint32) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()
	tx.LockTime = 0xabc
	addMultiInput(t, tx, parent, vout)
	require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", parent.Outputs[vout].Satoshis-200))

	return tx
}

func addMultiInput(t testing.TB, tx, parent *bt.Tx, vout uint32) {
	t.Helper()

	require.NoError(t, tx.FromUTXOs(&bt.UTXO{
		TxIDHash:      parent.TxIDChainHash(),
		Vout:          vout,
		LockingScript: parent.Outputs[vout].LockingScript,
		Satoshis:      parent.Outputs[vout].Satoshis,
	}))
	tx.Inputs[len(tx.Inputs)-1].UnlockingScript = dummyUnlockingScript
}

// StoreRoots creates the workload's outside parents.
func (w *MultiWorkload) StoreRoots(t testing.TB, db utxostore.Store, blockHeight uint32) {
	t.Helper()

	for _, root := range w.Roots {
		_, _, err := db.SpendAndCreate(context.Background(), root, blockHeight, utxostore.WithCreateOnly())
		require.NoError(t, err)
	}
}

// multiRecord is a store record with every txid replaced by its position in the
// workload, so two workloads of the same shape compare equal field by field.
type multiRecord struct {
	Fee, SizeInBytes         uint64
	Inpoints                 []string
	BlockIDs, BlockHeights   []uint32
	SubtreeIdxs              []int
	IsCoinbase, Conflicting  bool
	Locked                   bool
	UnminedSince             uint32
	OutputStatus             []int
	OutputSpenderLabel       []string
	OutputSpenderVin         []int
	ExistsInStore            bool
	TxInpointsParentHashNums int
}

func (w *MultiWorkload) labels() map[chainhash.Hash]string {
	labels := make(map[chainhash.Hash]string, len(w.Roots)+len(w.Txs))

	for i, r := range w.Roots {
		labels[*r.TxIDChainHash()] = fmt.Sprintf("root%d", i)
	}

	for i, tx := range w.Txs {
		labels[*tx.TxIDChainHash()] = fmt.Sprintf("tx%d", i)
	}

	return labels
}

// Records reads every list transaction's record and every output's spend state.
func (w *MultiWorkload) Records(t testing.TB, db utxostore.Store) []multiRecord {
	t.Helper()

	ctx := context.Background()
	labels := w.labels()
	label := func(h chainhash.Hash) string {
		if l, ok := labels[h]; ok {
			return l
		}

		return h.String()
	}

	records := make([]multiRecord, len(w.Txs))

	for i, tx := range w.Txs {
		md, err := db.Get(ctx, tx.TxIDChainHash(), fields.Fee, fields.SizeInBytes, fields.TxInpoints, fields.BlockIDs,
			fields.BlockHeights, fields.SubtreeIdxs, fields.IsCoinbase, fields.Conflicting, fields.Locked, fields.UnminedSince)
		if errors.Is(err, errors.ErrTxNotFound) {
			continue
		}

		require.NoError(t, err)

		r := multiRecord{
			ExistsInStore: true,
			Fee:           md.Fee, SizeInBytes: md.SizeInBytes,
			BlockIDs: md.BlockIDs, BlockHeights: md.BlockHeights, SubtreeIdxs: md.SubtreeIdxs,
			IsCoinbase: md.IsCoinbase, Conflicting: md.Conflicting, Locked: md.Locked,
			UnminedSince:             md.UnminedSince,
			TxInpointsParentHashNums: len(md.TxInpoints.ParentTxHashes),
		}

		for _, in := range md.TxInpoints.GetTxInpoints() {
			r.Inpoints = append(r.Inpoints, fmt.Sprintf("%s:%d", label(in.Hash), in.Index))
		}

		for vout, out := range tx.Outputs {
			utxoHash, err := util.UTXOHashFromOutput(tx.TxIDChainHash(), out, uint32(vout)) //nolint:gosec // test data
			require.NoError(t, err)

			resp, err := db.GetSpend(ctx, &utxostore.Spend{TxID: tx.TxIDChainHash(), Vout: uint32(vout), UTXOHash: utxoHash}) //nolint:gosec // test data
			require.NoError(t, err)

			r.OutputStatus = append(r.OutputStatus, resp.Status)

			if resp.SpendingData != nil {
				r.OutputSpenderLabel = append(r.OutputSpenderLabel, label(*resp.SpendingData.TxID))
				r.OutputSpenderVin = append(r.OutputSpenderVin, resp.SpendingData.Vin)
			} else {
				r.OutputSpenderLabel = append(r.OutputSpenderLabel, "")
				r.OutputSpenderVin = append(r.OutputSpenderVin, -1)
			}
		}

		records[i] = r
	}

	return records
}

// RootSpends reads which list transaction spent each root output.
func (w *MultiWorkload) RootSpends(t testing.TB, db utxostore.Store) []string {
	t.Helper()

	labels := w.labels()

	var out []string

	for _, root := range w.Roots {
		for vout, o := range root.Outputs {
			utxoHash, err := util.UTXOHashFromOutput(root.TxIDChainHash(), o, uint32(vout)) //nolint:gosec // test data
			require.NoError(t, err)

			resp, err := db.GetSpend(context.Background(), &utxostore.Spend{TxID: root.TxIDChainHash(), Vout: uint32(vout), UTXOHash: utxoHash}) //nolint:gosec // test data
			require.NoError(t, err)

			if resp.SpendingData == nil {
				out = append(out, "")
				continue
			}

			out = append(out, fmt.Sprintf("%s:%d", labels[*resp.SpendingData.TxID], resp.SpendingData.Vin))
		}
	}

	return out
}

// MarkMined runs the record-mined write for every list transaction.
func (w *MultiWorkload) MarkMined(t testing.TB, db utxostore.Store, blockID, blockHeight uint32) {
	t.Helper()

	hashes := make([]*chainhash.Hash, len(w.Txs))
	for i, tx := range w.Txs {
		hashes[i] = tx.TxIDChainHash()
	}

	_, err := db.SetMinedMulti(context.Background(), hashes, utxostore.MinedBlockInfo{BlockID: blockID, BlockHeight: blockHeight, SubtreeIdx: 1})
	require.NoError(t, err)
}

func loopSpendAndCreate(t testing.TB, db utxostore.Store, txs []*bt.Tx, blockHeight uint32, opts ...utxostore.CreateOption) {
	t.Helper()

	for _, tx := range txs {
		_, _, err := db.SpendAndCreate(context.Background(), tx, blockHeight, opts...)
		require.NoError(t, err)
	}
}

func requireAllStatus(t testing.TB, results []utxostore.SpendAndCreateMultiResult, want utxostore.SpendAndCreateMultiStatus) {
	t.Helper()

	for i, r := range results {
		require.Equal(t, want, r.Status, "tx %d: %v", i, r.Err)
	}
}

// SpendAndCreateMultiMatchesLoop is design test 1: a list written through
// SpendAndCreateMulti leaves exactly the records a plain loop of SpendAndCreate
// leaves, before and after the record-mined write, and returns the same Meta.
func SpendAndCreateMultiMatchesLoop(t *testing.T, db utxostore.Store) {
	ctx := context.Background()

	const height = 200

	loop := BuildMultiWorkload(t, 0x10, 5, 7)
	multi := BuildMultiWorkload(t, 0x11, 5, 7)

	loop.StoreRoots(t, db, height-1)
	multi.StoreRoots(t, db, height-1)

	loopMetas := make([]*meta.Data, 0, len(loop.Txs))

	for _, tx := range loop.Txs {
		md, _, err := db.SpendAndCreate(ctx, tx, height, utxostore.WithIgnoreLocked(true))
		require.NoError(t, err)

		loopMetas = append(loopMetas, md)
	}

	results, err := db.SpendAndCreateMulti(ctx, multi.Txs, height, utxostore.WithIgnoreLocked(true))
	require.NoError(t, err)
	require.Len(t, results, len(multi.Txs))
	requireAllStatus(t, results, utxostore.MultiTxCreated)

	for i, r := range results {
		require.NotNil(t, r.Meta, "tx %d", i)
		require.Equal(t, loopMetas[i].Fee, r.Meta.Fee, "tx %d fee", i)
		require.Equal(t, loopMetas[i].SizeInBytes, r.Meta.SizeInBytes, "tx %d size", i)
		require.Equal(t, loopMetas[i].IsCoinbase, r.Meta.IsCoinbase, "tx %d", i)
		require.Equal(t, loopMetas[i].Conflicting, r.Meta.Conflicting, "tx %d", i)
		require.Empty(t, r.Meta.BlockIDs, "tx %d: created unmined", i)
		require.Equal(t, len(loopMetas[i].TxInpoints.ParentTxHashes), len(r.Meta.TxInpoints.ParentTxHashes), "tx %d", i)
		require.Equal(t, len(loopMetas[i].TxInpoints.GetTxInpoints()), len(r.Meta.TxInpoints.GetTxInpoints()), "tx %d", i)
	}

	require.Equal(t, loop.Records(t, db), multi.Records(t, db), "records before the record-mined write")
	require.Equal(t, loop.RootSpends(t, db), multi.RootSpends(t, db))

	loop.MarkMined(t, db, 901, height)
	multi.MarkMined(t, db, 901, height)

	require.Equal(t, loop.Records(t, db), multi.Records(t, db), "records after the record-mined write")
}

// SpendAndCreateMultiSpendFailsPartway is design test 3. An outside parent spent
// beforehand by an unmined transaction fails its spender with ErrSpent, every
// descendant in the list is ParentFailed with nothing of it in the store, and
// every other transaction is written.
func SpendAndCreateMultiSpendFailsPartway(t *testing.T, db utxostore.Store) {
	ctx := context.Background()

	const height = 300

	w := BuildMultiWorkload(t, 0x20, 4, 6)
	w.StoreRoots(t, db, height-1)

	// An unmined transaction spends root 1's output 0 first, which level 0
	// transaction 1 needs.
	thief := spendOneOutput(t, w.Roots[1], 0)
	_, _, err := db.SpendAndCreate(ctx, thief, height)
	require.NoError(t, err)

	results, err := db.SpendAndCreateMulti(ctx, w.Txs, height, utxostore.WithIgnoreLocked(true))
	require.NoError(t, err)

	const width = 6

	// Level 0 tx 1 fails. Its descendants: level k tx 1 through output 0, and
	// level k tx 0 (0%3 == 0) through output 1 of level k-1 tx 1, and so on down.
	failed := map[int]bool{1: true}
	for level := 1; level < 4; level++ {
		for i := 0; i < width; i++ {
			idx := level*width + i
			if failed[(level-1)*width+i] || (i%3 == 0 && i+1 < width && failed[(level-1)*width+i+1]) {
				failed[idx] = true
			}
		}
	}

	records := w.Records(t, db)

	for i, r := range results {
		switch {
		case i == 1:
			require.Equal(t, utxostore.MultiTxFailed, r.Status)
			require.ErrorIs(t, r.Err, errors.ErrSpent)
		case failed[i]:
			require.Equal(t, utxostore.MultiTxParentFailed, r.Status, "tx %d", i)
			require.False(t, records[i].ExistsInStore, "tx %d must not be written", i)
		default:
			require.Equal(t, utxostore.MultiTxCreated, r.Status, "tx %d: %v", i, r.Err)
			require.True(t, records[i].ExistsInStore, "tx %d", i)
		}
	}

	// No spend of a failed or parent-failed transaction is left anywhere: its
	// parents' outputs are either unspent or spent by someone else.
	labels := w.labels()
	for i := range w.Txs {
		if !failed[i] {
			continue
		}

		for _, r := range records {
			for _, spender := range r.OutputSpenderLabel {
				require.NotEqual(t, labels[*w.Txs[i].TxIDChainHash()], spender, "a spend by failed tx %d is still in place", i)
			}
		}
	}

	for _, s := range w.RootSpends(t, db) {
		require.NotEqual(t, "tx1:0", s, "the failed spend was not rolled back")
	}
}

// SpendAndCreateMultiParentExists is design test 4. A parent in the list that
// propagation already created is Existed and not written; its child spends it.
func SpendAndCreateMultiParentExists(t *testing.T, db utxostore.Store) {
	ctx := context.Background()

	const height = 400

	w := BuildMultiWorkload(t, 0x30, 2, 3)
	w.StoreRoots(t, db, height-1)

	// Propagation created level 0 transaction 1 unmined, spending its inputs.
	_, _, err := db.SpendAndCreate(ctx, w.Txs[1], height-1)
	require.NoError(t, err)

	results, err := db.SpendAndCreateMulti(ctx, w.Txs, height, utxostore.WithIgnoreLocked(true))
	require.NoError(t, err)

	for i, r := range results {
		if i == 1 {
			require.Equal(t, utxostore.MultiTxExisted, r.Status)
			continue
		}

		require.Equal(t, utxostore.MultiTxCreated, r.Status, "tx %d: %v", i, r.Err)
	}

	// Level 1 tx 1 (list index 4) spent output 0 of the existing record, and
	// level 1 tx 0 (index 3) spent its output 1.
	records := w.Records(t, db)
	require.Equal(t, []string{"tx3", "tx4"}, []string{records[1].OutputSpenderLabel[1], records[1].OutputSpenderLabel[0]})
	require.Equal(t, uint32(height-1), records[1].UnminedSince, "the existing record was not rewritten")

	// A conflicting unmined spender of an existing parent's output fails the
	// child in the list.
	w2 := BuildMultiWorkload(t, 0x31, 2, 2)
	w2.StoreRoots(t, db, height-1)

	_, _, err = db.SpendAndCreate(ctx, w2.Txs[0], height-1)
	require.NoError(t, err)

	thief := spendOneOutput(t, w2.Txs[0], 0)
	_, _, err = db.SpendAndCreate(ctx, thief, height-1)
	require.NoError(t, err)

	results, err = db.SpendAndCreateMulti(ctx, w2.Txs, height, utxostore.WithIgnoreLocked(true))
	require.NoError(t, err)
	require.Equal(t, utxostore.MultiTxExisted, results[0].Status)
	require.Equal(t, utxostore.MultiTxCreated, results[1].Status, "%v", results[1].Err)
	require.Equal(t, utxostore.MultiTxFailed, results[2].Status)
	require.ErrorIs(t, results[2].Err, errors.ErrSpent)
	require.Equal(t, utxostore.MultiTxCreated, results[3].Status, "%v", results[3].Err)
}

// SpendAndCreateMultiRefusalWritesNothing is the store half of design test 5.
func SpendAndCreateMultiRefusalWritesNothing(t *testing.T, db utxostore.Store) {
	ctx := context.Background()

	const height = 500

	w := BuildMultiWorkload(t, 0x40, 2, 2)
	w.StoreRoots(t, db, height-1)

	reversed := []*bt.Tx{w.Txs[2], w.Txs[3], w.Txs[0], w.Txs[1]}

	results, err := db.SpendAndCreateMulti(ctx, reversed, height)
	require.True(t, utxostore.IsSpendAndCreateMultiRefused(err), "got %v", err)
	require.Nil(t, results)

	for _, r := range w.Records(t, db) {
		require.False(t, r.ExistsInStore)
	}

	for _, s := range w.RootSpends(t, db) {
		require.Empty(t, s)
	}
}

// crashAfterSpendStore wraps a store for DefaultSpendAndCreateMulti and makes
// the transaction named crashAt spend its inputs and then stop before its
// create, as a process killed between the two would. Every other transaction of
// that level still runs; later levels are never reached, because crash cancels
// the context the list runs under.
type crashAfterSpendStore struct {
	utxostore.Store
	crashAt chainhash.Hash
	cancel  context.CancelFunc
}

func (c *crashAfterSpendStore) SpendAndCreate(ctx context.Context, tx *bt.Tx, h uint32, opts ...utxostore.CreateOption) (*meta.Data, []*utxostore.Spend, error) {
	if *tx.TxIDChainHash() == c.crashAt {
		_, spends, err := c.Store.SpendAndCreate(ctx, tx, h, utxostore.WithSpendOnly(), utxostore.WithIgnoreLocked(true))
		c.cancel()

		if err != nil {
			return nil, spends, err
		}

		return nil, spends, errors.NewProcessingError("injected crash between spend and create")
	}

	return c.Store.SpendAndCreate(ctx, tx, h, opts...)
}

// SpendAndCreateMultiRepeatAtCutPoints is design test 2. It interrupts a list
// before the first write and between a transaction's spend and its create at
// several levels, then repeats the way the caller really does: the pre-check
// drops every transaction whose record exists, and the rest are sent again, once
// as one list and once cut in two. The final records must match a clean run, and
// no output may be spent by two transactions.
func SpendAndCreateMultiRepeatAtCutPoints(t *testing.T, db utxostore.Store) {
	const (
		height = 600
		levels = 4
		width  = 5
	)

	ctx := context.Background()

	reference := BuildMultiWorkload(t, 0x50, levels, width)
	reference.StoreRoots(t, db, height-1)
	loopSpendAndCreate(t, db, reference.Txs, height, utxostore.WithIgnoreLocked(true))

	want := reference.Records(t, db)
	wantRoots := reference.RootSpends(t, db)

	cuts := []struct {
		name  string
		crash int // list index whose create never happens; -1 crashes before any write
		split bool
		seed  byte
	}{
		{"before the first write", -1, false, 0x60},
		{"level 0, between spend and create", 2, false, 0x61},
		{"level 2, between spend and create", 2*width + 3, false, 0x62},
		{"last level, between spend and create", (levels-1)*width + 4, true, 0x63},
		{"level 1, repeated as two lists", width + 1, true, 0x64},
	}

	for _, cut := range cuts {
		t.Run(cut.name, func(t *testing.T) {
			w := BuildMultiWorkload(t, cut.seed, levels, width)
			w.StoreRoots(t, db, height-1)

			runCtx, cancel := context.WithCancel(ctx)

			if cut.crash < 0 {
				cancel()
			}

			wrapper := &crashAfterSpendStore{Store: db, cancel: cancel}
			if cut.crash >= 0 {
				wrapper.crashAt = *w.Txs[cut.crash].TxIDChainHash()
			}

			_, _ = utxostore.DefaultSpendAndCreateMulti(runCtx, wrapper, 8, w.Txs, height, utxostore.WithIgnoreLocked(true))
			cancel()

			// The caller's pre-check: drop every transaction with a record.
			var remaining []*bt.Tx

			for _, tx := range w.Txs {
				if _, err := db.Get(ctx, tx.TxIDChainHash(), fields.Fee); errors.Is(err, errors.ErrTxNotFound) {
					remaining = append(remaining, tx)
				} else {
					require.NoError(t, err)
				}
			}

			lists := [][]*bt.Tx{remaining}
			if cut.split && len(remaining) > 1 {
				lists = [][]*bt.Tx{remaining[:len(remaining)/2], remaining[len(remaining)/2:]}
			}

			for _, list := range lists {
				results, err := db.SpendAndCreateMulti(ctx, list, height, utxostore.WithIgnoreLocked(true))
				require.NoError(t, err)

				for i, r := range results {
					require.Contains(t, []utxostore.SpendAndCreateMultiStatus{utxostore.MultiTxCreated, utxostore.MultiTxExisted}, r.Status,
						"tx %d of the repeat: %v", i, r.Err)
				}
			}

			require.Equal(t, want, w.Records(t, db))
			require.Equal(t, wantRoots, w.RootSpends(t, db))
		})
	}
}
