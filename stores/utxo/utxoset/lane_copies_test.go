package utxoset

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// The copies of lane positions that teranode-cli prepare-lane-schema's columns allow: each output
// row carries its creating transaction's position, a spend carries it into the journal row and
// Unspend puts it back, and a child carries its parents' positions beside its inpoints.

// newLaneCopyStore is a store at shadow on a database with every lane column, as after
// prepare-lane-schema and a restart.
func newLaneCopyStore(t *testing.T) (*Store, context.Context) {
	t.Helper()

	s, ctx := newLaneStore(t)
	require.NoError(t, s.PrepareLaneSchema(ctx))

	full := reopenWithFlag(t, s, true)
	require.True(t, full.laneParentColumn)
	require.True(t, full.laneCoinColumn)

	return full, ctx
}

// childOf builds a transaction spending vout of parent.
func childOf(t *testing.T, parent *bt.Tx, vout uint32) *bt.Tx {
	t.Helper()

	child := bt.NewTx()
	require.NoError(t, child.FromUTXOs(&bt.UTXO{
		TxIDHash:      parent.TxIDChainHash(),
		Vout:          vout,
		LockingScript: parent.Outputs[vout].LockingScript,
		Satoshis:      parent.Outputs[vout].Satoshis,
	}))
	child.AddOutput(&bt.Output{
		Satoshis:      parent.Outputs[vout].Satoshis - 1_000,
		LockingScript: parent.Outputs[vout].LockingScript,
	})

	return child
}

// nullableLane reads one nullable BIGINT lane column; ok is false when no row matched.
func nullableLane(t *testing.T, s *Store, ctx context.Context, query string, args ...any) (value *int64, ok bool) {
	t.Helper()

	rows, err := s.pool.Query(ctx, query, args...)
	require.NoError(t, err)

	defer rows.Close()

	if !rows.Next() {
		require.NoError(t, rows.Err())
		return nil, false
	}

	require.NoError(t, rows.Scan(&value))

	return value, true
}

// coinLane is the lane_position of output vout of tx in the UTXO table.
func coinLane(t *testing.T, s *Store, ctx context.Context, tx *bt.Tx, vout uint32) (*int64, bool) {
	t.Helper()

	k := Pack(tx.TxIDChainHash()[:], vout)

	return nullableLane(t, s, ctx, `SELECT lane_position FROM utxo WHERE ukey = $1 AND txid = $2`, k, hashBytes(tx))
}

// journalLane is the lane_position of the journal row for output vout of tx.
func journalLane(t *testing.T, s *Store, ctx context.Context, tx *bt.Tx, vout uint32) (*int64, bool) {
	t.Helper()

	k := Pack(tx.TxIDChainHash()[:], vout)

	return nullableLane(t, s, ctx, `SELECT lane_position FROM spend_journal WHERE ukey = $1 AND txid = $2`, k, hashBytes(tx))
}

// parentLanes reads parent_lane_positions of tx from table; ok is false when no row matched.
func parentLanes(t *testing.T, s *Store, ctx context.Context, table string, tx *bt.Tx) (value []byte, ok bool) {
	t.Helper()

	rows, err := s.pool.Query(ctx, `SELECT parent_lane_positions FROM `+table+` WHERE txid = $1`, hashBytes(tx))
	require.NoError(t, err)

	defer rows.Close()

	if !rows.Next() {
		require.NoError(t, rows.Err())
		return nil, false
	}

	require.NoError(t, rows.Scan(&value))

	return value, true
}

func requireLaneValue(t *testing.T, want uint64, got *int64, ok bool, msg string) {
	t.Helper()

	require.True(t, ok, "%s: row exists", msg)
	require.NotNil(t, got, "%s: carries a position", msg)
	require.Equal(t, lane.ToSigned(want), *got, msg)
}

// A spend carries the output's lane position, its creating transaction's, into the journal row, and
// Unspend puts it back on the restored output. The child's record keeps its parents' positions
// beside its inpoints, and its own outputs take its position.
func TestLanePositionRidesTheSpendIntoTheJournalAndBackOnUnspend(t *testing.T) {
	s, ctx := newLaneCopyStore(t)

	// Lane 200 is stored as a negative BIGINT, so the copies must keep every bit.
	parentPosition := lane.Pack(200, 41_000)
	childPosition := lane.Pack(9, 7_300)

	parent := mkTx(t, 2, 5_000)
	_, _, err := s.SpendAndCreate(ctx, parent, 100, utxo.WithCreateOnly(), utxo.WithLanePosition(parentPosition))
	require.NoError(t, err)

	for vout := uint32(0); vout < 2; vout++ {
		v, ok := coinLane(t, s, ctx, parent, vout)
		requireLaneValue(t, parentPosition, v, ok, "the parent's output takes the parent's position")
	}

	child := childOf(t, parent, 0)
	_, spends, err := s.SpendAndCreate(ctx, child, 101,
		utxo.WithLanePosition(childPosition), utxo.WithParentLanePositions([]uint64{parentPosition}))
	require.NoError(t, err)
	require.Len(t, spends, 1)

	_, ok := coinLane(t, s, ctx, parent, 0)
	require.False(t, ok, "the spend deleted the output row")

	v, ok := journalLane(t, s, ctx, parent, 0)
	requireLaneValue(t, parentPosition, v, ok, "the journal row keeps the spent output's position")

	v, ok = coinLane(t, s, ctx, child, 0)
	requireLaneValue(t, childPosition, v, ok, "the child's output takes the child's position")

	got, ok := parentLanes(t, s, ctx, "tx_ident", child)
	require.True(t, ok)
	require.Equal(t, lane.AppendPositions(nil, []uint64{parentPosition}), got, "the child keeps its parent's position")

	require.NoError(t, s.Unspend(ctx, spends))

	_, ok = journalLane(t, s, ctx, parent, 0)
	require.False(t, ok, "Unspend consumes the journal row")

	v, ok = coinLane(t, s, ctx, parent, 0)
	requireLaneValue(t, parentPosition, v, ok, "Unspend restores the output with its position")
}

// The validator reads each parent's own lane position from the parent read it already makes, at
// shadow only.
func TestParentOutputsCarryTheParentsLanePosition(t *testing.T) {
	s, ctx := newLaneCopyStore(t)

	want := lane.Pack(3, 12)

	parent := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, parent, 100, utxo.WithLanePosition(want))
	require.NoError(t, err)

	op := []utxo.Outpoint{{TxID: *parent.TxIDChainHash(), Vout: 0}}

	// A coinbase parent says so beside its position, for the lanes shadow to count it apart.
	coinbase := mkTx(t, 1, 6_000)
	_, err = s.Create(ctx, coinbase, 100, utxo.WithSetCoinbase(true), utxo.WithLanePosition(lane.Pack(3, 13)))
	require.NoError(t, err)

	op = append(op, utxo.Outpoint{TxID: *coinbase.TxIDChainHash(), Vout: 0})

	answers, err := s.ParentOutputsForValidation(ctx, op)
	require.NoError(t, err)
	require.Equal(t, utxo.ParentOutputNotMined, answers[0].Status)
	require.Equal(t, want, answers[0].LanePosition)
	require.False(t, answers[0].IsCoinbase)
	require.True(t, answers[1].IsCoinbase)

	// The parent is spent, so the coin comes from the journal; the position still comes with the
	// parent's record.
	_ = spendOneOutput(t, s, ctx, parent, 0, 101)

	answers, err = s.ParentOutputsForValidation(ctx, op)
	require.NoError(t, err)
	require.Equal(t, want, answers[0].LanePosition, "a spent output's parent keeps its position")

	off := reopenWithFlag(t, s, false)

	answers, err = off.ParentOutputsForValidation(ctx, op)
	require.NoError(t, err)
	require.Equal(t, utxo.ParentOutputNotMined, answers[0].Status)
	require.Zero(t, answers[0].LanePosition, "lane positions off: the read does not ask")
	require.False(t, answers[1].IsCoinbase, "lane positions off: nothing reads the flag for lanes")
}

// The parents' positions travel where the inpoints do: into the containment row when the record is
// mined, and into the preserved copy.
func TestParentLanePositionsFollowTheInpoints(t *testing.T) {
	s, ctx := newLaneCopyStore(t)

	parentPosition := lane.Pack(4, 1)
	parent := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, parent, 100, utxo.WithLanePosition(parentPosition))
	require.NoError(t, err)

	child := childOf(t, parent, 0)
	_, _, err = s.SpendAndCreate(ctx, child, 101,
		utxo.WithLanePosition(lane.Pack(4, 2)), utxo.WithParentLanePositions([]uint64{parentPosition}))
	require.NoError(t, err)

	want := lane.AppendPositions(nil, []uint64{parentPosition})

	_, err = s.SetMinedMulti(ctx, hashes(child), utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 101, OnLongestChain: true})
	require.NoError(t, err)

	got, ok := parentLanes(t, s, ctx, "tx_mined", child)
	require.True(t, ok)
	require.Equal(t, want, got, "the containment row copies them from the identity row")

	// The preserved copy takes the full payload only from a stamped containment row.
	stampThrough(t, s, ctx, 0, map[uint32]uint32{101: 7})
	require.False(t, identExists(t, s, ctx, child), "the stamp deletes the identity row")

	require.NoError(t, s.PreserveTransactions(ctx, []chainhash.Hash{*child.TxIDChainHash()}, 5_000))

	got, ok = parentLanes(t, s, ctx, "preserved_parent", child)
	require.True(t, ok)
	require.Equal(t, want, got, "the preserved copy carries them")
}

// A list that does not match the transaction's parents, or that holds an invalid position, is not
// written at all.
func TestParentLanePositionsThatDoNotMatchAreNotWritten(t *testing.T) {
	s, ctx := newLaneCopyStore(t)

	parent := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, parent, 100, utxo.WithLanePosition(lane.Pack(5, 1)))
	require.NoError(t, err)

	for name, positions := range map[string][]uint64{
		"one too many": {lane.Pack(5, 1), lane.Pack(5, 2)},
		"invalid":      {lane.Pack(5, 0)},
	} {
		t.Run(name, func(t *testing.T) {
			child := childOf(t, parent, 0)
			_, err := s.Create(ctx, child, 101, utxo.WithLanePosition(lane.Pack(5, 9)), utxo.WithParentLanePositions(positions))
			require.NoError(t, err, "a parent list that does not fit is a miss, not an error")

			got, ok := parentLanes(t, s, ctx, "tx_ident", child)
			require.True(t, ok)
			require.Nil(t, got)

			own, null, ok := storedLane(t, s, ctx, "tx_ident", child)
			require.True(t, ok)
			require.False(t, null, "the record keeps its own position")
			require.Equal(t, lane.ToSigned(lane.Pack(5, 9)), own)

			require.NoError(t, s.Delete(ctx, child.TxIDChainHash()))
		})
	}
}

// With lane positions off nothing new is written, even on a database with every lane column: the
// daemon names no position, so the columns stay NULL.
func TestLaneCopiesWriteNothingNewWithLanePositionsOff(t *testing.T) {
	s, ctx := newLaneCopyStore(t)
	off := reopenWithFlag(t, s, false)
	require.True(t, off.laneParentColumn, "the columns outlive the flag")
	require.False(t, off.laneShadow)

	parent := mkTx(t, 1, 5_000)
	_, err := off.Create(ctx, parent, 100)
	require.NoError(t, err)

	child := childOf(t, parent, 0)
	_, spends, err := off.SpendAndCreate(ctx, child, 101)
	require.NoError(t, err)

	v, ok := journalLane(t, off, ctx, parent, 0)
	require.True(t, ok)
	require.Nil(t, v, "the journal row has no position")

	v, ok = coinLane(t, off, ctx, child, 0)
	require.True(t, ok)
	require.Nil(t, v, "the output has no position")

	got, ok := parentLanes(t, off, ctx, "tx_ident", child)
	require.True(t, ok)
	require.Nil(t, got, "the child has no parent positions")

	require.NoError(t, off.Unspend(ctx, spends))

	v, ok = coinLane(t, off, ctx, parent, 0)
	require.True(t, ok)
	require.Nil(t, v, "the restored output has no position")
}

// Without prepare-lane-schema's columns a store at shadow still writes what it can: the records'
// own positions, which opening at shadow adds a column for. The spends are counted as carrying none.
func TestLanePositionsOnWithoutTheLaneSchemaCountTheMissingCopies(t *testing.T) {
	s, ctx := newLaneStore(t)
	require.True(t, s.laneColumn)
	require.False(t, s.laneParentColumn)
	require.False(t, s.laneCoinColumn)

	parent := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, parent, 100, utxo.WithLanePosition(lane.Pack(6, 1)))
	require.NoError(t, err)

	before := readLaneSpendCoverage(t)

	child := childOf(t, parent, 0)
	_, _, err = s.SpendAndCreate(ctx, child, 101,
		utxo.WithLanePosition(lane.Pack(6, 2)), utxo.WithParentLanePositions([]uint64{lane.Pack(6, 1)}))
	require.NoError(t, err)

	after := readLaneSpendCoverage(t)
	require.Equal(t, before["no"]+1, after["no"], "the spend is counted without a position")
	require.Equal(t, before["yes"], after["yes"])
	requireGetLane(t, s, ctx, child, lane.Pack(6, 2), "the record's own position is still written")
}

// On utxoset the spend counter's carried label says whether the spent output's own position, its
// creating transaction's, was copied into the journal row, not whether the spender has a position.
// An output created before shadow has none, so its spend counts no while the spender, created at
// shadow, keeps its own position, which P4 reaches through the journal row's spending_txid. This is
// what mainnet showed: every spend counted no had spent an output created before the soak started.
func TestLaneSpendOfAnOutputCreatedBeforeShadowCountsNo(t *testing.T) {
	s, ctx := newLaneCopyStore(t)

	parent := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, parent, 100) // created before shadow: no position
	require.NoError(t, err)

	before := readLaneSpendCoverage(t)

	child := childOf(t, parent, 0)
	_, _, err = s.SpendAndCreate(ctx, child, 101, utxo.WithLanePosition(lane.Pack(8, 2)))
	require.NoError(t, err)

	after := readLaneSpendCoverage(t)
	require.Equal(t, before["no"]+1, after["no"], "a spend of an output with no position counts no")
	require.Equal(t, before["yes"], after["yes"])

	v, ok := journalLane(t, s, ctx, parent, 0)
	require.True(t, ok, "the spend wrote a journal row")
	require.Nil(t, v, "the journal row copies the output's missing position")

	requireGetLane(t, s, ctx, child, lane.Pack(8, 2), "the spender has its own position all the same")
}

// The coverage counters see every copy the store writes at shadow.
func TestLaneCoverageCountsCarriedPositions(t *testing.T) {
	s, ctx := newLaneCopyStore(t)

	parent := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, parent, 100, utxo.WithLanePosition(lane.Pack(7, 1)))
	require.NoError(t, err)

	spendsBefore := readLaneSpendCoverage(t)
	recordsBefore := readLaneRecordCoverage(t)

	child := childOf(t, parent, 0)
	_, _, err = s.SpendAndCreate(ctx, child, 101,
		utxo.WithLanePosition(lane.Pack(7, 2)), utxo.WithParentLanePositions([]uint64{lane.Pack(7, 1)}))
	require.NoError(t, err)

	spendsAfter := readLaneSpendCoverage(t)
	recordsAfter := readLaneRecordCoverage(t)

	require.Equal(t, spendsBefore["yes"]+1, spendsAfter["yes"])
	require.Equal(t, recordsBefore["own/yes"]+1, recordsAfter["own/yes"])
	require.Equal(t, recordsBefore["parents/yes"]+1, recordsAfter["parents/yes"])

	// A child whose parent read gave no position is counted without parents.
	other := childOf(t, child, 0)
	_, _, err = s.SpendAndCreate(ctx, other, 102, utxo.WithLanePosition(lane.Pack(7, 3)))
	require.NoError(t, err)
	require.Equal(t, recordsAfter["parents/no"]+1, readLaneRecordCoverage(t)["parents/no"])
}

// A list that creates and spends an output inside itself writes that spend straight to the journal,
// and the journal row still takes the output's position, its creator's.
func TestNettedJournalRowsTakeTheOutputsLanePosition(t *testing.T) {
	s, ctx := newLaneCopyStore(t)

	w := tests.BuildMultiWorkload(t, 0xe1, 3, 3)
	w.StoreRoots(t, s, 910)

	positions := make([]uint64, 4*len(w.Txs))
	for i := range positions {
		positions[i] = lane.Pack(11, uint64(i)+1) //nolint:gosec // i counts up from 0
	}

	s.SetLanePositionSource(tests.NewScriptedLanePositions(positions...))
	t.Cleanup(func() { s.SetLanePositionSource(nil) })

	results, err := s.SpendAndCreateMulti(ctx, w.Txs, 911, utxo.WithIgnoreLocked(true))
	require.NoError(t, err)
	require.Len(t, results, len(w.Txs))

	inList := make(map[chainhash.Hash]*bt.Tx, len(w.Txs))
	for _, tx := range w.Txs {
		inList[*tx.TxIDChainHash()] = tx
	}

	checked := 0

	for _, tx := range w.Txs {
		for _, in := range tx.Inputs {
			parent, ok := inList[*in.PreviousTxIDChainHash()]
			if !ok {
				continue
			}

			md, err := s.Get(ctx, parent.TxIDChainHash(), fields.LanePosition)
			require.NoError(t, err)
			require.True(t, lane.Valid(md.LanePosition))

			v, ok := journalLane(t, s, ctx, parent, in.PreviousTxOutIndex)
			requireLaneValue(t, md.LanePosition, v, ok, "an in-list spend's journal row")

			checked++
		}
	}

	require.Positive(t, checked, "the workload spends inside the list")
}

// readLaneSpendCoverage reads teranode_lane_spend_positions_total for utxoset, by carried.
func readLaneSpendCoverage(t *testing.T) map[string]float64 {
	t.Helper()

	return readLaneCounter(t, "teranode_lane_spend_positions_total", func(labels map[string]string) string {
		return labels["carried"]
	})
}

// readLaneRecordCoverage reads teranode_lane_record_positions_total for utxoset, by kind/carried.
func readLaneRecordCoverage(t *testing.T) map[string]float64 {
	t.Helper()

	return readLaneCounter(t, "teranode_lane_record_positions_total", func(labels map[string]string) string {
		return labels["kind"] + "/" + labels["carried"]
	})
}

func readLaneCounter(t *testing.T, name string, key func(map[string]string) string) map[string]float64 {
	t.Helper()

	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)

	out := map[string]float64{}

	for _, family := range families {
		if family.GetName() != name {
			continue
		}

		for _, m := range family.GetMetric() {
			labels := map[string]string{}
			for _, l := range m.GetLabel() {
				labels[l.GetName()] = l.GetValue()
			}

			if labels["store"] != laneStoreName {
				continue
			}

			out[key(labels)] += m.GetCounter().GetValue()
		}
	}

	return out
}

// The journal's copy-forward, which keeps a waiting transaction's spends past their partition's
// drop, carries the output's position too, so Unspend still restores it.
func TestJournalCopyForwardCarriesTheLanePosition(t *testing.T) {
	s, ctx := newLaneCopyStore(t)

	want := lane.Pack(13, 5)

	parent := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, parent, 1_000, utxo.WithLanePosition(want), utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 1_000, OnLongestChain: true}))
	require.NoError(t, err)

	waiting := spendOneOutput(t, s, ctx, parent, 0, 1_010)

	require.NoError(t, s.SetBlockHeight(1_728))

	dropped, err := s.dropSpendJournalPartitionsBelow(ctx, 1_440)
	require.NoError(t, err)
	require.Positive(t, dropped)

	v, ok := journalLane(t, s, ctx, parent, 0)
	requireLaneValue(t, want, v, ok, "the carried journal row")

	spends, err := utxo.GetSpends(waiting)
	require.NoError(t, err)
	require.NoError(t, s.Unspend(ctx, spends))

	v, ok = coinLane(t, s, ctx, parent, 0)
	requireLaneValue(t, want, v, ok, "the coin rebuilt from the carried row")
}

// Below the checkpoint the netted write writes each surviving coin with its creating transaction's
// position.
func TestNettedBelowCoinsTakeTheirTransactionsLanePosition(t *testing.T) {
	s, ctx := newLaneCopyStore(t)

	const height = 800

	w := tests.BuildMultiWorkload(t, 0xe2, 3, 3)
	w.StoreRoots(t, s, height-1)

	positions := make([]uint64, 2*len(w.Txs))
	for i := range positions {
		positions[i] = lane.Pack(14, uint64(i)+1) //nolint:gosec // i counts up from 0
	}

	s.SetLanePositionSource(tests.NewScriptedLanePositions(positions...))
	t.Cleanup(func() { s.SetLanePositionSource(nil) })

	list := outpointOnly(w.Txs)

	results, err := s.SpendAndCreateMulti(ctx, list, height, belowCheckpointOptions(height)...)
	require.NoError(t, err)

	for i, r := range results {
		require.Equal(t, utxo.MultiTxCreated, r.Status, "tx %d: %v", i, r.Err)
	}

	checked := 0

	for _, tx := range list {
		md, err := s.Get(ctx, tx.TxIDChainHash(), fields.LanePosition)
		require.NoError(t, err)
		require.True(t, lane.Valid(md.LanePosition))

		for vout := range tx.Outputs {
			v, ok := coinLane(t, s, ctx, tx, uint32(vout)) //nolint:gosec // a handful of outputs
			if !ok {
				continue // spent inside the list, so never written
			}

			requireLaneValue(t, md.LanePosition, v, ok, "a surviving coin")

			checked++
		}
	}

	require.Positive(t, checked, "the list leaves coins")
}

// A parent whose only records are its coin rows, or once spent its journal rows, still answers the
// validator's parent read with its position: the identity row is gone and its containment row is
// below the lookup floor, as for every parent mined more than about 576 blocks before.
func TestParentOutputsCarryThePositionFromCoinAndJournalRows(t *testing.T) {
	s, ctx := newLaneCopyStore(t)

	want := lane.Pack(15, 3)

	// Below mainnet's checkpoints, so the create takes the block path: coins and a containment
	// row at height 100, and no identity row.
	parent := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, parent, 100, utxo.WithLanePosition(want), utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 3, BlockHeight: 100, OnLongestChain: true}))
	require.NoError(t, err)
	require.False(t, identExists(t, s, ctx, parent))

	require.NoError(t, s.SetBlockHeight(5_000))

	op := []utxo.Outpoint{{TxID: *parent.TxIDChainHash(), Vout: 0}}

	answers, err := s.ParentOutputsForValidation(ctx, op)
	require.NoError(t, err)
	require.Equal(t, utxo.ParentOutputMined, answers[0].Status)
	require.Equal(t, want, answers[0].LanePosition, "answered from the coin row")

	_ = spendOneOutput(t, s, ctx, parent, 0, 5_001)

	answers, err = s.ParentOutputsForValidation(ctx, op)
	require.NoError(t, err)
	require.Equal(t, utxo.ParentOutputMined, answers[0].Status)
	require.Equal(t, want, answers[0].LanePosition, "answered from the journal row")
}

// A preserved copy taken from a coin row, for a parent with no containment row left, carries the
// coin's position.
func TestPreservedCopyFromACoinRowCarriesItsPosition(t *testing.T) {
	s, ctx := newLaneCopyStore(t)

	want := lane.Pack(16, 4)

	parent := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, parent, 100, utxo.WithLanePosition(want), utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 3, BlockHeight: 100, OnLongestChain: true}))
	require.NoError(t, err)

	_, err = s.pool.Exec(ctx, `DELETE FROM tx_mined WHERE txid = $1`, hashBytes(parent))
	require.NoError(t, err)

	require.NoError(t, s.PreserveTransactions(ctx, []chainhash.Hash{*parent.TxIDChainHash()}, 5_000))

	v, null, ok := storedLane(t, s, ctx, "preserved_parent", parent)
	require.True(t, ok, "the coin row gives a preserved copy")
	require.False(t, null)
	require.Equal(t, lane.ToSigned(want), v)
}
