package utxoset

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/bsv-blockchain/teranode/util/lanereader"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// lanePrunerMetric reads one series of a lane pruner counter from the default registry, 0 when
// absent.
func lanePrunerMetric(t *testing.T, name string, labels map[string]string) float64 {
	t.Helper()

	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)

	for _, f := range families {
		if f.GetName() != name {
			continue
		}

	metrics:
		for _, m := range f.GetMetric() {
			got := map[string]string{}
			for _, l := range m.GetLabel() {
				got[l.GetName()] = l.GetValue()
			}

			for k, v := range labels {
				if got[k] != v {
					continue metrics
				}
			}

			return m.GetCounter().GetValue()
		}
	}

	return 0
}

func journalVerdicts(t *testing.T, today, lanes, reason string) float64 {
	return lanePrunerMetric(t, "teranode_lane_pruner_verdicts_total", map[string]string{
		"store": lanereader.PruneStoreUtxoset, "today": today, "lanes": lanes, "reason": reason})
}

func journalReads(t *testing.T, kind string) float64 {
	return lanePrunerMetric(t, "teranode_lane_pruner_extra_reads_total", map[string]string{"store": lanereader.PruneStoreUtxoset, "kind": kind})
}

// installJournalLaneIndex installs a lane index of a main chain of heights 1 to 300, block ID =
// height, settle depth 100, where block h takes took[h] beside one filler position in lane 250.
func installJournalLaneIndex(t *testing.T, took map[uint32][]uint64) {
	t.Helper()

	hash := func(id uint32) chainhash.Hash {
		var h chainhash.Hash
		binary.LittleEndian.PutUint32(h[:], id)
		h[31] = 4

		return h
	}

	idx := lane.NewIndex(100)

	for h := uint32(1); h <= 300; h++ {
		byLane := map[uint8]*roaring64.Bitmap{250: roaring64.BitmapOf(uint64(h))}

		for _, p := range took[h] {
			l, pos := lane.Unpack(p)
			if byLane[l] == nil {
				byLane[l] = roaring64.New()
			}

			byLane[l].Add(pos)
		}

		var rows []lane.MapRow

		for l, bm := range byLane {
			r, err := lane.BuildMapRows(l, bm)
			require.NoError(t, err)

			rows = append(rows, r...)
		}

		require.NoError(t, idx.AddBlock(lane.IndexBlock{ID: h, Height: h, Hash: hash(h), Parent: hash(h - 1)}, rows))
	}

	require.NoError(t, idx.SetTip(hash(300)))

	lanereader.InstallIndex(ulogger.TestLogger{}, idx, 100)
	t.Cleanup(func() { lanereader.Start(context.Background(), nil, test.CreateBaseTestSettings(t), nil) })
}

// spendWith creates a transaction spending output vout of parent, with opts (a lane position, a
// block), and applies its spend at height.
func spendWith(t *testing.T, s *Store, ctx context.Context, parent *bt.Tx, vout, height uint32, opts ...utxo.CreateOption) *bt.Tx {
	t.Helper()

	child := childOf(t, parent, vout)

	_, err := s.Create(ctx, child, height, opts...)
	require.NoError(t, err)

	_, err = spendOnly(ctx, s, child, height)
	require.NoError(t, err)

	return child
}

// Positions in the lane index: parents in lane 7, spenders in lane 9.
var (
	journalParentPos  = lane.Pack(7, 500) // block 150: below the floor
	journalSettled    = lane.Pack(9, 40)  // block 190: below the floor
	journalSettled2   = lane.Pack(9, 42)  // block 190: below the floor
	journalSettled3   = lane.Pack(9, 43)  // block 190: below the floor
	journalShallow    = lane.Pack(9, 41)  // block 250: 50 deep
	journalUnmined    = lane.Pack(9, 99)  // no block
	journalIndexBlock = map[uint32][]uint64{
		150: {journalParentPos},
		190: {journalSettled, journalSettled2, journalSettled3},
		250: {journalShallow},
		260: {lane.Pack(7, 1000), lane.Pack(9, 1000)},
	}
)

// newUncheckpointedLaneCopyStore is newLaneCopyStore with no checkpoints, so the copy-forward also
// carries the spends of transactions mined at test heights.
func newUncheckpointedLaneCopyStore(t *testing.T) (*Store, context.Context) {
	t.Helper()

	s, ctx := newUncheckpointedLaneStore(t)
	require.NoError(t, s.PrepareLaneSchema(ctx))

	full := reopenWithFlag(t, s, true)
	require.True(t, full.laneCoinColumn)

	return full, ctx
}

// TestJournalDropJudgesEveryRowByTheLaneRule retires a journal partition at shadow and checks each of
// its rows is judged beside today's decision: carried forward for a waiting spender is today's keep,
// for a recently mined one keep_undo, and dropped is its delete.
func TestJournalDropJudgesEveryRowByTheLaneRule(t *testing.T) {
	s, ctx := newUncheckpointedLaneCopyStore(t)
	installJournalLaneIndex(t, journalIndexBlock)

	parent := mkTx(t, 6, 5_000)
	_, err := s.Create(ctx, parent, 1_000, utxo.WithLanePosition(journalParentPos), minedAt(1_000, 1))
	require.NoError(t, err)

	// Carried: the spender waits to be mined, and no block took it. Both keep.
	spendWith(t, s, ctx, parent, 0, 1_010, utxo.WithLanePosition(journalUnmined))
	// Dropped: the spender was mined at 1,010, below the journal's keep height, and its position is
	// settled. Both delete.
	spendWith(t, s, ctx, parent, 1, 1_010, utxo.WithLanePosition(journalSettled), minedAt(1_010, 2))
	// Dropped, but the spender's block is only 50 deep. Today deletes, lanes keeps.
	spendWith(t, s, ctx, parent, 2, 1_010, utxo.WithLanePosition(journalShallow), minedAt(1_010, 3))
	// Dropped, and the spender has no position. Lanes cannot judge.
	spendWith(t, s, ctx, parent, 3, 1_010, minedAt(1_010, 4))
	// Carried, as it still waits to be mined, while the lane index says a deep block took it: the
	// direction that must never happen in a soak, counted.
	spendWith(t, s, ctx, parent, 4, 1_010, utxo.WithLanePosition(journalSettled2))
	// Carried because its spender, spent at 1,010, was mined at 1,500, at or above the journal's keep
	// height: today keeps the row as the undo copy a reorg may need, not for the parent check, so it
	// counts keep_undo, outside the direction that must stay at zero.
	minedLate := spendWith(t, s, ctx, parent, 5, 1_010, utxo.WithLanePosition(journalSettled3))
	_, err = s.SetMinedMulti(ctx, hashes(minedLate), utxo.MinedBlockInfo{BlockID: 9, BlockHeight: 1_500, OnLongestChain: true})
	require.NoError(t, err)

	// A journal partition above the cutoff, which does not retire, so the pass is no backlog.
	require.NoError(t, s.ensureSpendJournalPartition(ctx, 1_700))

	require.NoError(t, s.SetBlockHeight(1_728))

	before := map[string]float64{
		"both keep":   journalVerdicts(t, lanereader.PruneKeep, lanereader.PruneKeep, lanereader.ReasonSpenderUnmined),
		"both delete": journalVerdicts(t, lanereader.PruneDelete, lanereader.PruneDelete, lanereader.ReasonSettled),
		"today only":  journalVerdicts(t, lanereader.PruneDelete, lanereader.PruneKeep, lanereader.ReasonSpenderShallow),
		"no position": journalVerdicts(t, lanereader.PruneDelete, lanereader.PruneCannotJudge, lanereader.ReasonNoPosition),
		"lanes only":  journalVerdicts(t, lanereader.PruneKeep, lanereader.PruneDelete, lanereader.ReasonSettled),
		"undo":        journalVerdicts(t, lanereader.PruneKeepUndo, lanereader.PruneDelete, lanereader.ReasonSettled),
		"rows":        journalReads(t, "journal_row"),
		"positions":   journalReads(t, "position"),
	}

	dropped, err := s.dropSpendJournalPartitionsBelow(ctx, 1_440)
	require.NoError(t, err)
	require.Positive(t, dropped)

	require.Equal(t, before["both keep"]+1, journalVerdicts(t, lanereader.PruneKeep, lanereader.PruneKeep, lanereader.ReasonSpenderUnmined))
	require.Equal(t, before["both delete"]+1, journalVerdicts(t, lanereader.PruneDelete, lanereader.PruneDelete, lanereader.ReasonSettled))
	require.Equal(t, before["today only"]+1, journalVerdicts(t, lanereader.PruneDelete, lanereader.PruneKeep, lanereader.ReasonSpenderShallow))
	require.Equal(t, before["no position"]+1, journalVerdicts(t, lanereader.PruneDelete, lanereader.PruneCannotJudge, lanereader.ReasonNoPosition))
	require.Equal(t, before["lanes only"]+1, journalVerdicts(t, lanereader.PruneKeep, lanereader.PruneDelete, lanereader.ReasonSettled))
	require.Equal(t, before["undo"]+1, journalVerdicts(t, lanereader.PruneKeepUndo, lanereader.PruneDelete, lanereader.ReasonSettled))
	require.Equal(t, before["rows"]+6, journalReads(t, "journal_row"))
	require.Equal(t, before["positions"]+6, journalReads(t, "position"), "each spender's position; the parent's is on the journal row")

	_, judged := s.laneJudgedLeaves.Load(uint32(1_010 / SpendJournalPartitionBlocks))
	require.True(t, judged, "the partition is remembered as judged")

	// Today's decision is the one acted on: the carried rows survive, the rest are gone.
	for vout, carried := range []bool{true, false, false, false, true, true} {
		_, ok := journalLane(t, s, ctx, parent, uint32(vout)) //nolint:gosec // five outputs
		require.Equal(t, carried, ok, "output %d", vout)
	}
}

// TestJournalDropReadsTheParentsPositionWithoutTheCoinColumn: on a database without
// spend_journal.lane_position, the parent's position is read from its own records.
func TestJournalDropReadsTheParentsPositionWithoutTheCoinColumn(t *testing.T) {
	s, ctx := newLaneStore(t)
	require.False(t, s.laneCoinColumn)
	installJournalLaneIndex(t, journalIndexBlock)

	parent := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, parent, 1_000, utxo.WithLanePosition(journalParentPos), minedAt(1_000, 1))
	require.NoError(t, err)

	spendWith(t, s, ctx, parent, 0, 1_010, utxo.WithLanePosition(journalSettled), minedAt(1_010, 2))

	require.NoError(t, s.SetBlockHeight(1_728))

	settled := journalVerdicts(t, lanereader.PruneDelete, lanereader.PruneDelete, lanereader.ReasonSettled)
	positions := journalReads(t, "position")

	_, err = s.dropSpendJournalPartitionsBelow(ctx, 1_440)
	require.NoError(t, err)

	require.Equal(t, settled+1, journalVerdicts(t, lanereader.PruneDelete, lanereader.PruneDelete, lanereader.ReasonSettled))
	require.Equal(t, positions+2, journalReads(t, "position"), "the spender's and the parent's")
}

// TestJournalDropAtOffJudgesNothing: with no lane reader, the drop reads nothing more and counts
// nothing.
func TestJournalDropAtOffJudgesNothing(t *testing.T) {
	s, ctx := newTestStore(t)

	lanereader.Start(context.Background(), nil, test.CreateBaseTestSettings(t), nil)
	require.Nil(t, lanereader.Current())

	parent := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, parent, 1_000, minedAt(1_000, 1))
	require.NoError(t, err)

	spendWith(t, s, ctx, parent, 0, 1_010, minedAt(1_010, 2))

	require.NoError(t, s.SetBlockHeight(1_728))

	rows := journalReads(t, "journal_row")
	noPosition := journalVerdicts(t, lanereader.PruneDelete, lanereader.PruneCannotJudge, lanereader.ReasonNoPosition)

	dropped, err := s.dropSpendJournalPartitionsBelow(ctx, 1_440)
	require.NoError(t, err)
	require.Positive(t, dropped)

	require.Equal(t, rows, journalReads(t, "journal_row"))
	require.Equal(t, noPosition, journalVerdicts(t, lanereader.PruneDelete, lanereader.PruneCannotJudge, lanereader.ReasonNoPosition))
	require.Zero(t, judgedLeaves(s), "no partition judged at off")
}

// judgedLeaves counts the journal partitions the lane rule has judged in this store.
func judgedLeaves(s *Store) int {
	n := 0

	s.laneJudgedLeaves.Range(func(any, any) bool {
		n++

		return true
	})

	return n
}

func journalUnjudged(t *testing.T, reason string) float64 {
	return lanePrunerMetric(t, "teranode_lane_pruner_unjudged_total", map[string]string{"store": lanereader.PruneStoreUtxoset, "reason": reason})
}

// TestJournalDropJudgingIsBoundedAndNeverHoldsTheDrop: a pass whose time budget is spent judges
// nothing more, counts the partition unjudged, and still drops it, carrying the waiting spend.
func TestJournalDropJudgingIsBoundedAndNeverHoldsTheDrop(t *testing.T) {
	s, ctx := newLaneCopyStore(t)
	installJournalLaneIndex(t, journalIndexBlock)

	budget := laneJudgePassBudget
	laneJudgePassBudget = 0
	t.Cleanup(func() { laneJudgePassBudget = budget })

	parent := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, parent, 1_000, utxo.WithLanePosition(journalParentPos), minedAt(1_000, 1))
	require.NoError(t, err)

	spendWith(t, s, ctx, parent, 0, 1_010, utxo.WithLanePosition(journalUnmined))
	spendWith(t, s, ctx, parent, 1, 1_010, utxo.WithLanePosition(journalSettled), minedAt(1_010, 2))

	require.NoError(t, s.SetBlockHeight(1_728))

	unjudged := journalUnjudged(t, "budget")
	rows := journalReads(t, "journal_row")

	dropped, err := s.dropSpendJournalPartitionsBelow(ctx, 1_440)
	require.NoError(t, err)
	require.Positive(t, dropped)

	require.Equal(t, unjudged+1, journalUnjudged(t, "budget"))
	require.Equal(t, rows, journalReads(t, "journal_row"), "no row read")
	require.Zero(t, judgedLeaves(s), "a partition the budget skipped is not remembered as judged")

	_, carried := journalLane(t, s, ctx, parent, 0)
	require.True(t, carried, "the waiting spend is carried")

	_, kept := journalLane(t, s, ctx, parent, 1)
	require.False(t, kept, "the mined spend dropped with its partition")
}

// TestJournalDropJudgesNoBacklog: a pass retiring more than one journal partition, a backlog, judges
// none of them, so catching up is never slowed.
func TestJournalDropJudgesNoBacklog(t *testing.T) {
	s, ctx := newLaneCopyStore(t)
	installJournalLaneIndex(t, journalIndexBlock)

	parent := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, parent, 700, utxo.WithLanePosition(journalParentPos), minedAt(700, 1))
	require.NoError(t, err)

	// Spends at 710 and 1,010 land in two journal partitions, both below the cutoff at 1,440.
	spendWith(t, s, ctx, parent, 0, 710, utxo.WithLanePosition(journalSettled), minedAt(710, 2))
	spendWith(t, s, ctx, parent, 1, 1_010, utxo.WithLanePosition(journalSettled2), minedAt(1_010, 3))

	require.NoError(t, s.SetBlockHeight(1_728))

	backlog := journalUnjudged(t, "backlog")
	rows := journalReads(t, "journal_row")

	_, err = s.dropSpendJournalPartitionsBelow(ctx, 1_440)
	require.NoError(t, err)

	require.Equal(t, backlog+2, journalUnjudged(t, "backlog"))
	require.Equal(t, rows, journalReads(t, "journal_row"))
	require.Zero(t, judgedLeaves(s), "a backlog partition is not remembered as judged")
}

// TestJournalLeafIsJudgedOnce: a partition already judged in this process is not judged again by a
// later pass, as when its drop failed and is retried.
func TestJournalLeafIsJudgedOnce(t *testing.T) {
	s, ctx := newLaneCopyStore(t)
	installJournalLaneIndex(t, journalIndexBlock)

	s.laneJudgedLeaves.Store(uint32(1_010/SpendJournalPartitionBlocks), struct{}{})

	parent := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, parent, 1_000, utxo.WithLanePosition(journalParentPos), minedAt(1_000, 1))
	require.NoError(t, err)

	spendWith(t, s, ctx, parent, 0, 1_010, utxo.WithLanePosition(journalSettled), minedAt(1_010, 2))

	require.NoError(t, s.SetBlockHeight(1_728))

	rows := journalReads(t, "journal_row")

	_, err = s.dropSpendJournalPartitionsBelow(ctx, 1_440)
	require.NoError(t, err)

	require.Equal(t, rows, journalReads(t, "journal_row"))
}

// TestJournalDropCountsAFailedJudgeAndStillDrops: a read the judging needs fails, the partition counts
// leaf_error, and the drop goes on, carrying the waiting spend.
func TestJournalDropCountsAFailedJudgeAndStillDrops(t *testing.T) {
	s, ctx := newLaneCopyStore(t)
	installJournalLaneIndex(t, journalIndexBlock)

	query := lanePositionsQuery
	lanePositionsQuery = "SELECT no_such_column FROM tx_ident WHERE $1::smallint[] IS NULL AND $2::bytea[] IS NULL"
	t.Cleanup(func() { lanePositionsQuery = query })

	parent := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, parent, 1_000, utxo.WithLanePosition(journalParentPos), minedAt(1_000, 1))
	require.NoError(t, err)

	spendWith(t, s, ctx, parent, 0, 1_010, utxo.WithLanePosition(journalUnmined))
	spendWith(t, s, ctx, parent, 1, 1_010, utxo.WithLanePosition(journalSettled), minedAt(1_010, 2))

	require.NoError(t, s.SetBlockHeight(1_728))

	leafErrors := journalUnjudged(t, "leaf_error")

	dropped, err := s.dropSpendJournalPartitionsBelow(ctx, 1_440)
	require.NoError(t, err)
	require.Positive(t, dropped)

	require.Equal(t, leafErrors+1, journalUnjudged(t, "leaf_error"))
	require.Equal(t, 1, judgedLeaves(s), "a partition whose judging failed is not judged again")

	_, carried := journalLane(t, s, ctx, parent, 0)
	require.True(t, carried, "the waiting spend is carried")

	_, kept := journalLane(t, s, ctx, parent, 1)
	require.False(t, kept, "the mined spend dropped with its partition")
}

// TestJournalDropBudgetCutsAJudgingShort: the pass's budget ends a partition's judging part way, the
// partition counts leaf_error, and the drop goes on.
func TestJournalDropBudgetCutsAJudgingShort(t *testing.T) {
	s, ctx := newLaneCopyStore(t)
	installJournalLaneIndex(t, journalIndexBlock)

	budget, query := laneJudgePassBudget, lanePositionsQuery
	laneJudgePassBudget = 300 * time.Millisecond
	// A position read that takes two seconds, longer than the budget.
	lanePositionsQuery = "SELECT k.txid, NULL::bigint FROM unnest($1::smallint[], $2::bytea[]) AS k(leaf, txid), pg_sleep(2)"

	t.Cleanup(func() { laneJudgePassBudget, lanePositionsQuery = budget, query })

	parent := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, parent, 1_000, utxo.WithLanePosition(journalParentPos), minedAt(1_000, 1))
	require.NoError(t, err)

	spendWith(t, s, ctx, parent, 0, 1_010, utxo.WithLanePosition(journalSettled), minedAt(1_010, 2))

	require.NoError(t, s.SetBlockHeight(1_728))

	leafErrors := journalUnjudged(t, "leaf_error")

	dropped, err := s.dropSpendJournalPartitionsBelow(ctx, 1_440)
	require.NoError(t, err)
	require.Positive(t, dropped)
	require.Equal(t, leafErrors+1, journalUnjudged(t, "leaf_error"))
}

// TestJournalDropReadsAPreservedParentsPosition: a parent known only by its preserved copy, whose
// journal row carries no position, gives its position from that copy.
func TestJournalDropReadsAPreservedParentsPosition(t *testing.T) {
	s, ctx := newUncheckpointedLaneCopyStore(t)
	installJournalLaneIndex(t, journalIndexBlock)

	parent := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, parent, 1_000, utxo.WithLanePosition(journalParentPos), minedAt(1_000, 1))
	require.NoError(t, err)

	spendWith(t, s, ctx, parent, 0, 1_010, utxo.WithLanePosition(journalSettled), minedAt(1_010, 2))

	// The preserved copy, planted directly: setup through PreserveTransactions would depend on its
	// own conditions for copying.
	_, err = s.pool.Exec(ctx, `
        INSERT INTO preserved_parent (txid, mined_height, block_id, subtree_idx, created_height, preserve_until, lane_position)
        VALUES ($1, 1000, 1, 0, 1000, 5000, $2)`, hashBytes(parent), lane.ToSigned(journalParentPos))
	require.NoError(t, err)

	dropIdentityRow(t, s, ctx, parent)
	_, err = s.pool.Exec(ctx, "DELETE FROM tx_mined WHERE txid = $1", hashBytes(parent))
	require.NoError(t, err)
	_, err = s.pool.Exec(ctx, "UPDATE spend_journal SET lane_position = NULL WHERE txid = $1", hashBytes(parent))
	require.NoError(t, err)

	require.NoError(t, s.SetBlockHeight(1_728))

	settled := journalVerdicts(t, lanereader.PruneDelete, lanereader.PruneDelete, lanereader.ReasonSettled)

	_, err = s.dropSpendJournalPartitionsBelow(ctx, 1_440)
	require.NoError(t, err)

	require.Equal(t, settled+1, journalVerdicts(t, lanereader.PruneDelete, lanereader.PruneDelete, lanereader.ReasonSettled))
}
