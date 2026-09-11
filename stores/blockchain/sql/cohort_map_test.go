package sql

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/cohort"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// newCohortMapTestStore spins up a fresh in-memory SQL store for cohort-map
// tests and waits for the startup on_main_chain rebuild to finish, so that the
// flags the tests read are the settled ones.
func newCohortMapTestStore(t *testing.T) *SQL {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)

	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(context.Background()) })

	waitForStartupRebuild(t, s)

	return s
}

// storeCohortTestBlock stores one block and returns its blocks-table id.
func storeCohortTestBlock(t *testing.T, s *SQL, block *model.Block) uint32 {
	t.Helper()

	id, _, err := s.StoreBlock(context.Background(), block, "peer")
	require.NoError(t, err)
	require.NotZero(t, id)

	return uint32(id)
}

// rawCohortMapRow is a cohort_map row read straight out of the table, used to
// prove the table is untouched across an operation.
type rawCohortMapRow struct {
	Cohort      uint64
	BlockID     uint64
	MemberCount uint64
	Verified    bool
}

// dumpCohortMap reads every cohort_map row in a stable order.
func dumpCohortMap(t *testing.T, s *SQL) []rawCohortMapRow {
	t.Helper()

	rows, err := s.db.Query(`SELECT cohort, block_id, member_count, verified FROM cohort_map ORDER BY cohort ASC, block_id ASC`)
	require.NoError(t, err)

	defer rows.Close()

	var out []rawCohortMapRow

	for rows.Next() {
		var r rawCohortMapRow
		require.NoError(t, rows.Scan(&r.Cohort, &r.BlockID, &r.MemberCount, &r.Verified))

		out = append(out, r)
	}

	require.NoError(t, rows.Err())

	return out
}

// TestCohortMap_InsertIsInsertOnly proves the property the whole design leans
// on for crash safety: re-recording a row that is already there neither errors
// nor changes what is stored, so replaying a block's map writes is a no-op.
func TestCohortMap_InsertIsInsertOnly(t *testing.T) {
	s := newCohortMapTestStore(t)
	ctx := context.Background()

	blockID := storeCohortTestBlock(t, s, block1)

	c := cohort.GenesisTime + 1000

	require.NoError(t, s.RecordCohortMap(ctx, []model.CohortMapRow{{
		Cohort:      c,
		BlockID:     blockID,
		MemberCount: 42,
		Verified:    true,
	}}))

	before := dumpCohortMap(t, s)
	require.Len(t, before, 1)

	// Re-record the same (cohort, block) pair with different payload. The row
	// must survive exactly as first written, and the call must not error.
	require.NoError(t, s.RecordCohortMap(ctx, []model.CohortMapRow{{
		Cohort:      c,
		BlockID:     blockID,
		MemberCount: 999,
		Verified:    false,
	}}))

	require.Equal(t, before, dumpCohortMap(t, s), "a repeated insert must leave the row untouched")

	blocks, err := s.CohortBlocks(ctx, []cohort.ID{c})
	require.NoError(t, err)
	require.Len(t, blocks[c], 1)
	require.Equal(t, uint64(42), blocks[c][0].MemberCount)
	require.True(t, blocks[c][0].Verified)
	require.Equal(t, blockID, blocks[c][0].BlockID)
	require.Equal(t, block1.Hash().String(), blocks[c][0].Hash.String())
}

// TestCohortMap_CohortMappedToTwoBlocks checks that a cohort straddling two
// blocks reports both of them.
func TestCohortMap_CohortMappedToTwoBlocks(t *testing.T) {
	s := newCohortMapTestStore(t)
	ctx := context.Background()

	id1 := storeCohortTestBlock(t, s, block1)
	id2 := storeCohortTestBlock(t, s, block2)

	c := cohort.GenesisTime + 2000

	require.NoError(t, s.RecordCohortMap(ctx, []model.CohortMapRow{
		{Cohort: c, BlockID: id1, MemberCount: 3},
		{Cohort: c, BlockID: id2, MemberCount: 7, Verified: true},
	}))

	blocks, err := s.CohortBlocks(ctx, []cohort.ID{c})
	require.NoError(t, err)
	require.Len(t, blocks[c], 2)

	// Ordered by height ascending.
	require.Equal(t, id1, blocks[c][0].BlockID)
	require.Equal(t, uint64(3), blocks[c][0].MemberCount)
	require.False(t, blocks[c][0].Verified)

	require.Equal(t, id2, blocks[c][1].BlockID)
	require.Equal(t, uint64(7), blocks[c][1].MemberCount)
	require.True(t, blocks[c][1].Verified)
}

// TestCohortMap_UnmappedCohortIsNotAnError checks that asking about a cohort
// nothing has mapped comes back empty rather than failing. That answer means
// "not mined", and the design needs it to be the cheap, ordinary case.
func TestCohortMap_UnmappedCohortIsNotAnError(t *testing.T) {
	s := newCohortMapTestStore(t)
	ctx := context.Background()

	blocks, err := s.CohortBlocks(ctx, []cohort.ID{cohort.GenesisTime + 3000})
	require.NoError(t, err)
	require.Empty(t, blocks)

	// And the empty request is fine too.
	blocks, err = s.CohortBlocks(ctx, nil)
	require.NoError(t, err)
	require.Empty(t, blocks)
}

// TestCohortMap_RecordRejectsUnsetCohort checks the one input the map cannot
// hold: the unset label, which means "no cohort recorded" and can never name a
// set of transactions.
func TestCohortMap_RecordRejectsUnsetCohort(t *testing.T) {
	s := newCohortMapTestStore(t)
	ctx := context.Background()

	blockID := storeCohortTestBlock(t, s, block1)

	err := s.RecordCohortMap(ctx, []model.CohortMapRow{{Cohort: cohort.Unset, BlockID: blockID}})
	require.Error(t, err)

	require.Empty(t, dumpCohortMap(t, s))
}

// TestCohortMap_RecordNoRowsIsANoOp checks that recording nothing is allowed.
func TestCohortMap_RecordNoRowsIsANoOp(t *testing.T) {
	s := newCohortMapTestStore(t)

	require.NoError(t, s.RecordCohortMap(context.Background(), nil))
	require.Empty(t, dumpCohortMap(t, s))
}

// TestCohortMap_RecordBatchesLargeInput checks that an input larger than one
// statement's worth of rows is written in full.
func TestCohortMap_RecordBatchesLargeInput(t *testing.T) {
	s := newCohortMapTestStore(t)
	ctx := context.Background()

	blockID := storeCohortTestBlock(t, s, block1)

	count := cohortMapInsertBatch*2 + 7
	rows := make([]model.CohortMapRow, 0, count)
	cohorts := make([]cohort.ID, 0, count)

	for i := 0; i < count; i++ {
		c := cohort.GenesisTime + 10000 + cohort.ID(i) //nolint:gosec // i is bounded by count
		rows = append(rows, model.CohortMapRow{Cohort: c, BlockID: blockID, MemberCount: uint64(i)})
		cohorts = append(cohorts, c)
	}

	require.NoError(t, s.RecordCohortMap(ctx, rows))
	require.Len(t, dumpCohortMap(t, s), count)

	blocks, err := s.CohortBlocks(ctx, cohorts)
	require.NoError(t, err)
	require.Len(t, blocks, count)
}

// TestCohortMap_AllocateSplitCohortIsIdempotent checks that the synthetic
// number handed out for one (source cohort, block) split is stable: a retry
// after a crash has to get the number the first attempt took, not a second one.
func TestCohortMap_AllocateSplitCohortIsIdempotent(t *testing.T) {
	s := newCohortMapTestStore(t)
	ctx := context.Background()

	source := cohort.GenesisTime + 4000

	first, err := s.AllocateSplitCohort(ctx, source, block1.Hash())
	require.NoError(t, err)
	require.True(t, first.IsSynthetic(), "allocated %s must be in the synthetic range", first)

	again, err := s.AllocateSplitCohort(ctx, source, block1.Hash())
	require.NoError(t, err)
	require.Equal(t, first, again, "the same (source, block) pair must always get the same number")

	// A different block for the same source is a different split.
	otherBlock, err := s.AllocateSplitCohort(ctx, source, block2.Hash())
	require.NoError(t, err)
	require.True(t, otherBlock.IsSynthetic())
	require.NotEqual(t, first, otherBlock)

	// A different source against the same block is a different split too.
	otherSource, err := s.AllocateSplitCohort(ctx, cohort.GenesisTime+4001, block1.Hash())
	require.NoError(t, err)
	require.True(t, otherSource.IsSynthetic())
	require.NotEqual(t, first, otherSource)
	require.NotEqual(t, otherBlock, otherSource)

	// A synthetic cohort can itself be split again.
	nested, err := s.AllocateSplitCohort(ctx, first, block3.Hash())
	require.NoError(t, err)
	require.True(t, nested.IsSynthetic())
}

// TestCohortMap_AllocateSplitCohortRejectsUnsplittableSources checks the guard
// on the sentinel labels: Unset, Historical and BornMined name classes of
// transaction, not sets that can be halved.
func TestCohortMap_AllocateSplitCohortRejectsUnsplittableSources(t *testing.T) {
	s := newCohortMapTestStore(t)
	ctx := context.Background()

	for _, source := range []cohort.ID{cohort.Unset, cohort.Historical, cohort.BornMined} {
		_, err := s.AllocateSplitCohort(ctx, source, block1.Hash())
		require.Error(t, err, "splitting %s must be refused", source)
	}

	_, err := s.AllocateSplitCohort(ctx, cohort.GenesisTime+5000, nil)
	require.Error(t, err, "a nil block hash must be refused")
}

// TestCohortMap_ReorgFlipsTheAnswerWithoutTouchingTheMap is the property the
// whole cohort design rests on: when a block leaves the main chain, every
// transaction in every cohort mapped to it stops being mined, and that happens
// purely through blocks.on_main_chain. Not one row of cohort_map is written,
// rewritten or deleted. If this test ever fails, the design's claim that a
// reorg costs no per-transaction work has failed with it.
func TestCohortMap_ReorgFlipsTheAnswerWithoutTouchingTheMap(t *testing.T) {
	s := newCohortMapTestStore(t)
	ctx := context.Background()

	// A small chain: genesis -> block1 -> block2 -> block3.
	storeCohortTestBlock(t, s, block1)
	storeCohortTestBlock(t, s, block2)
	block3ID := storeCohortTestBlock(t, s, block3)

	c := cohort.GenesisTime + 6000

	require.NoError(t, s.RecordCohortMap(ctx, []model.CohortMapRow{{
		Cohort:      c,
		BlockID:     block3ID,
		MemberCount: 5,
		Verified:    true,
	}}))

	before, err := s.CohortBlocks(ctx, []cohort.ID{c})
	require.NoError(t, err)
	require.Len(t, before[c], 1)
	require.True(t, before[c][0].OnMainChain, "pre-condition: block3 is on the main chain")
	require.False(t, before[c][0].Invalid, "pre-condition: block3 is valid")

	mapBefore := dumpCohortMap(t, s)
	require.Len(t, mapBefore, 1)

	// Take block3 off the main chain.
	_, err = s.InvalidateBlock(ctx, block3.Hash())
	require.NoError(t, err)
	require.False(t, getOnMainChain(t, s, block3.Hash().CloneBytes()), "block3 must be off the main chain")

	after, err := s.CohortBlocks(ctx, []cohort.ID{c})
	require.NoError(t, err)
	require.Len(t, after[c], 1)
	require.False(t, after[c][0].OnMainChain, "the cohort's answer must flip with the block's flag")
	require.True(t, after[c][0].Invalid)

	// The map itself is untouched: same row count, same contents.
	require.Equal(t, mapBefore, dumpCohortMap(t, s), "a reorg must not write to cohort_map")

	// And it flips back when the block returns, still with no writes.
	require.NoError(t, s.RevalidateBlock(ctx, block3.Hash()))
	require.True(t, getOnMainChain(t, s, block3.Hash().CloneBytes()), "block3 must be back on the main chain")

	restored, err := s.CohortBlocks(ctx, []cohort.ID{c})
	require.NoError(t, err)
	require.Len(t, restored[c], 1)
	require.True(t, restored[c][0].OnMainChain)
	require.False(t, restored[c][0].Invalid)

	require.Equal(t, mapBefore, dumpCohortMap(t, s), "revalidation must not write to cohort_map either")
}

// TestCohortMap_DeletingABlockCascadesItsRowsAway checks that the only way a
// cohort_map row ever disappears is with the block it points at, via the
// foreign key. That relies on sqlite having foreign keys switched on, which
// util.InitSQLDB does through the `_pragma=foreign_keys=on` DSN parameter, so
// this test is also the check that the pragma really is in force.
func TestCohortMap_DeletingABlockCascadesItsRowsAway(t *testing.T) {
	s := newCohortMapTestStore(t)
	ctx := context.Background()

	id1 := storeCohortTestBlock(t, s, block1)
	storeCohortTestBlock(t, s, block2)
	id3 := storeCohortTestBlock(t, s, block3)

	c := cohort.GenesisTime + 7000

	require.NoError(t, s.RecordCohortMap(ctx, []model.CohortMapRow{
		{Cohort: c, BlockID: id1, MemberCount: 1},
		{Cohort: c, BlockID: id3, MemberCount: 2},
	}))
	require.Len(t, dumpCohortMap(t, s), 2)

	// block3 is a leaf, so deleting it leaves the rest of the chain intact.
	require.NoError(t, s.DeleteBlock(ctx, block3.Hash()))

	remaining := dumpCohortMap(t, s)
	require.Len(t, remaining, 1, "block3's cohort_map row must have gone with the block")
	require.Equal(t, uint64(id1), remaining[0].BlockID)

	blocks, err := s.CohortBlocks(ctx, []cohort.ID{c})
	require.NoError(t, err)
	require.Len(t, blocks[c], 1)
	require.Equal(t, id1, blocks[c][0].BlockID)
}

// TestCohortMap_ForeignKeyRejectsUnknownBlock checks that a row can only ever
// point at a block that exists, which is what makes the cascade above the
// complete story of how rows leave the table.
func TestCohortMap_ForeignKeyRejectsUnknownBlock(t *testing.T) {
	s := newCohortMapTestStore(t)

	err := s.RecordCohortMap(context.Background(), []model.CohortMapRow{{
		Cohort:  cohort.GenesisTime + 8000,
		BlockID: 999999,
	}})
	require.Error(t, err)
}

// TestCohortMap_AllocateSplitCohortNumbersAreDistinct checks that a run of
// allocations never hands the same synthetic number to two different splits,
// and that each pair still gets a single stable answer when asked again.
func TestCohortMap_AllocateSplitCohortNumbersAreDistinct(t *testing.T) {
	s := newCohortMapTestStore(t)
	ctx := context.Background()

	const pairs = 8

	seen := make(map[cohort.ID]struct{}, pairs)

	for i := 0; i < pairs; i++ {
		hash := chainhash.HashH([]byte{byte(i)})

		allocated, err := s.AllocateSplitCohort(ctx, cohort.GenesisTime+9000, &hash)
		require.NoError(t, err)
		require.True(t, allocated.IsSynthetic())

		_, duplicate := seen[allocated]
		require.False(t, duplicate, "synthetic number %s handed out twice", allocated)

		seen[allocated] = struct{}{}

		repeat, err := s.AllocateSplitCohort(ctx, cohort.GenesisTime+9000, &hash)
		require.NoError(t, err)
		require.Equal(t, allocated, repeat)
	}

	require.Len(t, seen, pairs)
}
