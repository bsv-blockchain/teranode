package blockchain

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/util/cohort"
	"github.com/stretchr/testify/require"
)

// TestMockStoreRecordCohortMapRejectsUnset proves the mock refuses the unset
// cohort the same way the SQL store does, and that the refusal leaves nothing
// behind.
//
// The end state is what is asserted, not the call: a batch carrying one unset
// row must write NONE of its rows, so a test that uses the mock cannot pass on
// a call the real store would fail. If the guard moved below the write, the
// valid row in the same batch would land and CohortBlocks would answer with it.
func TestMockStoreRecordCohortMapRejectsUnset(t *testing.T) {
	ctx := context.Background()
	m := &MockStore{}

	err := m.RecordCohortMap(ctx, []CohortMapRow{
		{Cohort: cohort.ID(1_700_000_000), BlockID: 7, MemberCount: 3},
		{Cohort: cohort.Unset, BlockID: 8},
	})
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrInvalidArgument))

	// Nothing from the rejected batch was written, not even the valid row.
	blocks, err := m.CohortBlocks(ctx, []cohort.ID{cohort.ID(1_700_000_000)})
	require.NoError(t, err)
	require.Empty(t, blocks[cohort.ID(1_700_000_000)])

	// A batch with no unset row still records normally.
	require.NoError(t, m.RecordCohortMap(ctx, []CohortMapRow{
		{Cohort: cohort.ID(1_700_000_000), BlockID: 7, MemberCount: 3},
	}))
}

// TestMockStoreAllocateSplitCohortKeysOnTheNumericID proves the allocation key
// separates two different source cohorts for the same block, and is stable for
// a repeated pair.
func TestMockStoreAllocateSplitCohortKeysOnTheNumericID(t *testing.T) {
	ctx := context.Background()
	m := &MockStore{}

	hash := &chainhash.Hash{1, 2, 3}

	a, err := m.AllocateSplitCohort(ctx, cohort.ID(1_700_000_000), hash)
	require.NoError(t, err)

	again, err := m.AllocateSplitCohort(ctx, cohort.ID(1_700_000_000), hash)
	require.NoError(t, err)
	require.Equal(t, a, again, "the same pair must always get the same synthetic cohort")

	b, err := m.AllocateSplitCohort(ctx, cohort.ID(1_700_000_001), hash)
	require.NoError(t, err)
	require.NotEqual(t, a, b, "a different source cohort in the same block is a different allocation")
}
