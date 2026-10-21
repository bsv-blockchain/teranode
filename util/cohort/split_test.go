package cohort

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClassify(t *testing.T) {
	tests := []struct {
		name    string
		total   uint64
		inBlock uint64
		want    Class
		wantErr bool
	}{
		{name: "none in the block", total: 10, inBlock: 0, want: ClassOutside},
		{name: "one in the block", total: 10, inBlock: 1, want: ClassStraddle},
		{name: "all but one in the block", total: 10, inBlock: 9, want: ClassStraddle},
		{name: "all in the block", total: 10, inBlock: 10, want: ClassInside},
		{name: "single member outside", total: 1, inBlock: 0, want: ClassOutside},
		{name: "single member inside", total: 1, inBlock: 1, want: ClassInside},
		{name: "empty cohort", total: 0, inBlock: 0, wantErr: true},
		{name: "empty cohort with members in the block", total: 0, inBlock: 1, wantErr: true},
		{name: "more in the block than exist", total: 10, inBlock: 11, wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := Classify(test.total, test.inBlock)
			if test.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.Equal(t, test.want, got)
		})
	}
}

func TestClassString(t *testing.T) {
	require.Equal(t, "outside", ClassOutside.String())
	require.Equal(t, "straddle", ClassStraddle.String())
	require.Equal(t, "inside", ClassInside.String())
	require.Equal(t, "class(9)", Class(9).String())
}

func TestSideString(t *testing.T) {
	require.Equal(t, "in-block", SideInBlock.String())
	require.Equal(t, "out-of-block", SideOutOfBlock.String())
	require.Equal(t, "side(9)", Side(9).String())
}

func TestTargetString(t *testing.T) {
	require.Equal(t, "source", TargetSource.String())
	require.Equal(t, "fresh", TargetFresh.String())
	require.Equal(t, "target(9)", Target(9).String())
}

func TestStepKindString(t *testing.T) {
	require.Equal(t, "inherit-blocks", StepInheritBlocks.String())
	require.Equal(t, "restamp", StepRestamp.String())
	require.Equal(t, "map-new-block", StepMapNewBlock.String())
	require.Equal(t, "step(9)", StepKind(9).String())
}

func TestPlanSplitChoosesTheSmallerSide(t *testing.T) {
	source := GenesisTime + 1000

	tests := []struct {
		name               string
		total              uint64
		inBlock            uint64
		wantMoved          Side
		wantMovedCount     uint64
		wantRemainingCount uint64
		wantNewBlockCohort Target
	}{
		{
			name:               "in-block side is smaller so it moves",
			total:              100,
			inBlock:            10,
			wantMoved:          SideInBlock,
			wantMovedCount:     10,
			wantRemainingCount: 90,
			wantNewBlockCohort: TargetFresh,
		},
		{
			name:               "out-of-block side is smaller so it moves",
			total:              100,
			inBlock:            90,
			wantMoved:          SideOutOfBlock,
			wantMovedCount:     10,
			wantRemainingCount: 90,
			wantNewBlockCohort: TargetSource,
		},
		{
			name:               "an even split moves the in-block side",
			total:              100,
			inBlock:            50,
			wantMoved:          SideInBlock,
			wantMovedCount:     50,
			wantRemainingCount: 50,
			wantNewBlockCohort: TargetFresh,
		},
		{
			name:               "smallest possible straddle with one in the block",
			total:              2,
			inBlock:            1,
			wantMoved:          SideInBlock,
			wantMovedCount:     1,
			wantRemainingCount: 1,
			wantNewBlockCohort: TargetFresh,
		},
		{
			name:               "one transaction left outside",
			total:              1_000_000,
			inBlock:            999_999,
			wantMoved:          SideOutOfBlock,
			wantMovedCount:     1,
			wantRemainingCount: 999_999,
			wantNewBlockCohort: TargetSource,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plan, err := PlanSplit(source, test.total, test.inBlock)
			require.NoError(t, err)

			require.Equal(t, source, plan.Source)
			require.Equal(t, test.wantMoved, plan.Moved)
			require.Equal(t, test.wantMovedCount, plan.MovedCount)
			require.Equal(t, test.wantRemainingCount, plan.RemainingCount)
			require.Equal(t, test.total, plan.MovedCount+plan.RemainingCount)

			require.Len(t, plan.Steps, 3)
			require.Equal(t, test.wantNewBlockCohort, plan.Steps[2].Cohort)
		})
	}
}

func TestPlanSplitStepOrderIsMandatory(t *testing.T) {
	source := ID(4242) // a synthetic cohort, split for a second time

	plan, err := PlanSplit(source, 100, 10)
	require.NoError(t, err)

	// The order is the crash-safety property: inherit the source's existing
	// blocks onto the fresh cohort first, then move the smaller side, and only
	// then record the new block.
	require.Equal(t, []Step{
		{
			Kind:   StepInheritBlocks,
			Source: source,
			Cohort: TargetFresh,
		},
		{
			Kind:   StepRestamp,
			Source: source,
			Cohort: TargetFresh,
			Side:   SideInBlock,
			Count:  10,
		},
		{
			Kind:   StepMapNewBlock,
			Source: source,
			Cohort: TargetFresh,
		},
	}, plan.Steps)
}

func TestPlanSplitStepOrderWhenTheOutOfBlockSideMoves(t *testing.T) {
	source := GenesisTime + 77

	plan, err := PlanSplit(source, 100, 90)
	require.NoError(t, err)

	require.Equal(t, []Step{
		{
			Kind:   StepInheritBlocks,
			Source: source,
			Cohort: TargetFresh,
		},
		{
			Kind:   StepRestamp,
			Source: source,
			Cohort: TargetFresh,
			Side:   SideOutOfBlock,
			Count:  10,
		},
		{
			// The in-block transactions never moved, so the new block's row goes
			// against the source cohort.
			Kind:   StepMapNewBlock,
			Source: source,
			Cohort: TargetSource,
		},
	}, plan.Steps)
}

// TestPlanSplitWorkedExampleCohortE is the example from the design: cohort E has
// a million members and sits wholly inside block A; a competing block B contains
// 990,000 of them. Splitting E around B must move only the 10,000 members that B
// left out, and must inherit E's existing mapping to A onto the fresh cohort
// before anything is re-stamped.
func TestPlanSplitWorkedExampleCohortE(t *testing.T) {
	cohortE := GenesisTime + 123_456

	const (
		total   = uint64(1_000_000)
		inBlock = uint64(990_000)
	)

	class, err := Classify(total, inBlock)
	require.NoError(t, err)
	require.Equal(t, ClassStraddle, class)

	plan, err := PlanSplit(cohortE, total, inBlock)
	require.NoError(t, err)

	require.Equal(t, cohortE, plan.Source)
	require.Equal(t, SideOutOfBlock, plan.Moved)
	require.Equal(t, uint64(10_000), plan.MovedCount, "the 10,000 members block B left out are the ones that move")
	require.Equal(t, uint64(990_000), plan.RemainingCount)

	require.Len(t, plan.Steps, 3)

	require.Equal(t, StepInheritBlocks, plan.Steps[0].Kind)
	require.Equal(t, TargetFresh, plan.Steps[0].Cohort, "the fresh cohort inherits E's row for block A first")

	require.Equal(t, StepRestamp, plan.Steps[1].Kind)
	require.Equal(t, SideOutOfBlock, plan.Steps[1].Side)
	require.Equal(t, uint64(10_000), plan.Steps[1].Count)
	require.Equal(t, TargetFresh, plan.Steps[1].Cohort)

	require.Equal(t, StepMapNewBlock, plan.Steps[2].Kind)
	require.Equal(t, TargetSource, plan.Steps[2].Cohort, "block B's row goes against E, which still holds its 990,000 members")
}

func TestPlanSplitRejectsNonStraddles(t *testing.T) {
	source := GenesisTime + 1000

	tests := []struct {
		name    string
		source  ID
		total   uint64
		inBlock uint64
	}{
		{name: "wholly outside the block", source: source, total: 100, inBlock: 0},
		{name: "wholly inside the block", source: source, total: 100, inBlock: 100},
		{name: "empty cohort", source: source, total: 0, inBlock: 0},
		{name: "more in the block than exist", source: source, total: 100, inBlock: 101},
		{name: "unset cohort", source: Unset, total: 100, inBlock: 10},
		{name: "historical cohort", source: Historical, total: 100, inBlock: 10},
		{name: "born mined cohort", source: BornMined, total: 100, inBlock: 10},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plan, err := PlanSplit(test.source, test.total, test.inBlock)
			require.Error(t, err)
			require.Nil(t, plan.Steps)
		})
	}
}
