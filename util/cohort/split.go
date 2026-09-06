package cohort

import (
	"fmt"

	"github.com/bsv-blockchain/teranode/errors"
)

// Class says how a cohort sits against one block.
type Class uint8

const (
	// ClassOutside means none of the cohort's transactions are in the block, so
	// the block says nothing about the cohort and there is nothing to do.
	ClassOutside Class = iota

	// ClassStraddle means some but not all of the cohort's transactions are in
	// the block, so the cohort has to be split before it can be mapped.
	ClassStraddle

	// ClassInside means every one of the cohort's transactions is in the block,
	// so a single map row covers all of them and no transaction is touched.
	ClassInside
)

// String renders the class for logs.
func (c Class) String() string {
	switch c {
	case ClassOutside:
		return "outside"
	case ClassInside:
		return "inside"
	case ClassStraddle:
		return "straddle"
	default:
		return fmt.Sprintf("class(%d)", uint8(c))
	}
}

// Classify says how a cohort of total transactions, of which inBlock are in the
// block being processed, sits against that block.
func Classify(total, inBlock uint64) (Class, error) {
	if total == 0 {
		return ClassOutside, errors.NewInvalidArgumentError("cohort: cannot classify an empty cohort")
	}

	if inBlock > total {
		return ClassOutside, errors.NewInvalidArgumentError("cohort: %d transactions in block exceeds the cohort total of %d", inBlock, total)
	}

	switch inBlock {
	case 0:
		return ClassOutside, nil
	case total:
		return ClassInside, nil
	default:
		return ClassStraddle, nil
	}
}

// Side names one of the two halves a straddling cohort splits into.
type Side uint8

const (
	// SideInBlock is the half of the cohort whose transactions are in the block
	// being processed.
	SideInBlock Side = iota

	// SideOutOfBlock is the half whose transactions are not in that block.
	SideOutOfBlock
)

// String renders the side for logs.
func (s Side) String() string {
	switch s {
	case SideInBlock:
		return "in-block"
	case SideOutOfBlock:
		return "out-of-block"
	default:
		return fmt.Sprintf("side(%d)", uint8(s))
	}
}

// Target names a cohort inside a plan step without naming its number. The fresh
// cohort does not have a number yet when the plan is made: it is allocated from
// the synthetic allocation table when the plan is executed, so the plan can only
// point at it.
type Target uint8

const (
	// TargetSource is the straddling cohort being split.
	TargetSource Target = iota

	// TargetFresh is the synthetic cohort allocated to receive the moved side.
	TargetFresh
)

// String renders the target for logs.
func (t Target) String() string {
	switch t {
	case TargetSource:
		return "source"
	case TargetFresh:
		return "fresh"
	default:
		return fmt.Sprintf("target(%d)", uint8(t))
	}
}

// StepKind is what a plan step does.
type StepKind uint8

const (
	// StepInheritBlocks inserts one cohort map row per block the source cohort
	// is already mapped to, this time against the fresh cohort.
	StepInheritBlocks StepKind = iota

	// StepRestamp rewrites the cohort label on the transactions of the moved
	// side, from the source cohort to the fresh cohort.
	StepRestamp

	// StepMapNewBlock inserts the map row for the block being processed.
	StepMapNewBlock
)

// String renders the step kind for logs.
func (k StepKind) String() string {
	switch k {
	case StepInheritBlocks:
		return "inherit-blocks"
	case StepRestamp:
		return "restamp"
	case StepMapNewBlock:
		return "map-new-block"
	default:
		return fmt.Sprintf("step(%d)", uint8(k))
	}
}

// Step is one unit of work in a split plan. The steps of a plan are ordered and
// the order is mandatory; see Plan.
type Step struct {
	// Kind is what this step does.
	Kind StepKind

	// Source is the straddling cohort being split. It is set on every step.
	Source ID

	// Cohort is the cohort this step writes against: the cohort the map rows are
	// recorded for on StepInheritBlocks and StepMapNewBlock, and the cohort the
	// transactions are re-stamped to on StepRestamp (always the fresh one).
	Cohort Target

	// Side is the half of the cohort whose transactions this step re-stamps. It
	// is only meaningful on StepRestamp.
	Side Side

	// Count is how many transactions this step re-stamps. It is only meaningful
	// on StepRestamp.
	Count uint64
}

// Plan is the ordered recipe for splitting one straddling cohort around one
// block. Steps must be executed in the order given; the ordering is the whole
// crash-safety property of the design, so it is carried as data rather than
// described in prose, and a caller only has to run the slice front to back.
//
// Why that order, given a straddling cohort S mapped to blocks {X, Y} and a new
// block N:
//
//  1. Inherit first. Copy S's existing map rows onto the fresh cohort F while
//     the transactions that are about to move are still labelled S and so are
//     still covered by S's rows. A crash after this step has changed nothing a
//     reader can see: F holds no transactions yet, so its rows are inert.
//
//  2. Re-stamp the moved side to F second. Each transaction that moves goes from
//     rows {X, Y} to rows {X, Y}, because step 1 already put them there, so it
//     never loses a block it was in. A crash part-way through leaves some
//     transactions on S and some on F, and both read identically.
//
//  3. Map the new block last, against whichever cohort now holds the in-block
//     transactions. This is the step that makes the cohort mined in N, so it
//     comes after everything the in-block side needs is in place. A crash before
//     it leaves every transaction reading "not mined yet" - the safe direction,
//     because the block will be re-processed and the map is insert-only, so
//     replaying the whole plan is harmless.
//
// Doing it the other way round is what breaks: mapping N first, or re-stamping
// before inheriting, opens a window where a crash leaves transactions claiming a
// block they are not in, or dropping blocks they are in.
type Plan struct {
	// Source is the straddling cohort being split.
	Source ID

	// Moved is the side that is re-stamped to the fresh cohort. It is always the
	// smaller of the two, so the number of transactions rewritten is as small as
	// it can be.
	Moved Side

	// MovedCount is how many transactions are re-stamped.
	MovedCount uint64

	// RemainingCount is how many transactions stay on the source cohort.
	RemainingCount uint64

	// Steps are the steps to execute, in mandatory order.
	Steps []Step
}

// PlanSplit works out how to split a straddling cohort around the block being
// processed. total is the number of transactions in the cohort and inBlock is
// how many of them are in that block.
//
// The smaller side moves, and on a tie the in-block side moves, so that two
// nodes planning the same split reach the same plan. The new block's map row
// then goes against whichever cohort ended up holding the in-block
// transactions: the source cohort if the out-of-block side moved away, the
// fresh cohort if the in-block side moved to it.
func PlanSplit(source ID, total, inBlock uint64) (Plan, error) {
	if !source.IsClock() && !source.IsSynthetic() {
		return Plan{}, errors.NewInvalidArgumentError("cohort: cannot split %s: only clock and synthetic cohorts hold splittable transactions", source)
	}

	class, err := Classify(total, inBlock)
	if err != nil {
		return Plan{}, err
	}

	if class != ClassStraddle {
		return Plan{}, errors.NewInvalidArgumentError("cohort: cannot split %s: it is %s this block, not straddling it", source, class)
	}

	outOfBlock := total - inBlock

	plan := Plan{
		Source: source,
	}

	// The smaller side moves; a tie moves the in-block side, deterministically.
	newBlockRowAgainst := TargetSource

	if inBlock <= outOfBlock {
		plan.Moved = SideInBlock
		plan.MovedCount = inBlock
		plan.RemainingCount = outOfBlock
		newBlockRowAgainst = TargetFresh
	} else {
		plan.Moved = SideOutOfBlock
		plan.MovedCount = outOfBlock
		plan.RemainingCount = inBlock
	}

	plan.Steps = []Step{
		{
			Kind:   StepInheritBlocks,
			Source: source,
			Cohort: TargetFresh,
		},
		{
			Kind:   StepRestamp,
			Source: source,
			Cohort: TargetFresh,
			Side:   plan.Moved,
			Count:  plan.MovedCount,
		},
		{
			Kind:   StepMapNewBlock,
			Source: source,
			Cohort: newBlockRowAgainst,
		},
	}

	return plan, nil
}
