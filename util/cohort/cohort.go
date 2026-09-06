// Package cohort holds the pure core of the cohort-based mined state design
// (issue 556).
//
// Every transaction carries a single 4-byte cohort label, stamped once when the
// transaction record is created and never rewritten on the mining path. A block
// records which cohorts it contains as a handful of insert-only rows in the
// blockchain database, so a transaction's blocks are the map rows for its
// cohort, and the transaction is mined exactly when one of those rows sits on
// the main chain.
//
// This package is deliberately free of I/O: it defines the label, the rules for
// handing out labels, the rule for when a cohort is old enough to be mapped, the
// classification of a cohort against a block, and the ordered plan for splitting
// a cohort that straddles a block boundary. Everything else in the design
// imports it.
package cohort

import (
	"fmt"
	"math"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
)

// ID is a transaction's cohort label. It is a uint32 so that it costs four
// bytes on every transaction record.
//
// The value space is divided into three ranges:
//
//	0                        Unset      - no cohort recorded
//	1                        Historical - imported, born mined
//	2                        BornMined  - created as part of a block
//	[3, GenesisTime)         synthetic  - minted by a split, allocated from a table
//	[GenesisTime, MaxUint32] clock      - the Unix second the transaction was created in
//
// The synthetic range sits below the Bitcoin genesis timestamp precisely so that
// no synthetic ID can ever be mistaken for a clock ID, and so that clock IDs
// remain comparable by magnitude among themselves. Synthetic IDs are NOT
// magnitude-comparable with clock IDs: a synthetic number is an allocation
// counter, not a time.
type ID uint32

const (
	// Unset means no cohort was recorded for this transaction: either the record
	// predates the cohort feature, or the feature flag was off when it was
	// created. It is never a valid stamp.
	Unset ID = 0

	// Historical marks transactions brought in by a snapshot or seed import.
	// They were born already mined and their real creation time is unknown.
	Historical ID = 1

	// BornMined marks transactions created as part of a block - quick-validate,
	// legacy netsync, and coinbase transactions. They are mined by construction,
	// so they never need a cohort map row.
	BornMined ID = 2

	// FirstSynthetic is the lowest synthetic cohort ID. Synthetic IDs are minted
	// by cohort splits and handed out from an allocation table, not derived from
	// the clock.
	FirstSynthetic ID = 3

	// GenesisTime is the timestamp of the Bitcoin genesis block, and the
	// boundary between the synthetic range below it and the clock range at and
	// above it. No real transaction can have been created before it, so the
	// range below is free for synthetic use.
	GenesisTime ID = 1231006505

	// LastSynthetic is the highest synthetic cohort ID, one below GenesisTime.
	LastSynthetic ID = GenesisTime - 1

	// MaxClock is the highest representable clock cohort, the last Unix second a
	// uint32 can hold (in the year 2106).
	MaxClock ID = math.MaxUint32
)

// IsUnset reports whether no cohort was recorded.
func (id ID) IsUnset() bool {
	return id == Unset
}

// IsSentinel reports whether the ID is one of the fixed sentinel values that
// stand for a class of transaction rather than for a point in time or an
// allocation: Unset, Historical or BornMined.
func (id ID) IsSentinel() bool {
	return id == Unset || id == Historical || id == BornMined
}

// IsSynthetic reports whether the ID was minted by a split, that is, whether it
// falls in [FirstSynthetic, GenesisTime).
func (id ID) IsSynthetic() bool {
	return id >= FirstSynthetic && id < GenesisTime
}

// IsClock reports whether the ID is a wall-clock cohort, that is, the Unix
// second a transaction was created in.
func (id ID) IsClock() bool {
	return id >= GenesisTime
}

// String renders the ID the way a human wants to read it in a log line:
// sentinels by name, synthetic IDs as "synthetic:N", and clock IDs as the
// RFC3339 second they stand for.
func (id ID) String() string {
	switch {
	case id == Unset:
		return "unset"
	case id == Historical:
		return "historical"
	case id == BornMined:
		return "born-mined"
	case id.IsSynthetic():
		return fmt.Sprintf("synthetic:%d", uint32(id))
	default:
		return time.Unix(int64(id), 0).UTC().Format(time.RFC3339)
	}
}

// FromUnix builds a clock cohort from a Unix second. It fails for any second
// outside the clock range: before the Bitcoin genesis block, or beyond what a
// uint32 can hold.
func FromUnix(sec int64) (ID, error) {
	if sec < int64(GenesisTime) {
		return Unset, errors.NewInvalidArgumentError("cohort: unix second %d is before genesis %d", sec, int64(GenesisTime))
	}

	if sec > int64(MaxClock) {
		return Unset, errors.NewInvalidArgumentError("cohort: unix second %d is beyond the uint32 range %d", sec, int64(MaxClock))
	}

	return ID(sec), nil
}

// FromTime builds a clock cohort from a time, truncating to the second the time
// falls in.
func FromTime(t time.Time) (ID, error) {
	return FromUnix(t.Unix())
}

// Time returns the second this clock cohort stands for, in UTC. The second
// return value is false for anything that is not a clock cohort, in which case
// the time is the zero time.
func (id ID) Time() (time.Time, bool) {
	if !id.IsClock() {
		return time.Time{}, false
	}

	return time.Unix(int64(id), 0).UTC(), true
}
