package cohort

import (
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
)

// DefaultMinMapAge is how old a clock cohort must be before a block is allowed
// to map it. It gives in-flight transactions that were stamped with that second
// time to finish being written before the cohort is nailed to a block.
const DefaultMinMapAge = 7 * time.Second

// Stamper hands out cohort IDs at transaction create time. It is safe for
// concurrent use.
//
// The clock alone is not enough. Once a clock cohort has been mapped to a block,
// a transaction stamped into that same cohort would read as mined even though it
// was never in the block. So the stamper keeps a floor: the newest clock cohort
// it has been told is already mapped. Stamps are never issued at or below that
// floor.
// Stamp is lock-free: it reads the floor, compares, and returns. Two callers
// racing on the fallback both return floor+1, which is correct - a cohort is a
// group, many transactions share one - and neither can land at or below the
// floor, which is the only thing the guard has to guarantee. Stamping is on the
// per-transaction create path, so it must not serialise on a mutex.
type Stamper struct {
	now    func() time.Time
	floor  atomic.Uint32
	skewed atomic.Uint64
}

// StamperOption configures a Stamper at construction.
type StamperOption func(*Stamper)

// WithClock replaces the clock the stamper reads. It exists for tests; in
// production the stamper reads time.Now.
func WithClock(now func() time.Time) StamperOption {
	return func(s *Stamper) {
		if now != nil {
			s.now = now
		}
	}
}

// NewStamper builds a Stamper reading the wall clock, with no floor recorded
// yet.
func NewStamper(opts ...StamperOption) *Stamper {
	s := &Stamper{now: time.Now}

	for _, opt := range opts {
		opt(s)
	}

	return s
}

// Stamp returns the cohort to label a newly created transaction with: the clock
// cohort for the current second, or one above the floor if the clock has not
// moved past the floor yet.
//
// A clock that is slow, frozen or running backwards must not stop transactions
// being created, so Stamp falls back to floor+1 rather than failing, and counts
// the event in SkewedStamps. The same fallback covers a clock reading outside
// the representable range entirely - before the Bitcoin genesis second, or
// beyond what a uint32 can hold. It only fails in two cases that no amount of
// falling back can rescue: the clock is outside that range and no floor has ever
// been observed, so there is nothing to count up from; and the floor has reached
// the top of the uint32 range, so there is no room above it.
func (s *Stamper) Stamp() (ID, error) {
	// A clock reading outside the representable range leaves us with no
	// candidate at all; the floor fallback below is then the only option. Keep
	// the reason rather than asserting one - the clock can be too early or too
	// late, and reporting the wrong direction sends an operator the wrong way.
	candidate, clockErr := FromTime(s.now())
	if clockErr != nil {
		candidate = Unset
	}

	floor := ID(s.floor.Load())

	if candidate > floor {
		return candidate, nil
	}

	if floor == Unset {
		return Unset, errors.NewProcessingError("cohort: clock reading is outside the cohort range and no mapped cohort has been observed", clockErr)
	}

	if floor == MaxClock {
		return Unset, errors.NewProcessingError("cohort: clock cohort space is exhausted at %d", uint32(MaxClock))
	}

	s.skewed.Add(1)

	return floor + 1, nil
}

// ObserveMapped raises the floor to id, so that no later stamp lands in a cohort
// that is already nailed to a block.
//
// Only clock IDs count. Synthetic IDs are allocation counters drawn from the
// sub-genesis range and carry no time information at all, so they are not
// magnitude-comparable with clock cohorts; letting one raise the floor would set
// the floor to a small number and switch the guard off entirely, or - the other
// way round - a synthetic ID could never exceed a clock floor and so could never
// mean anything here. Sentinels stand for classes of transaction, not moments,
// so they are ignored for the same reason. A lower clock ID is ignored too: the
// floor only ever rises.
func (s *Stamper) ObserveMapped(id ID) {
	if !id.IsClock() {
		return
	}

	for {
		floor := ID(s.floor.Load())
		if id <= floor {
			return
		}

		if s.floor.CompareAndSwap(uint32(floor), uint32(id)) {
			return
		}
	}
}

// Floor returns the newest clock cohort the stamper knows to be mapped to a
// block. It is Unset until the first clock cohort is observed.
func (s *Stamper) Floor() ID {
	return ID(s.floor.Load())
}

// SkewedStamps returns how many stamps had to fall back to floor+1 because the
// clock had not advanced past the floor. A number that keeps climbing means the
// clock is slow, frozen, running backwards, or so far ahead that it falls
// outside the representable cohort range.
func (s *Stamper) SkewedStamps() uint64 {
	return s.skewed.Load()
}

// CanMap reports whether a cohort is allowed to be mapped to a block yet.
//
// A clock cohort has to be at least minAge old, measured from the second it
// stands for, so that transactions still being written with that second are not
// left stranded outside a block that claims their cohort. Synthetic cohorts are
// exempt: they are minted by a split, never by the clock, so nothing new is
// still arriving in them. Sentinels and Unset are never mapped.
func CanMap(id ID, now time.Time, minAge time.Duration) bool {
	if id.IsSynthetic() {
		return true
	}

	born, ok := id.Time()
	if !ok {
		return false
	}

	return now.Sub(born) >= minAge
}
