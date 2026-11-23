package settings

import (
	"github.com/bsv-blockchain/teranode/errors"
)

// LaneMigration is the step of the lane migration a node is at (utxostore_laneMigration). Each step
// adds to the one before: shadow stamps lane positions on new records, rows switches readers to the
// lane rows, and lanes stops the old writes.
type LaneMigration string

const (
	// LaneMigrationOff takes no lease and stamps nothing.
	LaneMigrationOff LaneMigration = "off"
	// LaneMigrationShadow stamps a lane position on every new record. Readers copy parent positions
	// onto the child and into txmeta; nothing decides acceptance or pruning from one yet.
	LaneMigrationShadow LaneMigration = "shadow"
	// LaneMigrationRows switches readers to the lane rows. Not supported by this build.
	LaneMigrationRows LaneMigration = "rows"
	// LaneMigrationLanes stops the old writes. Not supported by this build.
	LaneMigrationLanes LaneMigration = "lanes"
)

// Stamps reports whether new records carry lane positions at this step: shadow and every later one.
func (m LaneMigration) Stamps() bool {
	switch m {
	case LaneMigrationShadow, LaneMigrationRows, LaneMigrationLanes:
		return true
	default:
		return false
	}
}

// MaxLaneGrant is the most lane positions one grant can hold, 2^40 (about 1.1 trillion). A lane
// holds 2^56 positions, so it takes 65,536 grants of this size to use one up, where without a cap a
// single grant sized from a huge reported rate or blockchain_laneGrantMin could exhaust a lane for
// good. It is far above any real need: two minutes at a million positions a second is 1.2 x 10^8.
const MaxLaneGrant uint64 = 1 << 40

// CheckLaneMigration refuses a step this build does not implement, and any unknown value, so a node
// configured for a later step fails at start instead of running a step it only half has.
func CheckLaneMigration(m LaneMigration) error {
	switch m {
	case LaneMigrationOff, LaneMigrationShadow:
		return nil
	case LaneMigrationRows, LaneMigrationLanes:
		return errors.NewConfigurationError("utxostore_laneMigration=%s is not supported by this build; use off or shadow", m)
	default:
		return errors.NewConfigurationError("utxostore_laneMigration=%q is not a known step; use off or shadow", m)
	}
}

// CheckLaneLease refuses lease timings that would let two processes hand out positions in one lane
// at once. A process stops handing out positions LaneLeaseStopAfter after its last successful
// renewal was sent, and the blockchain service closes the lease LaneLeaseCloseAfter after it
// received the last renewal, so stop must come before close. The check sees only this process's
// settings, so it compares this process's stop with this process's close. The close that counts
// is the blockchain service's, which may run in another pod: every pod must share both settings.
// If they differ, positions stay unique, but two holders can hand out positions in one lane at
// once. A process renews every LaneLeaseRenewInterval, so one renewal must fit inside the stop or
// the lease could never be kept.
func (s BlockChainSettings) CheckLaneLease() error {
	if s.LaneLeaseRenewInterval <= 0 {
		return errors.NewConfigurationError("blockchain_laneLeaseRenewInterval must be positive, got %s", s.LaneLeaseRenewInterval)
	}

	if s.LaneLeaseStopAfter >= s.LaneLeaseCloseAfter {
		return errors.NewConfigurationError("blockchain_laneLeaseStopAfter (%s) must be shorter than blockchain_laneLeaseCloseAfter (%s)", s.LaneLeaseStopAfter, s.LaneLeaseCloseAfter)
	}

	if s.LaneGrantDuration <= 0 {
		return errors.NewConfigurationError("blockchain_laneGrantDuration must be positive, got %s", s.LaneGrantDuration)
	}

	if s.LaneGrantMin > MaxLaneGrant {
		return errors.NewConfigurationError("blockchain_laneGrantMin (%d) must be at most %d", s.LaneGrantMin, MaxLaneGrant)
	}

	if s.LaneLeaseRenewInterval >= s.LaneLeaseStopAfter {
		return errors.NewConfigurationError("blockchain_laneLeaseRenewInterval (%s) must be shorter than blockchain_laneLeaseStopAfter (%s)", s.LaneLeaseRenewInterval, s.LaneLeaseStopAfter)
	}

	return nil
}
