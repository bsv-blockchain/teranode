package options

import (
	"time"

	"github.com/bsv-blockchain/teranode/util/lane"
)

// LaneLeaseOpen asks for a new lease on a free lane, with its first grant.
type LaneLeaseOpen struct {
	// Holder names the process asking, recorded with the lease and its hand-outs.
	Holder string
	// Height is the best block height at the time, recorded for operators.
	Height uint32
	// GrantMin is the size of the first grant.
	GrantMin uint64
	// CloseAfter is the close time of the sweep that runs, under the hand-out lock, before a lane is chosen.
	CloseAfter time.Duration
}

// LaneLeaseRenew renews a lease, reports what the holder has taken, and may ask for the next grant.
type LaneLeaseRenew struct {
	Lease lane.LeaseID
	Lane  uint8
	// Taken is the highest position the holder has taken in the lane so far; zero for none.
	Taken uint64
	// Rate is positions taken per second, measured by the holder.
	Rate uint64
	// WantGrant asks for the next grant.
	WantGrant bool
	// Settles finishes the grant before the current one, once the holder has switched.
	Settles lane.Settles
	// Height is the best block height at the time, recorded with a grant.
	Height uint32
	// GrantMin is the fewest positions in a grant.
	GrantMin uint64
	// GrantDuration is how long a grant should last at Rate.
	GrantDuration time.Duration
}

// LaneLeaseRelease ends a lease cleanly.
type LaneLeaseRelease struct {
	Lease lane.LeaseID
	Lane  uint8
	// LastTaken is the last position the holder took in the lane; zero for none.
	LastTaken uint64
	// Settles finishes the grant before the current one, as on a renewal.
	Settles lane.Settles
}
