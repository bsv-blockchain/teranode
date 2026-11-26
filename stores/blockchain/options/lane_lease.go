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
	// GrantMin requests the first grant's size, capped by the grant limit and positions left.
	GrantMin uint64
	// CloseAfter is the close time of the sweep that runs, under the hand-out lock, before a lane is chosen.
	CloseAfter time.Duration
}

// LaneLeaseRenew renews a lease, reports what the holder has taken, and may ask for the next grant.
type LaneLeaseRenew struct {
	Lease lane.LeaseID
	Lane  uint8
	// Rate is positions taken per second, measured by the holder.
	Rate uint64
	// WantGrant asks for the next grant.
	WantGrant bool
	// Settles finishes the oldest grant the holder has left and not yet reported; recorded only
	// with WantGrant, on the new grant's hand-out row.
	Settles lane.Settles
	// Height is the best block height at the time, recorded with a grant.
	Height uint32
	// GrantMin requests a minimum grant size, capped by the grant limit and positions left.
	GrantMin uint64
	// GrantDuration is how long a grant should last at Rate.
	GrantDuration time.Duration
}

// LaneLeaseRelease ends a lease cleanly.
type LaneLeaseRelease struct {
	Lease lane.LeaseID
	Lane  uint8
	// LastTaken is the highest position the holder gave out in the lane that may carry a record;
	// positions at the top that came back unwritten are left out, so it is zero when none was given
	// out or every one came back.
	LastTaken uint64
	// Settles finishes the oldest grant left and not yet reported, as on a renewal; a newer one
	// left after it is settled by LastTaken.
	Settles lane.Settles
}

// WithLaneMapRows has StoreBlock insert the block's lane map rows in the block's own database
// transaction, so the rows commit with the block and only with it. The rows are measured, never
// acted on: a row the store cannot insert is rolled back alone, counted, and the block is stored.
func WithLaneMapRows(rows []lane.MapRow) StoreBlockOption {
	return func(opts *StoreBlockOptions) {
		opts.LaneMapRows = rows
	}
}

// WithOwnBlockLaneMapRows is WithLaneMapRows for a block this node mined, so the lane metrics
// count its rows under block="own" rather than as a peer's.
func WithOwnBlockLaneMapRows(rows []lane.MapRow) StoreBlockOption {
	return func(opts *StoreBlockOptions) {
		opts.LaneMapRows = rows
		opts.LaneMapRowsOwnBlock = true
	}
}

// LaneMetricsBlock is the block label the lane metrics give the block these options store:
// lane.BlockOwn for a block this node mined, lane.BlockPeer otherwise.
func (o StoreBlockOptions) LaneMetricsBlock() string {
	if o.LaneMapRowsOwnBlock {
		return lane.BlockOwn
	}

	return lane.BlockPeer
}
